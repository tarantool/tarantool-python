import sys
import time
import unittest
import warnings

import tarantool
from tarantool.error import (
    ClusterConnectWarning,
    DatabaseError,
    NetworkError,
    NetworkWarning,
    PoolTolopogyError,
    PoolTolopogyWarning,
)

from .lib.skip import skip_or_run_sql_test
from .lib.tarantool_server import TarantoolServer


def create_server(_id):
    srv = TarantoolServer()
    srv.script = 'test/suites/box.lua'
    srv.start()
    srv.admin("box.schema.user.create('test', {password = 'test', " +
              "if_not_exists = true})")
    srv.admin("box.schema.user.grant('test', 'execute', 'universe')")
    srv.admin("box.schema.space.create('test')")
    srv.admin(r"box.space.test:format({"
             +r" { name = 'pk', type = 'string' }," +
              r" { name = 'id', type = 'number', is_nullable = true }" +
              r"})")
    srv.admin(r"box.space.test:create_index('pk'," +
              r"{ unique = true," +
              r"  parts = {{field = 1, type = 'string'}}})")
    srv.admin(r"box.space.test:create_index('id'," +
              r"{ unique = true," +
              r"  parts = {{field = 2, type = 'number', is_nullable=true}}})")
    srv.admin("box.schema.user.grant('test', 'read,write', 'space', 'test')")
    srv.admin("json = require('json')")

    # Create srv_id function (for testing purposes).
    srv.admin("function srv_id() return %s end" % _id)
    return srv


@unittest.skipIf(sys.platform.startswith("win"),
                 'Pool tests on windows platform are not supported')
class TestSuite_Pool(unittest.TestCase):
    def set_ro(self, srv, read_only):
        if read_only:
            req = r'box.cfg{read_only = true}'
        else:
            req = r'box.cfg{read_only = false}'

        srv.admin(req)

    def set_cluster_ro(self, read_only_list):
        assert len(self.servers) == len(read_only_list)

        for i in range(len(self.servers)):
            self.set_ro(self.servers[i], read_only_list[i])

    def retry(self, func, count=5, timeout=0.5):
        for i in range(count):
            try:
                func()
            except Exception as e:
                if i + 1 == count:
                    raise e

            time.sleep(timeout)

    @classmethod
    def setUpClass(self):
        print(' POOL '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

    def setUp(self):
        # Create five servers and extract helpful fields for tests.
        self.servers = []
        self.addrs = []
        self.servers_count = 5
        for i in range(self.servers_count):
            srv = create_server(i)
            self.servers.append(srv)
            self.addrs.append({'host': srv.host, 'port': srv.args['primary']})

    def test_00_basic(self):
        self.set_cluster_ro([False, False, True, False, True])

        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        self.assertSequenceEqual(
            self.pool.eval('return box.info().ro', mode=tarantool.Mode.RW),
            [False])
        self.assertSequenceEqual(
            self.pool.eval('return box.info().ro', mode=tarantool.Mode.RO),
            [True])
        self.assertSequenceEqual(
            self.pool.eval('return box.info().ro', mode=tarantool.Mode.PREFER_RW),
            [False])
        self.assertSequenceEqual(
            self.pool.eval('return box.info().ro', mode=tarantool.Mode.PREFER_RO),
            [True])

    def test_01_roundrobin(self):
        self.set_cluster_ro([False, False, True, False, True])
        RW_ports = set([str(self.addrs[0]['port']), str(self.addrs[1]['port']), str(self.addrs[3]['port'])])
        RO_ports = set([str(self.addrs[2]['port']), str(self.addrs[4]['port'])])
        all_ports = set()
        for addr in self.addrs:
            all_ports.add(str(addr['port']))

        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test',
            refresh_delay=0.2)

        def get_port(self, mode):
            resp = self.pool.eval('return box.cfg.listen', mode=mode)
            self.assertIsInstance(resp.data[0], str)
            return resp.data[0]

        # Expect ANY iterate through all instances.
        ANY_ports_result = set()
        for i in range(len(self.servers)):
            ANY_ports_result.add(get_port(self, tarantool.Mode.ANY))

        self.assertSetEqual(ANY_ports_result, all_ports)

        # Expect RW iterate through all RW instances.
        RW_ports_result = set()
        for i in range(len(self.servers)):
            RW_ports_result.add(get_port(self, tarantool.Mode.RW))

        self.assertSetEqual(RW_ports_result, RW_ports)

        # Expect RO iterate through all RO instances.
        RO_ports_result = set()
        for i in range(len(self.servers)):
            RO_ports_result.add(get_port(self, tarantool.Mode.RO))

        self.assertSetEqual(RO_ports_result, RO_ports)

        # Expect PREFER_RW iterate through all RW instances if there is at least one.
        PREFER_RW_ports_result = set()
        for i in range(len(self.servers)):
            PREFER_RW_ports_result.add(get_port(self, tarantool.Mode.PREFER_RW))

        self.assertSetEqual(PREFER_RW_ports_result, RW_ports)

        # Expect PREFER_RO iterate through all RO instances if there is at least one.
        PREFER_RO_ports_result = set()
        for i in range(len(self.servers)):
            PREFER_RO_ports_result.add(get_port(self, tarantool.Mode.PREFER_RO))

        self.assertSetEqual(PREFER_RO_ports_result, RO_ports)

        # Setup cluster with no RW.
        self.set_cluster_ro([True, True, True, True, True])

        # Expect RW to fail if there are no RW.
        def expect_RW_to_fail_if_there_are_no_RW():
            with self.assertRaises(PoolTolopogyError):
                self.pool.eval('return box.cfg.listen', mode=tarantool.Mode.RW)

        self.retry(func=expect_RW_to_fail_if_there_are_no_RW)

        # Expect PREFER_RW iterate through all instances if there are no RW.
        def expect_PREFER_RW_iterate_through_all_instances_if_there_are_no_RW():
            PREFER_RW_ports_result_all_ro = set()
            for i in range(len(self.servers)):
                PREFER_RW_ports_result_all_ro.add(get_port(self, tarantool.Mode.PREFER_RW))

            self.assertSetEqual(PREFER_RW_ports_result_all_ro, all_ports)
    
        self.retry(func=expect_PREFER_RW_iterate_through_all_instances_if_there_are_no_RW)

        # Setup cluster with no RO.
        self.set_cluster_ro([False, False, False, False, False])

        # Expect RO to fail if there are no RO.
        def expect_RO_to_fail_if_there_are_no_RO():
            with self.assertRaises(PoolTolopogyError):
                self.pool.eval('return box.cfg.listen', mode=tarantool.Mode.RO)

        self.retry(func=expect_RO_to_fail_if_there_are_no_RO)

        # Expect PREFER_RO iterate through all instances if there are no RO.
        def expect_PREFER_RO_iterate_through_all_instances_if_there_are_no_RO():
            PREFER_RO_ports_result_all_rw = set()
            for i in range(len(self.servers)):
                PREFER_RO_ports_result_all_rw.add(get_port(self, tarantool.Mode.PREFER_RO))

            self.assertSetEqual(PREFER_RO_ports_result_all_rw, all_ports)

        self.retry(func=expect_PREFER_RO_iterate_through_all_instances_if_there_are_no_RO)

    def test_02_exception_raise(self):
        self.set_cluster_ro([False, False, True, False, True])

        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        with self.assertRaises(DatabaseError):
            self.pool.call('non_existing_procedure', mode=tarantool.Mode.ANY)

    def test_03_insert(self):
        self.set_cluster_ro([True, True, False, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        self.assertSequenceEqual(
            self.pool.insert('test', ['test_03_insert_1', 1]),
            [['test_03_insert_1', 1]])
        self.assertSequenceEqual(
            self.pool.insert('test', ['test_03_insert_2', 2],
                mode=tarantool.Mode.RW),
            [['test_03_insert_2', 2]])

        conn_2 = tarantool.connect(
            host=self.addrs[2]['host'],
            port=self.addrs[2]['port'],
            user='test',
            password='test')

        try:
            self.assertSequenceEqual(
                conn_2.select('test', 'test_03_insert_1'),
                [['test_03_insert_1', 1]])
        finally:
            conn_2.close()

    def test_04_delete(self):
        self.set_cluster_ro([True, True, True, False, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        conn_3 = tarantool.connect(
            host=self.addrs[3]['host'],
            port=self.addrs[3]['port'],
            user='test',
            password='test')

        try:
            conn_3.insert('test', ['test_04_delete_1', 1])
            conn_3.insert('test', ['test_04_delete_2', 2])

            self.assertSequenceEqual(
                self.pool.delete('test', 'test_04_delete_1'),
                [['test_04_delete_1', 1]])
            self.assertSequenceEqual(
                conn_3.select('test', 'test_04_delete_1'),
                [])

            self.assertSequenceEqual(
                self.pool.delete('test', 2, index='id', mode=tarantool.Mode.RW),
                [['test_04_delete_2', 2]])
            self.assertSequenceEqual(
                conn_3.select('test', 'test_04_delete_2'),
                [])
        finally:
            conn_3.close()

    def test_05_upsert(self):
        self.set_cluster_ro([True, False, True, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        conn_1 = tarantool.connect(
            host=self.addrs[1]['host'],
            port=self.addrs[1]['port'],
            user='test',
            password='test')

        try:
            self.assertSequenceEqual(
                self.pool.upsert('test', ['test_05_upsert', 3], [('+', 1, 1)]),
                [])
            self.assertSequenceEqual(
                conn_1.select('test', 'test_05_upsert'),
                [['test_05_upsert', 3]])

            self.assertSequenceEqual(
                self.pool.upsert('test', ['test_05_upsert', 3],
                    [('+', 1, 1)], mode=tarantool.Mode.RW), [])
            self.assertSequenceEqual(
                conn_1.select('test', 'test_05_upsert'),
                [['test_05_upsert', 4]])
        finally:
            conn_1.close()

    def test_06_update(self):
        self.set_cluster_ro([True, True, True, True, False])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        conn_4 = tarantool.connect(
            host=self.addrs[4]['host'],
            port=self.addrs[4]['port'],
            user='test',
            password='test')

        try:
            conn_4.insert('test', ['test_06_update_1', 3])
            conn_4.insert('test', ['test_06_update_2', 14])

            self.assertSequenceEqual(
                self.pool.update('test', ('test_06_update_1',), [('+', 1, 1)]),
                [['test_06_update_1', 4]])
            self.assertSequenceEqual(
                conn_4.select('test', 'test_06_update_1'),
                [['test_06_update_1', 4]])

            self.assertSequenceEqual(
                self.pool.update('test', ('test_06_update_2',),
                    [('=', 1, 10)], mode=tarantool.Mode.RW),
                [['test_06_update_2', 10]])
            self.assertSequenceEqual(
                conn_4.select('test', 'test_06_update_2'),
                [['test_06_update_2', 10]])
        finally:
            conn_4.close()

    def test_07_replace(self):
        self.set_cluster_ro([True, True, True, True, False])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        conn_4 = tarantool.connect(
            host=self.addrs[4]['host'],
            port=self.addrs[4]['port'],
            user='test',
            password='test')

        try:
            conn_4.insert('test', ['test_07_replace', 3])

            self.assertSequenceEqual(
                self.pool.replace('test', ['test_07_replace', 4],
                    mode=tarantool.Mode.RW),
                [['test_07_replace', 4]])
            self.assertSequenceEqual(
                conn_4.select('test', 'test_07_replace'),
                [['test_07_replace', 4]])

            self.assertSequenceEqual(
                self.pool.replace('test', ['test_07_replace', 5]),
                [['test_07_replace', 5]])
            self.assertSequenceEqual(
                conn_4.select('test', 'test_07_replace'),
                [['test_07_replace', 5]])
        finally:
            conn_4.close()

    def test_08_select(self):
        self.set_cluster_ro([False, False, False, False, False])

        for addr in self.addrs:
            conn = tarantool.connect(
                host=addr['host'],
                port=addr['port'],
                user='test',
                password='test')

            try:
                conn.insert('test', ['test_08_select', 3])
            finally:
                conn.close()

        self.set_cluster_ro([False, True, False, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        self.assertSequenceEqual(
            self.pool.select('test', 'test_08_select'),
            [['test_08_select', 3]])
        self.assertSequenceEqual(
            self.pool.select('test', ['test_08_select'],
                mode=tarantool.Mode.ANY),
            [['test_08_select', 3]])
        self.assertSequenceEqual(
            self.pool.select('test', 3, index='id',
                mode=tarantool.Mode.RO),
            [['test_08_select', 3]])
        self.assertSequenceEqual(
            self.pool.select('test', [3], index='id',
                mode=tarantool.Mode.PREFER_RW),
            [['test_08_select', 3]])

    def test_09_ping(self):
        self.pool = tarantool.ConnectionPool(addrs=self.addrs,
            user='test',
            password='test')

        with self.assertRaisesRegex(ValueError, "Please, specify 'mode' keyword argument"):
            self.pool.ping()

        self.assertTrue(self.pool.ping(mode=tarantool.Mode.ANY) > 0)
        self.assertEqual(self.pool.ping(mode=tarantool.Mode.RW, notime=True), "Success")

    def test_10_call(self):
        self.set_cluster_ro([False, True, False, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        with self.assertRaisesRegex(ValueError, "Please, specify 'mode' keyword argument"):
            self.pool.call('box.info')

        self.assertEqual(
            self.pool.call('box.info', mode=tarantool.Mode.RW)[0]['ro'],
            False)

        self.assertSequenceEqual(
            self.pool.call('json.encode', {'test_10_call': 1}, mode=tarantool.Mode.ANY),
            ['{"test_10_call":1}'])

    def test_11_eval(self):
        self.set_cluster_ro([False, True, False, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        with self.assertRaisesRegex(ValueError, "Please, specify 'mode' keyword argument"):
            self.pool.eval('return box.info()')

        self.assertEqual(
            self.pool.eval('return box.info()', mode=tarantool.Mode.RW)[0]['ro'],
            False)

        self.assertSequenceEqual(
            self.pool.eval('return json.encode(...)', {'test_11_eval': 1}, mode=tarantool.Mode.ANY),
            ['{"test_11_eval":1}'])

    @skip_or_run_sql_test
    def test_12_execute(self):
        self.set_cluster_ro([False, True, True, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test')

        with self.assertRaisesRegex(ValueError, "Please, specify 'mode' keyword argument"):
            self.pool.execute("insert into \"test\" values ('test_12_execute_1', 1)")

        resp = self.pool.execute(
            "insert into \"test\" values ('test_12_execute_1', 1)",
            mode=tarantool.Mode.RW)

        self.assertEqual(resp.affected_row_count, 1)
        self.assertEqual(resp.data, None)

        resp = self.pool.execute(
            'insert into "test" values (:pk, :id)',
            { 'pk': 'test_12_execute_2', 'id': 2},
            mode=tarantool.Mode.RW)
        self.assertEqual(resp.affected_row_count, 1)
        self.assertEqual(resp.data, None)

        conn_0 = tarantool.connect(
            host=self.addrs[0]['host'],
            port=self.addrs[0]['port'],
            user='test',
            password='test')

        try:
            self.assertSequenceEqual(
                conn_0.select('test', 'test_12_execute_1'),
                [['test_12_execute_1', 1]])
            self.assertSequenceEqual(
                conn_0.select('test', 'test_12_execute_2'),
                [['test_12_execute_2', 2]])
        finally:
            conn_0.close()

    def test_13_failover(self):
        warnings.simplefilter('ignore', category=NetworkWarning)
        warnings.simplefilter('ignore', category=PoolTolopogyWarning)

        self.set_cluster_ro([False, True, True, True, True])
        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test',
            refresh_delay=0.2)

        # Simulate failover
        self.servers[0].stop()
        self.set_ro(self.servers[1], False)

        def expect_RW_request_execute_on_new_master():
            self.assertSequenceEqual(
                self.pool.eval('return box.cfg.listen', mode=tarantool.Mode.RW),
                [ str(self.addrs[1]['port']) ])

        self.retry(func=expect_RW_request_execute_on_new_master)

    def test_14_cluster_with_instances_dead_in_runtime_is_ok(self):
        warnings.simplefilter('ignore', category=ClusterConnectWarning)

        self.set_cluster_ro([False, True, False, True, True])
        self.servers[0].stop()

        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test',
            refresh_delay=0.2)

        self.pool.ping(mode=tarantool.Mode.RW)

    def test_15_cluster_with_dead_instances_on_start_is_ok(self):
        warnings.simplefilter('ignore', category=ClusterConnectWarning)

        self.set_cluster_ro([False, True, True, True, True])
        self.servers[0].stop()

        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test',
            refresh_delay=0.2)

        self.servers[0].start()

        def ping_RW():
            self.pool.ping(mode=tarantool.Mode.RW)

        self.retry(func=ping_RW)

    def test_16_is_closed(self):
        self.set_cluster_ro([False, False, True, False, True])

        self.pool = tarantool.ConnectionPool(
            addrs=self.addrs,
            user='test',
            password='test',)

        self.assertEqual(self.pool.is_closed(), False)

        self.pool.close()

        self.assertEqual(self.pool.is_closed(), True)

    def tearDown(self):
        if hasattr(self, 'pool'):
            self.pool.close()

        for srv in self.servers:
            srv.stop()
            srv.clean()
