"""
This module tests space and index schema fetch.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring,fixme,too-many-public-methods,too-many-branches,too-many-statements

import sys
import unittest
import pkg_resources

import tarantool
from tarantool.error import NotSupportedError

from .lib.tarantool_server import TarantoolServer
from .lib.skip import skip_or_run_constraints_test


# FIXME: I'm quite sure that there is a simpler way to count
# a method calls, but I failed to find any. It seems, I should
# look at unittest.mock more thoroughly.
class MethodCallCounter:
    def __init__(self, obj, method_name):
        self._call_count = 0
        self._bind(obj, method_name)

    def _bind(self, obj, method_name):
        self._obj = obj
        self._method_name = method_name
        self._saved_method = getattr(obj, method_name)

        def wrapper(_, *args, **kwargs):
            self._call_count += 1
            return self._saved_method(*args, **kwargs)

        bound_wrapper = wrapper.__get__(obj.__class__, obj)
        setattr(obj, method_name, bound_wrapper)

    def unbind(self):
        if self._saved_method is not None:
            setattr(self._obj, self._method_name, self._saved_method)

    def call_count(self):
        return self._call_count


class TestSuiteSchemaAbstract(unittest.TestCase):
    # Define 'encoding' field in a concrete class.
    encoding = None

    @classmethod
    def setUpClass(cls):
        params = f'connection.encoding: {repr(cls.encoding)}'
        print(f' SCHEMA ({params}) '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        cls.srv = TarantoolServer()
        cls.srv.script = 'test/suites/box.lua'
        cls.srv.start()
        cls.srv.admin("box.schema.user.create('test', {password = 'test', if_not_exists = true})")
        cls.srv.admin("box.schema.user.grant('test', 'read,write,execute', 'universe')")

        # Create server_function and tester space (for fetch_schema opt testing purposes).
        cls.srv.admin("function server_function() return 2+2 end")
        cls.srv.admin("""
        box.schema.create_space(
            'tester', {
            format = {
                {name = 'id', type = 'unsigned'},
                {name = 'name', type = 'string', is_nullable = true},
            }
        })
        """)
        cls.srv.admin("""
        box.space.tester:create_index(
            'primary_index', {
            parts = {
                {field = 1, type = 'unsigned'},
            }
        })
        """)
        cls.srv.admin("box.space.tester:insert({1, null})")

        cls.con = tarantool.Connection(cls.srv.host, cls.srv.args['primary'],
                                       encoding=cls.encoding, user='test', password='test')
        cls.con_schema_disable = tarantool.Connection(cls.srv.host, cls.srv.args['primary'],
                                                      encoding=cls.encoding, fetch_schema=False,
                                                      user='test', password='test')
        if not sys.platform.startswith("win"):
            # Schema fetch disable tests via mesh and pool connection
            # are not supported on windows platform.
            cls.mesh_con_schema_disable = tarantool.MeshConnection(
                host=cls.srv.host,
                port=cls.srv.args['primary'],
                fetch_schema=False,
                user='test', password='test')
            cls.pool_con_schema_disable = tarantool.ConnectionPool(
                [{
                    'host': cls.srv.host,
                    'port': cls.srv.args['primary']
                }],
                user='test', password='test',
                fetch_schema=False)
        cls.sch = cls.con.schema

        cls.unicode_space_name_literal = '∞'
        cls.unicode_index_name_literal = '→'

        cls.unicode_space_id, cls.unicode_index_id = cls.srv.admin("""
            do
                local space = box.schema.create_space('\\xe2\\x88\\x9e')
                local index = space:create_index('\\xe2\\x86\\x92')
                return space.id, index.id
            end
        """)

        if cls.srv.admin.tnt_version >= pkg_resources.parse_version('2.10.0'):
            cls.srv.admin("""
            box.schema.create_space(
                'constr_tester_1', {
                format = {
                    { name = 'id', type = 'unsigned' },
                    { name = 'payload', type = 'number' },
                }
            })
            box.space.constr_tester_1:create_index('I1', { parts = {'id'} })

            box.space.constr_tester_1:replace({1, 999})

            box.schema.create_space(
                'constr_tester_2', {
                format = {
                    { name = 'id', type = 'unsigned' },
                    { name = 'table1_id', type = 'unsigned',
                      foreign_key = { fk_video = { space = 'constr_tester_1', field = 'id' } },
                    },
                    { name = 'payload', type = 'number' },
                }
            })
            box.space.constr_tester_2:create_index('I1', { parts = {'id'} })
            box.space.constr_tester_2:create_index('I2', { parts = {'table1_id'} })
            """)

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

        # Count calls of fetch methods. See <fetch_count>.
        self.fetch_space_counter = MethodCallCounter(self.sch, 'fetch_space')
        self.fetch_index_counter = MethodCallCounter(self.sch, 'fetch_index')

    def tearDown(self):
        self.fetch_space_counter.unbind()
        self.fetch_index_counter.unbind()

    @property
    def fetch_count(self):
        """Amount of fetch_{space,index}() calls.

           It is initialized to zero before each test case.
        """
        res = 0
        res += self.fetch_space_counter.call_count()
        res += self.fetch_index_counter.call_count()
        return res

    def verify_unicode_space(self, space):
        self.assertEqual(space.sid, self.unicode_space_id)
        self.assertEqual(space.name, self.unicode_space_name_literal)
        self.assertEqual(space.arity, 1)

    def verify_unicode_index(self, index):
        self.assertEqual(index.space.name, self.unicode_space_name_literal)
        self.assertEqual(index.iid, self.unicode_index_id)
        self.assertEqual(index.name, self.unicode_index_name_literal)
        self.assertEqual(len(index.parts), 1)

    def test_01_space_bad(self):
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no space.*'):
            self.sch.get_space(0)
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no space.*'):
            self.sch.get_space(0)
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no space.*'):
            self.sch.get_space('bad_name')

    def test_02_index_bad(self):
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no space.*'):
            self.sch.get_index(0, 'primary')
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no space.*'):
            self.sch.get_index('bad_space', 'primary')
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no index.*'):
            self.sch.get_index(280, 'bad_index')
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no index.*'):
            self.sch.get_index(280, 'bad_index')
        with self.assertRaisesRegex(tarantool.SchemaError, 'There\'s no index.*'):
            self.sch.get_index(280, 3)

    def test_03_01_space_name__(self):
        self.con.flush_schema()
        space = self.sch.get_space('_schema')
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_space')
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_index')
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

        space = self.sch.get_space(self.unicode_space_name_literal)
        self.verify_unicode_space(space)

    def test_03_02_space_number(self):
        self.con.flush_schema()
        space = self.sch.get_space(272)
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space(280)
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space(288)
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

        space = self.sch.get_space(self.unicode_space_id)
        self.verify_unicode_space(space)

    def test_04_space_cached(self):
        space = self.sch.get_space('_schema')
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_space')
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_index')
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

        space = self.sch.get_space(self.unicode_space_name_literal)
        self.verify_unicode_space(space)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

    def test_05_01_index_name___name__(self):
        self.con.flush_schema()
        index = self.sch.get_index('_index', 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 'name')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_space', 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index('_space', 'name')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_name_literal,
                                   self.unicode_index_name_literal)
        self.verify_unicode_index(index)

    def test_05_02_index_name___number(self):
        self.con.flush_schema()
        index = self.sch.get_index('_index', 0)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_space', 0)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index('_space', 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_name_literal,
                                   self.unicode_index_id)
        self.verify_unicode_index(index)

    def test_05_03_index_number_name__(self):
        self.con.flush_schema()
        index = self.sch.get_index(288, 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(288, 'name')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 'name')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_id,
                                   self.unicode_index_name_literal)
        self.verify_unicode_index(index)

    def test_05_04_index_number_number(self):
        self.con.flush_schema()
        index = self.sch.get_index(288, 0)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(288, 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 0)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_id,
                                   self.unicode_index_id)
        self.verify_unicode_index(index)

    def test_06_index_cached(self):
        index = self.sch.get_index('_index', 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

        cases = (
            (self.unicode_space_name_literal, self.unicode_index_name_literal),
            (self.unicode_space_name_literal, self.unicode_index_id),
            (self.unicode_space_id, self.unicode_index_name_literal),
            (self.unicode_space_id, self.unicode_index_id),
        )
        for space, index_id in cases:
            index = self.sch.get_index(space, index_id)
            self.verify_unicode_index(index)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

    def test_07_schema_version_update(self):
        _space_len = len(self.con.select('_space'))
        self.srv.admin("box.schema.create_space('ttt22')")
        self.assertEqual(len(self.con.select('_space')), _space_len + 1)

    # For schema fetch disable testing purposes.
    testing_methods = {
        'unavailable': {
            'replace': {
                'input': ['tester', (1, None)],
                'output': [[1, None]],
            },
            'delete': {
                'input': ['tester', 1],
                'output': [[1, None]],
            },
            'insert': {
                'input': ['tester', (1, None)],
                'output': [[1, None]],
            },
            'upsert': {
                'input': ['tester', (1, None), []],
                'output': [],
            },
            'update': {
                'input': ['tester', 1, []],
                'output': [[1, None]],
            },
            'select': {
                'input': ['tester', 1],
                'output': [[1, None]],
            },
            'space': {
                'input': ['tester'],
            },
        },
        'available': {
            # CRUD methods are also tested with the fetch_schema=False opt,
            # see the test_crud.py file.
            'call': {
                'input': ['server_function'],
                'output': [4],
            },
            'eval': {
                'input': ['return 2+2'],
                'output': [4],
            },
            'ping': {
                'input': [],
            },
        },
    }

    def _run_test_schema_fetch_disable(self, con, mode=None):
        # Enable SQL test case for tarantool 2.* and higher.
        if int(str(self.srv.admin.tnt_version)[0]) > 1:
            if self.srv.admin.tnt_version >= pkg_resources.parse_version('2.11.0'):
                # SEQSCAN keyword is explicitly allowing to use seqscan
                # https://github.com/tarantool/tarantool/commit/77648827326ad268ec0ffbcd620c2371b65ef2b4
                # It was introduced in 2.11.0-rc1. If compat.sql_seq_scan_default
                # set to "new" (default value since 3.0), returns error
                # if trying to scan without keyword.
                self.testing_methods['available']['execute'] = {
                    'input': ['SELECT * FROM SEQSCAN "tester"'],
                    'output': [[1, None]],
                }
            else:
                self.testing_methods['available']['execute'] = {
                    'input': ['SELECT * FROM "tester"'],
                    'output': [[1, None]],
                }

        # Testing the schemaless connection with methods
        # that should NOT be available.
        if mode is not None:
            for addr in con.pool.keys():
                self.assertEqual(con.pool[addr].conn.schema_version, 0)
                self.assertEqual(con.pool[addr].conn.schema, None)
        else:
            self.assertEqual(con.schema_version, 0)
            self.assertEqual(con.schema, None)
        for method_case in self.testing_methods['unavailable']:
            with self.subTest(name=method_case):
                if isinstance(con, tarantool.ConnectionPool) and method_case == 'space':
                    continue
                testing_function = getattr(con, method_case)
                try:
                    if mode is not None:
                        _ = testing_function(
                            *self.testing_methods['unavailable'][method_case]['input'],
                            mode=mode)
                    else:
                        _ = testing_function(
                            *self.testing_methods['unavailable'][method_case]['input'])
                except NotSupportedError as exc:
                    self.assertEqual(exc.message, 'This method is not available in '
                                                  'connection opened with fetch_schema=False')
        # Testing the schemaless connection with methods
        # that should be available.
        for method_case in self.testing_methods['available']:
            with self.subTest(name=method_case):
                testing_function = getattr(con, method_case)
                if mode is not None:
                    resp = testing_function(
                        *self.testing_methods['available'][method_case]['input'],
                        mode=mode)
                else:
                    resp = testing_function(
                        *self.testing_methods['available'][method_case]['input'])
                if method_case == 'ping':
                    self.assertEqual(isinstance(resp, float), True)
                else:
                    self.assertEqual(
                        resp.data,
                        self.testing_methods['available'][method_case]['output'])

        # Turning the same connection into schemaful.
        if mode is not None:
            for addr in con.pool.keys():
                con.pool[addr].conn.update_schema(con.pool[addr].conn.schema_version)
        else:
            con.update_schema(con.schema_version)

        # Testing the schemaful connection with methods
        # that should NOW be available.
        for method_case in self.testing_methods['unavailable']:
            with self.subTest(name=method_case):
                if isinstance(con, tarantool.ConnectionPool) and method_case == 'space':
                    continue
                testing_function = getattr(con, method_case)
                if mode is not None:
                    resp = testing_function(
                        *self.testing_methods['unavailable'][method_case]['input'],
                        mode=mode)
                else:
                    resp = testing_function(
                        *self.testing_methods['unavailable'][method_case]['input'])
                if method_case == 'space':
                    self.assertEqual(isinstance(resp, tarantool.space.Space), True)
                else:
                    self.assertEqual(
                        resp.data,
                        self.testing_methods['unavailable'][method_case]['output'])
        # Testing the schemaful connection with methods
        # that should have remained available.
        for method_case in self.testing_methods['available']:
            with self.subTest(name=method_case):
                testing_function = getattr(con, method_case)
                if mode is not None:
                    resp = testing_function(
                        *self.testing_methods['available'][method_case]['input'],
                        mode=mode)
                else:
                    resp = testing_function(
                        *self.testing_methods['available'][method_case]['input'])
                if method_case == 'ping':
                    self.assertEqual(isinstance(resp, float), True)
                else:
                    self.assertEqual(
                        resp.data,
                        self.testing_methods['available'][method_case]['output'])
        if mode is not None:
            self.assertNotEqual(con.pool[addr].conn.schema_version, 1)
            self.assertNotEqual(con.pool[addr].conn.schema, None)
        else:
            self.assertNotEqual(con.schema_version, 1)
            self.assertNotEqual(con.schema, None)

    def test_08_schema_fetch_disable_via_connection(self):
        self._run_test_schema_fetch_disable(self.con_schema_disable)

    @unittest.skipIf(
        sys.platform.startswith("win"),
        'Schema fetch disable tests via mesh connection on windows platform are not supported')
    def test_08_schema_fetch_disable_via_mesh_connection(self):
        self._run_test_schema_fetch_disable(self.mesh_con_schema_disable)

    @unittest.skipIf(
        sys.platform.startswith("win"),
        'Schema fetch disable tests via connection pool on windows platform are not supported')
    def test_08_schema_fetch_disable_via_connection_pool(self):
        self._run_test_schema_fetch_disable(self.pool_con_schema_disable,
                                            mode=tarantool.Mode.ANY)

    @skip_or_run_constraints_test
    def test_09_foreign_key_info_fetched_to_schema(self):
        self.assertIn('foreign_key', self.sch.get_space('constr_tester_2').format['table1_id'])

    @skip_or_run_constraints_test
    def test_10_foreign_key_valid_replace(self):
        self.assertSequenceEqual(
            self.con.replace('constr_tester_2', [1, 1, 623]),
            [[1, 1, 623]])

    @skip_or_run_constraints_test
    def test_11_foreign_key_invalid_replace(self):
        with self.assertRaisesRegex(tarantool.DatabaseError,
                                    'foreign tuple was not found'):
            self.con.replace('constr_tester_2', [2, 999, 623])

    @classmethod
    def tearDownClass(cls):
        # We need to drop spaces with foreign keys with predetermined order,
        # otherwise remote server clean() will fail to clean up resources.
        if cls.srv.admin.tnt_version >= pkg_resources.parse_version('2.10.0'):
            cls.srv.admin("""
            box.space.constr_tester_2:drop()
            box.space.constr_tester_1:drop()
            """)

        cls.con.close()
        cls.con_schema_disable.close()
        if not sys.platform.startswith("win"):
            # Schema fetch disable tests via mesh and pool connection
            # are not supported on windows platform.
            cls.mesh_con_schema_disable.close()
            cls.pool_con_schema_disable.close()
        cls.srv.stop()
        cls.srv.clean()


class TestSuiteSchemaUnicodeConnection(TestSuiteSchemaAbstract):
    encoding = 'utf-8'


class TestSuiteSchemaBinaryConnection(TestSuiteSchemaAbstract):
    encoding = None
