"""
This module tests work with a cluster of Tarantool servers through
MeshConnection.
"""
# pylint: disable=missing-class-docstring,missing-function-docstring

import sys
import unittest
import warnings
from time import sleep
import tarantool
from tarantool.error import (
    NetworkError,
    ConfigurationError,
    NetworkWarning,
    ClusterDiscoveryWarning,
)
from .lib.tarantool_server import TarantoolServer


def create_server(_id):
    srv = TarantoolServer()
    srv.script = 'test/suites/box.lua'
    srv.start()
    srv.admin("box.schema.user.create('test', {password = 'test', "
              "if_not_exists = true})")
    srv.admin("box.schema.user.grant('test', 'execute', 'universe')")

    # Create srv_id function (for testing purposes).
    srv.admin(f"function srv_id() return {_id} end")
    return srv


@unittest.skipIf(sys.platform.startswith("win"),
                 'Mesh tests on windows platform are not supported')
class TestSuiteMesh(unittest.TestCase):
    # pylint: disable=too-many-instance-attributes

    def define_cluster_function(self, func_name, servers):
        addresses = [(srv.host, srv.args['primary']) for srv in servers]
        addresses_lua = ",".join(f"'{address[0]}:{address[1]}'" for address in addresses)
        func_body = f"""
            function {func_name}()
                return {{{addresses_lua}}}
            end
        """
        for srv in self.servers:
            srv.admin(func_body)

    def define_custom_cluster_function(self, func_name, retval):
        func_body = f"""
            function {func_name}()
                return {retval}
            end
        """
        for srv in self.servers:
            srv.admin(func_body)

    @classmethod
    def setUpClass(cls):
        print(' MESH '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)

    def setUp(self):
        # Create two servers and extract helpful fields for tests.
        self.srv = create_server(1)
        self.srv2 = create_server(2)
        self.servers = [self.srv, self.srv2]
        self.host_1 = self.srv.host
        self.port_1 = self.srv.args['primary']
        self.host_2 = self.srv2.host
        self.port_2 = self.srv2.args['primary']

        # Create get_all_nodes() function on servers.
        self.get_all_nodes_func_name = 'get_all_nodes'
        self.define_cluster_function(self.get_all_nodes_func_name,
                                     self.servers)

    def test_00_basic(self):
        def assert_srv_id(con, srv_id):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', category=NetworkWarning)
                resp = con.call('srv_id')
            self.assertEqual(resp.data and resp.data[0], srv_id)

        con = tarantool.MeshConnection(addrs=[
            {'host': self.host_1, 'port': self.port_1},
            {'host': self.host_2, 'port': self.port_2},
        ], user='test', password='test')

        try:
            # Response from instance#1.
            assert_srv_id(con, 1)

            # Stop instance#1 -- response from instance#2.
            self.srv.stop()
            assert_srv_id(con, 2)

            # Start instance#1, stop instance#2 -- response from
            # instance#1 again.
            self.srv.start()
            self.srv.admin('function srv_id() return 1 end')
            self.srv2.stop()
            assert_srv_id(con, 1)

            # Stop instance #2 -- NetworkError (because we have no
            # alive servers anymore).
            self.srv.stop()
            with self.assertRaises(NetworkError):
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', category=NetworkWarning)
                    con.ping()
        finally:
            con.close()

    def test_01_contructor(self):
        # Verify that an error is risen when no addresses are
        # configured (neither with host/port, nor with addrs).
        with self.assertRaises(ConfigurationError):
            tarantool.MeshConnection()

        # Verify that a bad address given at initialization leads
        # to an error.
        bad_addrs = [
            {"port": 1234},                         # no host
            {"host": "localhost"},                  # no port
            {"host": "localhost", "port": "1234"},  # port is str
        ]
        for bad_addr in bad_addrs:
            with self.assertRaises(ConfigurationError):
                con = tarantool.MeshConnection(bad_addr.get('host'),
                                               bad_addr.get('port'))
            with self.assertRaises(ConfigurationError):
                con = tarantool.MeshConnection(addrs=[bad_addr])

        # Verify that identical addresses are squashed.
        addrs = [{"host": "localhost", "port": 1234}]
        con = tarantool.MeshConnection("localhost", 1234, addrs=addrs,
                                       connect_now=False)
        try:
            self.assertEqual(len(con.strategy.addrs), 1)
        finally:
            con.close()

    def test_02_discovery_bad_address(self):
        retvals = [
            "",
            "1",
            "'localhost:1234'",
            "{}",
            "error('raise an error')",
            "{'localhost:foo'}",
            "{'localhost:0'}",
            "{'localhost:65536'}",
            "{'localhost:1234:5678'}",
            "{':1234'}",
            "{'localhost:'}",
        ]
        for retval in retvals:
            func_name = 'bad_cluster_discovery'
            self.define_custom_cluster_function(func_name, retval)
            con = tarantool.MeshConnection(self.host_1, self.port_1,
                                           user='test', password='test')
            try:
                con.cluster_discovery_function = func_name

                # Verify that a cluster discovery (that is triggered
                # by ping) give one or two warnings.
                with warnings.catch_warnings(record=True) as warns:
                    con.ping()
                    self.assertTrue(len(warns) in (1, 2))
                    for warn in warns:
                        self.assertIs(warn.category, ClusterDiscoveryWarning)

                # Verify that incorrect or empty result was discarded.
                self.assertEqual(len(con.strategy.addrs), 1)
                self.assertEqual(con.strategy.addrs[0]['host'], self.host_1)
                self.assertEqual(con.strategy.addrs[0]['port'], self.port_1)
            finally:
                con.close()

    def test_03_discovery_bad_good_addresses(self):
        func_name = 'bad_and_good_addresses'
        retval = f"{{'localhost:', '{self.host_2}:{self.port_2}'}}"
        self.define_custom_cluster_function(func_name, retval)
        con = tarantool.MeshConnection(self.host_1, self.port_1,
                                       user='test', password='test')
        try:
            con.cluster_discovery_function = func_name

            # Verify that a cluster discovery (that is triggered
            # by ping) give one warning.
            with warnings.catch_warnings(record=True) as warns:
                con.ping()
                self.assertEqual(len(warns), 1)
                self.assertIs(warns[0].category, ClusterDiscoveryWarning)

            # Verify that only second address was accepted.
            self.assertEqual(len(con.strategy.addrs), 1)
            self.assertEqual(con.strategy.addrs[0]['host'], self.host_2)
            self.assertEqual(con.strategy.addrs[0]['port'], self.port_2)
        finally:
            con.close()

    def test_04_discovery_add_address(self):
        # Create a mesh connection; pass only the first server
        # address.
        con = tarantool.MeshConnection(
            self.host_1, self.port_1, user='test', password='test',
            cluster_discovery_function=self.get_all_nodes_func_name,
            connect_now=False)

        try:
            # Verify that the strategy has one address that comes from
            # the constructor arguments.
            self.assertEqual(len(con.strategy.addrs), 1)
            con.connect()

            # Verify that we work with the first server.
            resp = con.call('srv_id')
            self.assertEqual(resp.data and resp.data[0], 1)

            # Verify that the refresh was successful and the strategy
            # has 2 addresses.
            self.assertEqual(len(con.strategy.addrs), 2)
        finally:
            con.close()

    def test_05_discovery_delay(self):
        # Create a mesh connection, pass only the first server address.
        con = tarantool.MeshConnection(
            self.host_1, self.port_1, user='test', password='test',
            cluster_discovery_function=self.get_all_nodes_func_name,
            cluster_discovery_delay=1)

        try:
            # Verify that the strategy has two addresses come from
            # the function right after connecting.
            self.assertEqual(len(con.strategy.addrs), 2)

            # Drop addresses list to the initial state.
            con.strategy.update([con.strategy.addrs[0], ])

            # Verify that the discovery will not be performed until
            # 'cluster_discovery_delay' seconds will be passed.
            con.ping()
            self.assertEqual(len(con.strategy.addrs), 1)

            sleep(1.1)

            # Refresh after cluster_discovery_delay.
            con.ping()
            self.assertEqual(len(con.strategy.addrs), 2)
        finally:
            con.close()

    def test_06_reconnection(self):
        # Create a mesh connection; pass only the first server
        # address.
        con = tarantool.MeshConnection(
            self.host_1, self.port_1, user='test', password='test',
            cluster_discovery_function=self.get_all_nodes_func_name)

        try:
            con.last_nodes_refresh = 0
            resp = con.call('srv_id')
            self.assertEqual(resp.data and resp.data[0], 1)

            # Verify that the last discovery was successful and the
            # strategy has 2 addresses.
            self.assertEqual(len(con.strategy.addrs), 2)

            self.srv.stop()

            # Verify that we switched to the second server.
            with warnings.catch_warnings():
                # Suppress reconnection warnings.
                warnings.simplefilter("ignore")
                resp = con.call('srv_id')
            self.assertEqual(resp.data and resp.data[0], 2)
        finally:
            con.close()

    def test_07_discovery_exclude_address(self):
        # Define function to get back only second server.
        func_name = 'get_second_node'
        self.define_cluster_function(func_name, [self.srv2])

        # Create a mesh connection, pass only the first server address.
        con = tarantool.MeshConnection(
            self.host_1, self.port_1, user='test', password='test',
            cluster_discovery_function=func_name)

        try:
            # Verify that discovery was successful and the strategy
            # has 1 address.
            self.assertEqual(len(con.strategy.addrs), 1)

            # Verify that the current server is second one.
            resp = con.call('srv_id')
            self.assertEqual(resp.data and resp.data[0], 2)
        finally:
            con.close()

    def tearDown(self):
        self.srv.stop()
        self.srv.clean()

        self.srv2.stop()
        self.srv2.clean()
