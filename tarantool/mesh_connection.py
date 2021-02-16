# -*- coding: utf-8 -*-
'''
This module provides MeshConnection class with automatic switch
between tarantool instances and basic Round-Robin strategy.
'''

import time


from tarantool.connection import Connection
from tarantool.error import (
    warn,
    NetworkError,
    DatabaseError,
    ConfigurationError,
    ClusterDiscoveryWarning,
)
from tarantool.utils import ENCODING_DEFAULT
from tarantool.const import (
    CONNECTION_TIMEOUT,
    SOCKET_TIMEOUT,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_DELAY,
    CLUSTER_DISCOVERY_DELAY,
)

from tarantool.request import (
    RequestCall
)

try:
    string_types = basestring
except NameError:
    string_types = str


def parse_uri(uri):
    # TODO: Support Unix sockets.
    def parse_error(uri, msg):
        msg = 'URI "%s": %s' % (uri, msg)
        return None, msg

    if not uri:
        return parse_error(uri, 'should not be None or empty string')
    if not isinstance(uri, string_types):
        return parse_error(uri, 'should be of a string type')
    if uri.count(':') != 1:
        return parse_error(uri, 'does not match host:port scheme')

    host, port_str = uri.split(':', 1)
    if not host:
        return parse_error(uri, 'host value is empty')

    try:
        port = int(port_str)
    except ValueError:
        return parse_error(uri, 'port should be a number')

    return {'host': host, 'port': port}, None


def validate_address(address):
    def format_error(address, err):
        return None, 'Address %s: %s' % (str(address), err)

    if not isinstance(address, dict):
        return format_error(address, 'address must be a dict')

    if 'port' not in address or address['port'] is None:
        return format_error(address, 'port is not set or None')

    if isinstance(address['port'], int):
        # Looks like an inet address.

        # Validate host.
        if 'host' not in address or address['host'] is None:
            return format_error(address,
                                'host is mandatory for an inet address')
        if not isinstance(address['host'], string_types):
            return format_error(address,
                                'host must be a string for an inet address')

        # Validate port.
        if not isinstance(address['port'], int):
            return format_error(address,
                                'port must be an int for an inet address')
        if address['port'] < 1 or address['port'] > 65535:
            return format_error(address, 'port must be in range [1, 65535] '
                                         'for an inet address')

        # Looks okay.
        return True, None
    elif isinstance(address['port'], string_types):
        # Looks like a unix address.

        # Expect no host.
        if 'host' in address and address['host'] is not None:
            return format_error(
                address, 'host must be unset or None for a unix address')

        # Validate port.
        if not isinstance(address['port'], string_types):
            return format_error(address,
                                'port must be a string for a unix address')

        # Looks okay.
        return True, None

    return format_error(address, 'port must be an int or a string')


class RoundRobinStrategy(object):
    """
    Simple round-robin address rotation
    """
    def __init__(self, addrs):
        self.update(addrs)

    def update(self, new_addrs):
        # Verify new_addrs is a non-empty list.
        assert new_addrs and isinstance(new_addrs, list)

        # Remove duplicates.
        new_addrs_unique = []
        for addr in new_addrs:
            if addr not in new_addrs_unique:
                new_addrs_unique.append(addr)
        new_addrs = new_addrs_unique

        # Save a current address if any.
        if 'pos' in self.__dict__ and 'addrs' in self.__dict__:
            current_addr = self.addrs[self.pos]
        else:
            current_addr = None

        # Determine a position of a current address (if any) in
        # the new addresses list.
        if current_addr and current_addr in new_addrs:
            new_pos = new_addrs.index(current_addr)
        else:
            new_pos = -1

        self.addrs = new_addrs
        self.pos = new_pos

    def getnext(self):
        self.pos = (self.pos + 1) % len(self.addrs)
        return self.addrs[self.pos]


class MeshConnection(Connection):
    '''
   Represents a connection to a cluster of Tarantool servers.

   This class uses Connection to connect to one of the nodes of the cluster.
   The initial list of nodes is passed to the constructor in 'addrs' parameter.
   The class set in 'strategy_class' parameter is used to select a node from
   the list and switch nodes in case of unavailability of the current node.

   'cluster_discovery_function' param of the constructor sets the name of a
   stored Lua function used to refresh the list of available nodes. The
   function takes no parameters and returns a list of strings in format
   'host:port'. A generic function for getting the list of nodes looks like
   this:

    .. code-block:: lua

        function get_cluster_nodes()
            return {
                '192.168.0.1:3301',
                '192.168.0.2:3302',
                -- ...
            }
        end

    You may put in this list whatever you need depending on your cluster
    topology. Chances are you'll want to make the list of nodes from nodes'
    replication config. Here is an example for it:

    .. code-block:: lua

        local uri_lib = require('uri')

        function get_cluster_nodes()
            local nodes = {}

            local replicas = box.cfg.replication

            for i = 1, #replicas do
                local uri = uri_lib.parse(replicas[i])

                if uri.host and uri.service then
                    table.insert(nodes, uri.host .. ':' .. uri.service)
                end
            end

            -- if your replication config doesn't contain the current node
            -- you have to add it manually like this:
            table.insert(nodes, '192.168.0.1:3301')

            return nodes
        end
    '''

    def __init__(self, host=None, port=None,
                 user=None,
                 password=None,
                 socket_timeout=SOCKET_TIMEOUT,
                 reconnect_max_attempts=RECONNECT_MAX_ATTEMPTS,
                 reconnect_delay=RECONNECT_DELAY,
                 connect_now=True,
                 encoding=ENCODING_DEFAULT,
                 call_16=False,
                 connection_timeout=CONNECTION_TIMEOUT,
                 addrs=None,
                 strategy_class=RoundRobinStrategy,
                 cluster_discovery_function=None,
                 cluster_discovery_delay=CLUSTER_DISCOVERY_DELAY):
        if addrs is None:
            addrs = []
        else:
            # Don't change user provided arguments.
            addrs = addrs[:]

        if host and port:
            addrs.insert(0, {'host': host, 'port': port})

        # Verify that at least one address is provided.
        if not addrs:
            raise ConfigurationError(
                'Neither "host" and "port", nor "addrs" arguments are set')

        # Verify addresses.
        for addr in addrs:
            ok, msg = validate_address(addr)
            if not ok:
                raise ConfigurationError(msg)

        self.strategy_class = strategy_class
        self.strategy = strategy_class(addrs)

        addr = self.strategy.getnext()
        host = addr['host']
        port = addr['port']

        self.cluster_discovery_function = cluster_discovery_function
        self.cluster_discovery_delay = cluster_discovery_delay
        self.last_nodes_refresh = 0

        super(MeshConnection, self).__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            socket_timeout=socket_timeout,
            reconnect_max_attempts=reconnect_max_attempts,
            reconnect_delay=reconnect_delay,
            connect_now=connect_now,
            encoding=encoding,
            call_16=call_16,
            connection_timeout=connection_timeout)

    def connect(self):
        super(MeshConnection, self).connect()
        if self.connected and self.cluster_discovery_function:
            self._opt_refresh_instances()

    def _opt_reconnect(self):
        '''
        Attempt to connect "reconnect_max_attempts" times to each
        available address.
        '''

        last_error = None
        for _ in range(len(self.strategy.addrs)):
            try:
                super(MeshConnection, self)._opt_reconnect()
                last_error = None
                break
            except NetworkError as e:
                last_error = e
                addr = self.strategy.getnext()
                self.host = addr["host"]
                self.port = addr["port"]

        if last_error:
            raise last_error

    def _opt_refresh_instances(self):
        '''
        Refresh list of tarantool instances in a cluster.
        Reconnect if a current instance was gone from the list.
        '''
        now = time.time()

        if not self.connected or not self.cluster_discovery_function or \
                now - self.last_nodes_refresh < self.cluster_discovery_delay:
            return

        # Call a cluster discovery function w/o reconnection. If
        # something going wrong: warn about that and ignore.
        request = RequestCall(self, self.cluster_discovery_function, (),
                              self.call_16)
        try:
            resp = self._send_request_wo_reconnect(request)
        except DatabaseError as e:
            msg = 'got "%s" error, skipped addresses updating' % str(e)
            warn(msg, ClusterDiscoveryWarning)
            return

        if not resp.data or not resp.data[0] or \
                not isinstance(resp.data[0], list):
            msg = "got incorrect response instead of URI list, " + \
                  "skipped addresses updating"
            warn(msg, ClusterDiscoveryWarning)
            return

        # Validate received address list.
        new_addrs = []
        for uri in resp.data[0]:
            addr, msg = parse_uri(uri)
            if not addr:
                warn(msg, ClusterDiscoveryWarning)
                continue

            ok, msg = validate_address(addr)
            if not ok:
                warn(msg, ClusterDiscoveryWarning)
                continue

            new_addrs.append(addr)

        if not new_addrs:
            msg = "got no correct URIs, skipped addresses updating"
            warn(msg, ClusterDiscoveryWarning)
            return

        self.strategy.update(new_addrs)
        self.last_nodes_refresh = now

        # Disconnect from a current instance if it was gone from
        # an instance list and connect to one of new instances.
        current_addr = {'host': self.host, 'port': self.port}
        if current_addr not in self.strategy.addrs:
            self.close()
            addr = self.strategy.getnext()
            self.host = addr['host']
            self.port = addr['port']
            self._opt_reconnect()

    def _send_request(self, request):
        '''
        Update instances list if "cluster_discovery_function" is provided and a
        last update was more then "cluster_discovery_delay" seconds ago.

        After that perform a request as usual and return an instance of
        `Response` class.

        :param request: object representing a request
        :type request: `Request` instance

        :rtype: `Response` instance
        '''
        self._opt_refresh_instances()
        return super(MeshConnection, self)._send_request(request)
