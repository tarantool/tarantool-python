"""
This module provides API for interaction with Tarantool servers cluster.
"""

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
    DEFAULT_TRANSPORT,
    DEFAULT_SSL_KEY_FILE,
    DEFAULT_SSL_CERT_FILE,
    DEFAULT_SSL_CA_FILE,
    DEFAULT_SSL_CIPHERS,
    DEFAULT_SSL_PASSWORD,
    DEFAULT_SSL_PASSWORD_FILE,
    CLUSTER_DISCOVERY_DELAY,
)

from tarantool.request import (
    RequestCall
)

default_addr_opts = {
    'transport': DEFAULT_TRANSPORT,
    'ssl_key_file': DEFAULT_SSL_KEY_FILE,
    'ssl_cert_file': DEFAULT_SSL_CERT_FILE,
    'ssl_ca_file': DEFAULT_SSL_CA_FILE,
    'ssl_ciphers': DEFAULT_SSL_CIPHERS,
    'ssl_password': DEFAULT_SSL_PASSWORD,
    'ssl_password_file': DEFAULT_SSL_PASSWORD_FILE,
    'auth_type': None,
}


def parse_uri(uri):
    """
    Parse URI received from cluster discovery function.

    :param uri: URI received from cluster discovery function
    :type uri: :obj:`str`

    :return: First value: `{"host": host, "port": port}` or ``None`` in
        case of fail, second value: ``None`` or error message in case of
        fail.
    :rtype: first value: :obj:`dict` or ``None``,
        second value: ``None`` or :obj:`str`
    """

    # TODO: Support Unix sockets.
    def parse_error(uri, msg):
        msg = 'URI "%s": %s' % (uri, msg)
        return None, msg

    if not uri:
        return parse_error(uri, 'should not be None or empty string')
    if not isinstance(uri, str):
        return parse_error(uri, 'should be of a string type')
    if uri.count(':') != 1:
        return parse_error(uri, 'does not match host:port scheme')

    host, opts_str = uri.split(':', 1)
    if not host:
        return parse_error(uri, 'host value is empty')

    opts_array = opts_str.split('?', 1)
    port_str = opts_array[0]
    if len(opts_array) > 1:
        opts_str = opts_array[1]
    else:
        opts_str = ""

    result = {'host': host}
    try:
        result['port'] = int(port_str)
    except ValueError:
        return parse_error(uri, 'port should be a number')

    for k, v in default_addr_opts.items():
        result[k] = v

    if opts_str != "":
        for opt_str in opts_str.split('&'):
            opt = opt_str.split('=')
            if len(opt) != 2:
                continue
            for k in default_addr_opts:
                if k == opt[0]:
                    result[k] = opt[1]

    return result, None


def prepare_address(address):
    """
    Validate address dictionary, fill with default values.
    For format refer to
    :paramref:`~tarantool.ConnectionPool.params.addrs`.

    :param address: Address dictionary.
    :type address: :obj:`dict`

    :return: Address dictionary or ``None`` in case of failure, second
        value: ``None`` or error message in case of failure.
    :rtype: first value: :obj:`dict` or ``None``,
        second value: ``None`` or :obj:`str`
    """

    def format_error(address, err):
        return None, 'Address %s: %s' % (str(address), err)

    if not isinstance(address, dict):
        return format_error(address, 'address must be a dict')

    if 'port' not in address or address['port'] is None:
        return format_error(address, 'port is not set or None')

    result = {}
    for k, v in address.items():
        result[k] = v
    # Set default values.
    for k, v in default_addr_opts.items():
        if k not in result:
            result[k] = v

    if isinstance(result['port'], int):
        # Looks like an inet address.

        # Validate host.
        if 'host' not in result or result['host'] is None:
            return format_error(result,
                                'host is mandatory for an inet result')
        if not isinstance(result['host'], str):
            return format_error(result,
                                'host must be a string for an inet result')

        # Validate port.
        if not isinstance(result['port'], int):
            return format_error(result,
                                'port must be an int for an inet result')
        if result['port'] < 1 or result['port'] > 65535:
            return format_error(result, 'port must be in range [1, 65535] '
                                         'for an inet result')

        # Looks okay.
        return result, None
    elif isinstance(result['port'], str):
        # Looks like a unix address.

        # Expect no host.
        if 'host' in result and result['host'] is not None:
            return format_error(
                result, 'host must be unset or None for a unix result')

        # Validate port.
        if not isinstance(result['port'], str):
            return format_error(result,
                                'port must be a string for a unix result')

        # Looks okay.
        return result, None

    return format_error(result, 'port must be an int or a string')


def update_connection(conn, address):
    """
    Update connection info after rotation.

    :param conn: Connection mesh to update.
    :type conn: :class:`~tarantool.MeshConnection`

    :param address: New active connection address.
    :type address: :obj:`dict`
    """

    conn.host = address["host"]
    conn.port = address["port"]
    conn.transport = address['transport']
    conn.ssl_key_file = address['ssl_key_file']
    conn.ssl_cert_file = address['ssl_cert_file']
    conn.ssl_ca_file = address['ssl_ca_file']
    conn.ssl_ciphers = address['ssl_ciphers']
    conn.ssl_password = address['ssl_password']
    conn.ssl_password_file = address['ssl_password_file']
    conn.auth_type = address['auth_type']


class RoundRobinStrategy(object):
    """
    Defines strategy to choose next pool server after fail.
    """

    def __init__(self, addrs):
        """
        :param addrs: Server addresses list, refer to
            :paramref:`~tarantool.ConnectionPool.params.addrs`.
        :type addrs: :obj:`list` of :obj:`dict`
        """
        self.update(addrs)

    def update(self, new_addrs):
        """
        Refresh the strategy state with new addresses.

        :param new_addrs: Updated server addresses list.
        :type addrs: :obj:`list` of :obj:`dict`
        """

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
        """
        Get next cluster server.

        :return: Server address.
        :rtype: :obj:`dict`
        """

        self.pos = (self.pos + 1) % len(self.addrs)
        return self.addrs[self.pos]


class MeshConnection(Connection):
    """
    Represents a connection to a cluster of Tarantool servers.
    """

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
                 transport=DEFAULT_TRANSPORT,
                 ssl_key_file=DEFAULT_SSL_KEY_FILE,
                 ssl_cert_file=DEFAULT_SSL_CERT_FILE,
                 ssl_ca_file=DEFAULT_SSL_CA_FILE,
                 ssl_ciphers=DEFAULT_SSL_CIPHERS,
                 ssl_password=DEFAULT_SSL_PASSWORD,
                 ssl_password_file=DEFAULT_SSL_PASSWORD_FILE,
                 auth_type=None,
                 addrs=None,
                 strategy_class=RoundRobinStrategy,
                 cluster_discovery_function=None,
                 cluster_discovery_delay=CLUSTER_DISCOVERY_DELAY):
        """
        :param host: Refer to
            :paramref:`~tarantool.Connection.params.host`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param port: Refer to
            :paramref:`~tarantool.Connection.params.host`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param user: Refer to
            :paramref:`~tarantool.Connection.params.user`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param password: Refer to
            :paramref:`~tarantool.Connection.params.password`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param socket_timeout: Refer to
            :paramref:`~tarantool.Connection.params.socket_timeout`.
            Value would be used for the current active connection.

        :param reconnect_max_attempts: Refer to
            :paramref:`~tarantool.Connection.params.reconnect_max_attempts`.
            Value would be used for the current active connection.

        :param reconnect_delay: Refer to
            :paramref:`~tarantool.Connection.params.reconnect_delay`.
            Value would be used for the current active connection.

        :param connect_now: If ``True``, connect to server on
            initialization. Otherwise, you have to call
            :meth:`~tarantool.MeshConnection.connect` manually after
            initialization.
        :type connect_now: :obj:`bool`, optional

        :param encoding: Refer to
            :paramref:`~tarantool.Connection.params.encoding`.
            Value would be used for the current active connection.

        :param call_16: Refer to
            :paramref:`~tarantool.Connection.params.call_16`.
            Value would be used for the current active connection.

        :param connection_timeout: Refer to
            :paramref:`~tarantool.Connection.params.connection_timeout`.
            Value would be used for the current active connection.

        :param transport: Refer to
            :paramref:`~tarantool.Connection.params.transport`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param ssl_key_file: Refer to
            :paramref:`~tarantool.Connection.params.ssl_key_file`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param ssl_cert_file: Refer to
            :paramref:`~tarantool.Connection.params.ssl_cert_file`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param ssl_ca_file: Refer to
            :paramref:`~tarantool.Connection.params.ssl_ca_file`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param ssl_ciphers: Refer to
            :paramref:`~tarantool.Connection.params.ssl_ciphers`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param auth_type: Refer to
            :paramref:`~tarantool.Connection.params.auth_type`.
            Value would be used to add one more server in
            :paramref:`~tarantool.MeshConnection.params.addrs` list.

        :param addrs: Cluster servers addresses list. Refer to
            :paramref:`~tarantool.ConnectionPool.params.addrs`.

        :param strategy_class: Strategy for choosing a server after
            the current server fails. Defaults to the round-robin
            strategy.
        :type strategy_class: :obj:`object`, optional

        :param cluster_discovery_function: sets the name of the stored
            Lua function used to refresh the list of available nodes.
            The function takes no parameters and returns a list of
            strings in the format ``'host:port'``. A generic function
            for getting the list of nodes looks like this:

            .. code-block:: lua

                function get_cluster_nodes()
                    return {
                        '192.168.0.1:3301',
                        '192.168.0.2:3302?transport=ssl&ssl_ca_file=/path/to/ca.cert',
                        -- ...
                    }
                end

            You can put in this list whatever you need, depending on
            your cluster topology. Chances are you'll want to derive
            the list of nodes from the nodes' replication configuration.
            Here is an example:

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

                    -- if your replication config doesn't contain the current node,
                    -- you have to add it manually like this:
                    table.insert(nodes, '192.168.0.1:3301')

                    return nodes
                end

        :type cluster_discovery_function: :obj:`str` or :obj:`None`,
            optional

        :param cluster_discovery_delay: Minimal time between address
            list refresh.
        :type cluster_discovery_delay: :obj:`float`, optional

        :raises: :exc:`~tarantool.error.ConfigurationError`,
            :class:`~tarantool.Connection` exceptions,
            :class:`~tarantool.MeshConnection.connect` exceptions
        """

        if addrs is None:
            addrs = []
        else:
            # Don't change user provided arguments.
            addrs = addrs[:]

        if host and port:
            addrs.insert(0, {'host': host,
                             'port': port,
                             'transport': transport,
                             'ssl_key_file': ssl_key_file,
                             'ssl_cert_file': ssl_cert_file,
                             'ssl_ca_file': ssl_ca_file,
                             'ssl_ciphers': ssl_ciphers,
                             'ssl_password': ssl_password,
                             'ssl_password_file': ssl_password_file,
                             'auth_type': auth_type})

        # Verify that at least one address is provided.
        if not addrs:
            raise ConfigurationError(
                'Neither "host" and "port", nor "addrs" arguments are set')

        # Prepare addresses for usage.
        new_addrs = []
        for addr in addrs:
            new_addr, msg = prepare_address(addr)
            if not new_addr:
                raise ConfigurationError(msg)
            new_addrs.append(new_addr)
        addrs = new_addrs

        self.strategy_class = strategy_class
        self.strategy = strategy_class(addrs)
        addr = self.strategy.getnext()

        self.cluster_discovery_function = cluster_discovery_function
        self.cluster_discovery_delay = cluster_discovery_delay
        self.last_nodes_refresh = 0

        super(MeshConnection, self).__init__(
            host=addr['host'],
            port=addr['port'],
            user=user,
            password=password,
            socket_timeout=socket_timeout,
            reconnect_max_attempts=reconnect_max_attempts,
            reconnect_delay=reconnect_delay,
            connect_now=connect_now,
            encoding=encoding,
            call_16=call_16,
            connection_timeout=connection_timeout,
            transport=addr['transport'],
            ssl_key_file=addr['ssl_key_file'],
            ssl_cert_file=addr['ssl_cert_file'],
            ssl_ca_file=addr['ssl_ca_file'],
            ssl_ciphers=addr['ssl_ciphers'],
            ssl_password=addr['ssl_password'],
            ssl_password_file=addr['ssl_password_file'],
            auth_type=addr['auth_type'])

    def connect(self):
        """
        Create a connection to some server in the cluster. Refresh
        addresses info after success. There is no need to call this
        method explicitly until you have set ``connect_now=False`` on
        initialization.

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :class:`~tarantool.Connection.connect` exceptions
        """
        super(MeshConnection, self).connect()
        if self.connected and self.cluster_discovery_function:
            self._opt_refresh_instances()

    def _opt_reconnect(self):
        """
        Attempt to connect
        :paramref:`~tarantool.MeshConnection.reconnect_max_attempts`
        times to each available address.

        :raise: :class:`~tarantool.Connection.connect` exceptions
        """

        last_error = None
        for _ in range(len(self.strategy.addrs)):
            try:
                super(MeshConnection, self)._opt_reconnect()
                last_error = None
                break
            except NetworkError as e:
                last_error = e
                addr = self.strategy.getnext()
                update_connection(self, addr)

        if last_error:
            raise last_error

    def _opt_refresh_instances(self):
        """
        Refresh the list of Tarantool instances in a cluster.
        Reconnect if the current instance has disappeared from the list.

        :raise: :exc:`~AssertionError`,
            :exc:`~tarantool.error.SchemaError`,
            :exc:`~tarantool.error.NetworkError`,
            :class:`~tarantool.MeshConnection._opt_reconnect` exceptions
        """

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
            msg = 'got "%s" error, skipped address updates' % str(e)
            warn(msg, ClusterDiscoveryWarning)
            return

        if not resp.data or not resp.data[0] or \
                not isinstance(resp.data[0], list):
            msg = "got incorrect response instead of URI list, " + \
                  "skipped address updates"
            warn(msg, ClusterDiscoveryWarning)
            return

        # Prepare for usage received address list.
        new_addrs = []
        for uri in resp.data[0]:
            addr, msg = parse_uri(uri)
            if not addr:
                warn(msg, ClusterDiscoveryWarning)
                continue

            new_addr, msg = prepare_address(addr)
            if not new_addr:
                warn(msg, ClusterDiscoveryWarning)
                continue

            new_addrs.append(new_addr)

        if not new_addrs:
            msg = "got no correct URIs, skipped address updates"
            warn(msg, ClusterDiscoveryWarning)
            return

        self.strategy.update(new_addrs)
        self.last_nodes_refresh = now

        # Disconnect from a current instance if it was gone from
        # an instance list and connect to one of new instances.
        current_addr = {'host': self.host,
                        'port': self.port,
                        'transport': self.transport,
                        'ssl_key_file': self.ssl_key_file,
                        'ssl_cert_file': self.ssl_cert_file,
                        'ssl_ca_file': self.ssl_ca_file,
                        'ssl_ciphers': self.ssl_ciphers,
                        'ssl_password': self.ssl_password,
                        'ssl_password_file': self.ssl_password_file,
                        'auth_type': self._client_auth_type}
        if current_addr not in self.strategy.addrs:
            self.close()
            addr = self.strategy.getnext()
            update_connection(self, addr)
            self._opt_reconnect()

    def _send_request(self, request, on_push=None, on_push_ctx=None):
        """
        Send a request to a Tarantool server. If required, refresh
        addresses list before sending a request.

        :param request: Request to send.
        :type request: :class:`~tarantool.request.Request`

        :rtype: :class:`~tarantool.response.Response`

        :raise: :class:`~tarantool.MeshConnection._opt_reconnect` exceptions,
             :class:`~tarantool.Connection._send_request` exceptions
        """

        self._opt_refresh_instances()
        return super(MeshConnection, self)._send_request(request, on_push, on_push_ctx)
