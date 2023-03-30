"""
This module provides helpers to setup running Tarantool server.
"""

import socket
import re

import pkg_resources
import yaml


class TarantoolAdmin():
    """
    Class to setup running Tarantool server.
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.is_connected = False
        self.socket = None
        self._tnt_version = None

    def connect(self):
        """
        Connect to running Tarantool server.
        """

        self.socket = socket.create_connection((self.host, self.port))
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.is_connected = True
        self.socket.recv(256)  # skip greeting

    def disconnect(self):
        """
        Disconnect from the Tarantool server.
        """

        if self.is_connected:
            self.socket.close()
            self.socket = None
            self.is_connected = False

    def reconnect(self):
        """
        Reconnect to the running Tarantool server.
        """

        self.disconnect()
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.disconnect()

    def __call__(self, command):
        return self.execute(command)

    def execute(self, command):
        """
        Evaluate some Lua code on the Tarantool server.
        """

        if not command:
            return None

        if not self.is_connected:
            self.connect()

        cmd = (command.replace('\n', ' ') + '\n').encode()
        try:
            self.socket.sendall(cmd)
        except socket.error:
            # reconnect and try again
            self.reconnect()
            self.socket.sendall(cmd)

        bufsiz = 4096
        res = ""

        while True:
            buf = self.socket.recv(bufsiz)
            if not buf:
                break
            res = res + buf.decode()
            if (res.rfind("\n...\n") >= 0 or res.rfind("\r\n...\r\n") >= 0):
                break

        return yaml.safe_load(res)

    @property
    def tnt_version(self):
        """
        Connected Tarantool server version.
        """

        if self._tnt_version is not None:
            return self._tnt_version

        raw_version = re.match(
            r'[\d.]+', self.execute('box.info.version')[0]
        ).group()

        self._tnt_version = pkg_resources.parse_version(raw_version)

        return self._tnt_version
