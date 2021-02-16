import socket
import yaml


class TarantoolAdmin(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.is_connected = False
        self.socket = None

    def connect(self):
        self.socket = socket.create_connection((self.host, self.port))
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.is_connected = True
        self.socket.recv(256)  # skip greeting

    def disconnect(self):
        if self.is_connected:
            self.socket.close()
            self.socket = None
            self.is_connected = False

    def reconnect(self):
        self.disconnect()
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()

    def __call__(self, command):
        return self.execute(command)

    def execute(self, command):
        if not command:
            return

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
