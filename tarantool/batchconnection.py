import select

import msgpack

from tarantool.connection import Connection
from tarantool.error import (
    NetworkError
)

def BatchException(Exception):
    pass

def BatchConnection(Connection):
    def __init__(self):
        super(BatchConnection, self).__init__()
        # Batch mode flag
        self.batch = False
        # Write Buffer
        # We feed him data in batch mode
        self.wbuffer  = msgpack.Packer(autoreset=False)
        # Read Buffer
        # We feed him data that we'll recv from socket
        self.rbuffer  = msgpack.Unpacker(use_list=True)
        # Mapping for requests <int>:<response_body> and request count
        self.reqdict  = {}
        self.reqcount = 0
        # Poller and poll timeout
        self.poller   = select.poll()
        self.timeout  = 0

    def connect_basic(self):
        super(BatchConnection, self).connect_basic()
        self.poll_register()

    def begin(self):
        if self.batch:
            raise BatchException("Already batch mode")
        self.batch = True

    def commit(self):
        pass

    def rollback(self):
        if not self.batch:
            raise BatchException("Can't rollback in non-batch mode")
        self.batch = False
        self.wbuffer.reset()

    def poll_register(self):
        if self._socket is None:
            raise BatchException("Can't register socket in poller")
        mode = select.POLLIN | select.POLLPRI | select.POLLHUP
        mode = mode | select.POLLERR | select.POLLOUT
        self.poller.register(self._socket, mode)

    def poll_unregister(self):
        if self._socket is None:
            raise BatchException("Can't unregister socket in poller")
        self.poller.unregister(self._socket)

    def check_flag(self, flag):
        events = self.poller.poll(self.timeout)
        if len(events) > 0:
            pollerr_val = events[0][1] & select.POLLERR
            pollnval_val = events[0][1] & select.POLLNVAL
            if (pollerr_val || pollnval_val):
                raise BatchException("Error while checking status: POLLERR %d," +
                        " POLLNVAL %d" % (pollerr_val, pollnval_val))
            return ((events[0][1] & flag) > 0)
        return False

    def check_readable(self):
        return self.check_flag(select.POLLIN | select.POLLPRI)

    def check_writeable(self):
        return self.check_flag(select.POLLOUT)

    def check_closed(self):
        return self.check_flag(select.POLLHUP)

    def send_requests(self):
        self._socket.sendall(bytes(self.wbuffer))
        self.wbuffer.reset()

    def recv_parse(self):
        while True:
            try:
                val = self.rbuffer.unpack()
                return val
            except msgpack.OutOfData as e:
                self.rbuffer.feed(self._socket.recv(16384))
                continue

    def recv_requests(self, count):
        while self.check_readable():
            self.rbuffer.feed(self._socket.recv(16384))
        while count:
            rlen = self.recv_parse()
            rheader = self.recv_parse()
            rbody = self.recv_parse()
            count = count - 1
            sync = rheader.get(IPROTO_SYNC, None)
            assert (sync is not None)
            assert (self.reqdict.get(sync, 0) is None)
            self.reqdict[sync] = [rheader, rbody]

