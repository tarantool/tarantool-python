"""
This module provides helpers to work with remote Tarantool server
(used on Windows).
"""

import sys
import os
import random
import string
import time

from .tarantool_admin import TarantoolAdmin


# a time during which try to acquire a lock
AWAIT_TIME = 60  # seconds

# on which port bind a socket for binary protocol
BINARY_PORT = 3301


def get_random_string():
    """
    :type: :obj:`str`
    """
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(16))


class RemoteTarantoolServer():
    """
    Class to work with remote Tarantool server.
    """

    def __init__(self):
        self.host = os.environ['REMOTE_TARANTOOL_HOST']

        self.args = {}
        self.args['primary'] = BINARY_PORT
        self.args['admin'] = os.environ['REMOTE_TARANTOOL_CONSOLE_PORT']

        assert self.args['primary'] != self.args['admin']

        # a name to using for a lock
        self.whoami = get_random_string()

        self.admin = TarantoolAdmin(self.host, self.args['admin'])
        self.lock_is_acquired = False

        # emulate stopped server
        self.acquire_lock()
        self.admin.execute('box.cfg{listen = box.NULL}')

    def acquire_lock(self):
        """
        Acquire lock on the remote server so no concurrent tests would run.
        """

        deadline = time.time() + AWAIT_TIME
        while True:
            res = self.admin.execute(f'return acquire_lock("{ self.whoami}")')
            ok = res[0]
            err = res[1] if not ok else None
            if ok:
                break
            if time.time() > deadline:
                raise RuntimeError(f'can not acquire "{self.whoami}" lock: {str(err)}')
            print(f'waiting to acquire "{self.whoami}" lock',
                  file=sys.stderr)
            time.sleep(1)
        self.lock_is_acquired = True

    def touch_lock(self):
        """
        Refresh locked state on the remote server so no concurrent
        tests would run.
        """

        assert self.lock_is_acquired
        res = self.admin.execute(f'return touch_lock("{self.whoami}")')
        ok = res[0]
        err = res[1] if not ok else None
        if not ok:
            raise RuntimeError(f'can not update "{self.whoami}" lock: {str(err)}')

    def release_lock(self):
        """
        Release loack so another test suite can run on the remote
        server.
        """

        res = self.admin.execute(f'return release_lock("{self.whoami}")')
        ok = res[0]
        err = res[1] if not ok else None
        if not ok:
            raise RuntimeError(f'can not release "{self.whoami}" lock: {str(err)}')
        self.lock_is_acquired = False

    def start(self):
        """
        Initialize the work with the remote server.
        """

        if not self.lock_is_acquired:
            self.acquire_lock()
        self.admin.execute(f'box.cfg{{listen = "0.0.0.0:{self.args["primary"]}"}}')

    def stop(self):
        """
        Finish the work with the remote server.
        """

        self.admin.execute('box.cfg{listen = box.NULL}')
        self.release_lock()

    def is_started(self):
        """
        Check if we still work with the remote server.
        """

        return self.lock_is_acquired

    def clean(self):
        """
        Do nothing.
        """

    def __del__(self):
        self.admin.disconnect()
