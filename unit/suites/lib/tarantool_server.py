# -*- coding: utf-8 -*-

import os
import os.path
import errno
import shlex
import random
import socket
import tempfile

import time
import shutil
import subprocess

from .tarantool_admin import TarantoolAdmin

def check_port(port, rais=True):
    try:
        sock = socket.create_connection(("localhost", port))
    except socket.error:
        return True
    if rais:
        raise RuntimeError("The server is already running on port {0}".format(port))
    return False

def find_port(port = None):
    if port is None:
        port = random.randrange(3300, 9999)
    while port < 9999:
        if check_port(port, False):
            return port
        port += 1
    return find_port(3300)


class RunnerException(object):
    pass


class TarantoolServer(object):
    default_tarantool = {
            "bin":           "tarantool",
            "logfile":   "tarantool.log",
            "init":           "init.lua"}

    default_cfg = {
            "custom_proc_title": "\"tarantool-python testing\"",
            "slab_alloc_arena":                             0.5,
            "pid_file":                           "\"box.pid\"",
            "rows_per_wal":                                 200}

    @property
    def logfile_path(self):
        return os.path.join(self.vardir, self.default_tarantool['logfile'])

    @property
    def cfgfile_path(self):
        return os.path.join(self.vardir, self.default_tarantool['config'])

    @property
    def script_path(self):
        return os.path.join(self.vardir, self.default_tarantool['init'])

    @property
    def script_dst(self):
        return os.path.join(self.vardir, os.path.basename(self.script))

    @property
    def script(self):
        if not hasattr(self, '_script'): self._script = None
        return self._script

    @script.setter
    def script(self, val):
        if val is None:
            if hasattr(self, '_script'):
                delattr(self, '_script')
            return
        self._script = os.path.abspath(val)

    @property
    def binary(self):
        if not hasattr(self, '_binary'):
            self._binary = self.find_exe()
        return self._binary

    @property
    def _admin(self):
        if not hasattr(self, 'admin'):
            self.admin = None
        return self.admin

    @_admin.setter
    def _admin(self, port):
        try:
            int(port)
        except ValueError:
            raise ValueError("Bad port number: '%s'" % port)
        if hasattr(self, 'admin'):
            del self.admin
        self.admin = TarantoolAdmin('localhost', port)

    @property
    def log_des(self):
        if not hasattr(self, '_log_des'):
            self._log_des = open(self.logfile_path, 'a')
        return self._log_des

    @log_des.deleter
    def log_des(self):
        if not hasattr(self, '_log_des'):
            return
        if not self._log_des.closed:
            self._log_des.close()
        delattr(self, '_log_des')

    def __new__(cls):
        if os.name == 'nt':
            from .remote_tarantool_server import RemoteTarantoolServer
            return RemoteTarantoolServer()
        return super(TarantoolServer, cls).__new__(cls)

    def __init__(self):
        os.popen('ulimit -c unlimited')
        self.host = 'localhost'
        self.args = {}
        self.args['primary'] = find_port()
        self.args['admin'] = find_port(self.args['primary'] + 1)
        self._admin = self.args['admin']
        self.vardir = tempfile.mkdtemp(prefix='var_', dir=os.getcwd())
        self.find_exe()
        self.process = None

    def find_exe(self):
        if 'TARANTOOL_BOX_PATH' in os.environ:
            os.environ["PATH"] = os.environ["TARANTOOL_BOX_PATH"] + os.pathsep + os.environ["PATH"]

        for _dir in os.environ["PATH"].split(os.pathsep):
            exe = os.path.join(_dir, self.default_tarantool["bin"])
            if os.access(exe, os.X_OK):
                return os.path.abspath(exe)
        raise RuntimeError("Can't find server executable in " + os.environ["PATH"])

    def generate_configuration(self):
        os.putenv("PRIMARY_PORT", str(self.args['primary']))
        os.putenv("ADMIN_PORT", str(self.args['admin']))

    def prepare_args(self):
        return shlex.split(self.binary if not self.script else self.script_dst)

    def wait_until_started(self):
        """ Wait until server is started.

        Server consists of two parts:
        1) wait until server is listening on sockets
        2) wait until server tells us his status
        """

        while True:
            try:
                temp = TarantoolAdmin('localhost', self.args['admin'])
                while True:
                    ans = temp('box.info.status')[0]
                    if ans in ('running', 'hot_standby', 'orphan') or ans.startswith('replica'):
                        return True
                    elif ans in ('loading',):
                        continue
                    else:
                        raise Exception("Strange output for `box.info.status`: %s" % (ans))
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    time.sleep(0.1)
                    continue
                raise

    def start(self):
        # Main steps for running Tarantool\Box
        # * Find binary file          --DONE(find_exe -> binary)
        # * Create vardir             --DONE(__init__)
        # * Generate cfgfile          --DONE(generate_configuration)
        # * (MAYBE) Copy init.lua     --INSIDE
        # * Concatenate arguments and
        #   start Tarantool\Box       --DONE(prepare_args)
        # * Wait unitl Tarantool\Box
        #   started                   --DONE(wait_until_started)
        self.generate_configuration()
        if self.script:
            shutil.copy(self.script, self.script_dst)
            os.chmod(self.script_dst, 0o777)
        args = self.prepare_args()
        self.process = subprocess.Popen(args,
                cwd = self.vardir,
                stdout=self.log_des,
                stderr=self.log_des)
        self.wait_until_started()

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            self.process.wait()

    def restart(self):
        self.stop()
        self.start()

    def clean(self):
        if os.path.isdir(self.vardir):
            shutil.rmtree(self.vardir)

    def __del__(self):
        self.stop()
        self.clean()

    def touch_lock(self):
        # A stub method to be compatible with
        # RemoteTarantoolServer.
        pass

    def is_started(self):
        return self.process is not None
