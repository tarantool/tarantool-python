import os
import errno
import shlex
import random
import socket
import tempfile

import yaml
import time
import shutil
import subprocess

def check_port(port, rais=True):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
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


class TarantoolAdmin(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.is_connected = False
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def connect(self):
        self.socket.connect((self.host, self.port))
        self.is_connected = True

    def disconnect(self):
        if self.is_connected:
            self.socket.close()
            self.is_connected = False

    def reconnect(self):
        self.disconnect()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.connect()

    def opt_reconnect(self):
        """ On a socket which was disconnected, recv of 0 bytes immediately
            returns with no data. On a socket which is alive, it returns EAGAIN.
            Make use of this property and detect whether or not the socket is
            dead. Reconnect a dead socket, do nothing if the socket is good."""
        try:
            if self.socket.recv(0, socket.MSG_DONTWAIT) == '':
                self.reconnect()
        except socket.error as e:
            if e.errno == errno.EAGAIN:
                pass
            else:
                self.reconnect()

    def execute(self, command):
        self.opt_reconnect()
        return self.execute_no_reconnect(command)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()

    def __call__(self, command):
        return self.execute(command)

    def execute_no_reconnect(self, command):
        if not command:
            return
        cmd = command.replace('\n', ' ') + '\n'
        self.socket.sendall(cmd)

        bufsiz = 4096
        res = ""

        while True:
            buf = self.socket.recv(bufsiz)
            if not buf:
                break
            res = res + buf
            if (res.rfind("\n...\n") >= 0 or res.rfind("\r\n...\r\n") >= 0):
                break

        return yaml.load(res)

class TarantoolServer(object):
    default_tarantool = {
            "bin":       "tarantool_box",
            "config":    "tarantool.cfg",
            "logfile":   "tarantool.log",
            "init":           "init.lua"}

    default_cfg = {
            "custom_proc_title": "\"tarantool-python testing\"",
            "slab_alloc_arena":                             0.5,
            "pid_file":                           "\"box.pid\"",
            "rows_per_wal":                                 200}

    config_template = (
        "slab_alloc_arena = {slab_alloc_arena}\n"
        "custom_proc_title = {custom_proc_title}\n"
        "pid_file = {pid_file}\n"
        "primary_port = {primary}\n"
        "admin_port = {admin}\n"
        "rows_per_wal = {rows_per_val}")
    @property
    def logfile_path(self):
        return os.path.join(self.vardir, self.default_tarantool['logfile'])

    @property
    def cfgfile_path(self):
        return os.path.join(self.vardir, self.default_tarantool['config'])

    @property
    def initlua_path(self):
        return os.path.join(self.vardir, self.default_tarantool['init'])

    @property
    def initlua_source(self):
        if not hasattr(self, '_initlua_source'):
            self._initlua_source = None
        return self._initlua_source
    @initlua_source.setter
    def initlua_source(self, val):
        if val is None:
            self._initlua_source = None
        self._initlua_source = os.path.abspath(val)

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
        if not hasattr(self, 'admin') or self.admin is None:
            self.admin = TarantoolAdmin('localhost', port)
            return
        if self.admin.port != port:
            self.admin.port = port
            self.admin.reconnect()

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


    # self.logfile

    def __init__(self):
        os.popen('ulimit -c unlimited')
        self.args = {}
        self.args['primary'] = find_port()
        self.args['admin'] = find_port(self.args['primary'] + 1)
        self._admin = self.args['admin']
        self.vardir = tempfile.mkdtemp(prefix='var_', dir=os.getcwd())

    def find_exe(self):
        path = os.environ['PATH']
        if 'TARANTOOL_BOX_PATH' in os.environ:
            path = os.environ['TARANTOOL_BOX_PATH'] + os.pathsep + path

        for _dir in path.split(os.pathsep):
            exe = os.path.join(_dir, self.default_tarantool["bin"])
            if os.access(exe, os.X_OK):
                return os.path.abspath(exe)
        raise RuntimeError("Can't find server executable in " + path)

    def generate_configuration(self):
        with open(self.cfgfile_path, 'w') as cfgfile:
            args = dict(self.default_cfg)
            args.update(self.args)
            for i in self.config_template.splitlines():
                try:
                    cfgfile.write(i.format(**args) + '\n')
                except (IndexError, KeyError):
                    continue

    def prepare_args(self):
        return shlex.split(self.binary + ' ' + (self.initlua_path if self.initlua_source is not None else ''))

    def wait_until_started(self):
        """ Wait until server is started.

        Server consists of two parts:
        1) wait until server is listening on sockets
        2) wait until server tells us his status

        """

        while True:
            try:
                temp = TarantoolAdmin('localhost', self.args['admin'])
                ans = temp('box.info.status')[0]
                if ans in ('primary', 'hot_standby', 'orphan') or ans.startswith('replica'):
                    return True
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
        if self.initlua_source:
            shutil.copy(self.initlua_source, self.initlua_path)
        args = self.prepare_args()
        self.process = subprocess.Popen(args,
                cwd = self.vardir,
                stdout=self.log_des,
                stderr=self.log_des)
        self.wait_until_started()

    def stop(self):
        self.process.terminate()
        self.process.wait()

    def restart(self):
        self.stop()
        self.start()

    def clean(self):
        shutil.rmtree(self.vardir)

    def __del__(self):
        self.stop()
        self.clean()

