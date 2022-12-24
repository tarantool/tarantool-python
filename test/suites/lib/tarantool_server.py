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

from tarantool.const import (
    SSL_TRANSPORT
)


def check_port(port, rais=True):
    try:
        sock = socket.create_connection(("0.0.0.0", port))
    except socket.error:
        return True
    sock.close()

    if rais:
        raise RuntimeError("The server is already running on port {0}".format(port))
    return False


def find_port(port=None):
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
            "bin":     "tarantool",
            "logfile": "tarantool.log",
            "init":    "init.lua"}

    default_cfg = {
            "custom_proc_title": "\"tarantool-python testing\"",
            "memtx_memory":      0.5 * 1024**3,  # 0.5 GiB
            "pid_file":          "\"box.pid\""}

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
        self.admin = TarantoolAdmin('0.0.0.0', port)

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

    def __new__(cls,
                transport=None,
                ssl_key_file=None,
                ssl_cert_file=None,
                ssl_ca_file=None,
                ssl_ciphers=None,
                ssl_password=None,
                ssl_password_file=None,
                create_unix_socket=False,
                auth_type=None):
        if os.name == 'nt':
            from .remote_tarantool_server import RemoteTarantoolServer
            return RemoteTarantoolServer()
        return super(TarantoolServer, cls).__new__(cls)

    def __init__(self,
                 transport=None,
                 ssl_key_file=None,
                 ssl_cert_file=None,
                 ssl_ca_file=None,
                 ssl_ciphers=None,
                 ssl_password=None,
                 ssl_password_file=None,
                 create_unix_socket=False,
                 auth_type=None):
        os.popen('ulimit -c unlimited').close()

        if create_unix_socket:
            self.host = None
            self.args = {}
            self._socket = tempfile.NamedTemporaryFile(suffix='.sock', delete=False)
            self.args['primary'] = self._socket.name
            self.args['admin'] = find_port()
        else:
            self.host = '0.0.0.0'
            self.args = {}
            self._socket = None
            self.args['primary'] = find_port()
            self.args['admin'] = find_port(self.args['primary'] + 1)

        self._admin = self.args['admin']
        self.vardir = tempfile.mkdtemp(prefix='var_', dir=os.getcwd())
        self.find_exe()
        self.process = None
        self.transport = transport
        self.ssl_key_file = ssl_key_file
        self.ssl_cert_file = ssl_cert_file
        self.ssl_ca_file = ssl_ca_file
        self.ssl_ciphers = ssl_ciphers
        self.ssl_password = ssl_password
        self.ssl_password_file = ssl_password_file
        self.auth_type = auth_type

    def find_exe(self):
        if 'TARANTOOL_BOX_PATH' in os.environ:
            os.environ["PATH"] = os.environ["TARANTOOL_BOX_PATH"] + os.pathsep + os.environ["PATH"]

        for _dir in os.environ["PATH"].split(os.pathsep):
            exe = os.path.join(_dir, self.default_tarantool["bin"])
            if os.access(exe, os.X_OK):
                return os.path.abspath(exe)
        raise RuntimeError("Can't find server executable in " + os.environ["PATH"])

    def generate_listen(self, port, port_only):
        if not port_only and self.transport == SSL_TRANSPORT:
            listen = self.host + ":" + str(port) + "?transport=ssl&"
            if self.ssl_key_file:
                listen += "ssl_key_file={}&".format(self.ssl_key_file)
            if self.ssl_cert_file:
                listen += "ssl_cert_file={}&".format(self.ssl_cert_file)
            if self.ssl_ca_file:
                listen += "ssl_ca_file={}&".format(self.ssl_ca_file)
            if self.ssl_ciphers:
                listen += "ssl_ciphers={}&".format(self.ssl_ciphers)
            if self.ssl_password:
                listen += "ssl_password={}&".format(self.ssl_password)
            if self.ssl_password_file:
                listen += "ssl_password_file={}&".format(self.ssl_password_file)
            listen = listen[:-1]
        else:
            listen = str(port)
        return listen

    def generate_configuration(self):
        primary_listen = self.generate_listen(self.args['primary'], False)
        admin_listen = self.generate_listen(self.args['admin'], True)
        os.putenv("LISTEN", primary_listen)
        os.putenv("ADMIN", admin_listen)
        if self.auth_type is not None:
            os.putenv("AUTH_TYPE", self.auth_type)
        else:
            os.putenv("AUTH_TYPE", "")

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
                temp = TarantoolAdmin('0.0.0.0', self.args['admin'])
                while True:
                    ans = temp('box.info.status')[0]
                    if ans in ('running', 'hot_standby', 'orphan') or ans.startswith('replica'):
                        temp.disconnect()
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
                cwd=self.vardir,
                stdout=self.log_des,
                stderr=self.log_des)
        self.wait_until_started()

    def stop(self):
        self.admin.disconnect()
        if self.process.poll() is None:
            self.process.terminate()
            self.process.wait()

    def restart(self):
        self.stop()
        self.start()

    def clean(self):
        if os.path.isdir(self.vardir):
            shutil.rmtree(self.vardir)

        if os.path.exists(self.args['primary']):
            os.remove(self.args['primary'])

        if (self._socket is not None) and (not self._socket.file.closed):
            self._socket.close()

        del self.log_des

    def __del__(self):
        self.stop()
        self.clean()

    def touch_lock(self):
        # A stub method to be compatible with
        # RemoteTarantoolServer.
        pass

    def is_started(self):
        return self.process is not None
