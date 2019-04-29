import sys
import os
import time
import yaml
from lib.tarantool_server import TarantoolServer
sys.path.append('../tarantool')
from mesh_connection import MeshConnection
from tarantool.const import (
    SOCKET_TIMEOUT,
    RECONNECT_DELAY,
)
from tarantool.error import NetworkError
from tarantool.utils import ENCODING_DEFAULT

INSTANCE_N = 2


def check_connection(con):
    try:
        s = con.space('test')
        print s.select()
    except NetworkError:
        print 'NetworkError !'
    except Exception as e:
        print e


# Start instances
master = server
cluster = [master]
for i in range(INSTANCE_N):
    server = TarantoolServer(server.ini)
    server.script = 'cluster-py/instance%d.lua' % (i+1)
    server.vardir = os.path.join(server.vardir, 'instance', str(i))
    server.deploy()
    server.admin("box.schema.user.grant('guest', 'read,write,execute', 'universe')")
    server.admin("_ = box.schema.space.create('test')")
    server.admin("_ = box.space.test:create_index('primary')")
    server.admin("box.space.test:insert{%d, %s}" % (1, i), silent = True)
    cluster.append(server)

# Make a list of servers
sources = []
for server in cluster[1:]:
    sources.append(yaml.safe_load(server.admin('box.cfg.listen', silent=True))[0])

addrs = []
for addr in sources:
    addrs.append({'host': None, 'port': addr})

con = MeshConnection(addrs=addrs,
                     user=None,
                     password=None,
                     socket_timeout=SOCKET_TIMEOUT,
                     reconnect_max_attempts=0,
                     reconnect_delay=RECONNECT_DELAY,
                     connect_now=True,
                     encoding=ENCODING_DEFAULT)

cluster[0].stop()       # stop server - no effect
check_connection(con)   # instance#1
cluster[1].stop()       # stop instance#1
check_connection(con)   # instance#2
cluster[1].start()      # start instance#1
cluster[2].stop()       # stop instance#2
check_connection(con)   # instance#1 again
cluster[1].stop()       # stop instance#1
check_connection(con)   # both stopped: NetworkError !

master.cleanup()
master.deploy()
