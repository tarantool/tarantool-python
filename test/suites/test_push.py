import sys
import unittest
import tarantool
from .lib.tarantool_server import TarantoolServer


def create_server():
    srv = TarantoolServer()
    srv.script = 'test/suites/box.lua'
    srv.start()
    srv.admin("box.schema.user.create('test', {password = 'test', " +
              "if_not_exists = true})")
    srv.admin("box.schema.user.grant('test', 'read,write,execute', 'universe')")

    # Create server_function (for testing purposes).
    srv.admin("""
    function server_function()
        x = {0,0}
        while x[1] < 3 do
            x[1] = x[1] + 1
            box.session.push(x)
        end
        return x
    end
    """)

    # Create tester space and on_replace trigger (for testing purposes).
    srv.admin("""
    box.schema.create_space(
        'tester', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'name', type = 'string'},
        }
    })
    """)
    srv.admin("""
    box.space.tester:create_index(
        'primary_index', {
        parts = {
            {field = 1, type = 'unsigned'},
        }
    })
    """)
    srv.admin("""
    box.space.tester:create_index(
        'primary_index', {
        parts = {
            {field = 1, type = 'unsigned'},
        }
    })
    """)
    srv.admin("""
    function on_replace_callback()
        x = {0,0}
        while x[1] < 300 do
            x[1] = x[1] + 100
            box.session.push(x)
        end
    end
    """)
    srv.admin("""
    box.space.tester:on_replace(
        on_replace_callback
    )
    """)

    return srv


# Callback for on_push arg (for testing purposes).
def push_callback(data, on_push_ctx=[]):
    data[0][1] = data[0][1] + 1
    on_push_ctx.append(data)


class TestSuite_Push(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print(' PUSH '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        # Create server and extract helpful fields for tests.
        self.srv = create_server()
        self.host = self.srv.host
        self.port = self.srv.args['primary']

    def setUp(self):
        # Open connection, connection pool and mesh connection to instance.
        self.conn = tarantool.Connection(host=self.host, port=self.port, 
                                         user='test', password='test')
        self.conn_pool = tarantool.ConnectionPool([{'host':self.host, 'port':self.port}], 
                                                     user='test', password='test')
        self.mesh_conn = tarantool.MeshConnection(host=self.host, port=self.port, 
                                                  user='test', password='test')

    push_test_cases = {
        'call': {
            'input': {
                'args': ['server_function'],
                'kwargs': {
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[1, 1]], [[2, 1]], [[3, 1]]],
                'resp': [3, 0],
            },
        },
        'eval': {
            'input': {
                'args': ['return server_function()'],
                'kwargs': {
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[1, 1]], [[2, 1]], [[3, 1]]],
                'resp': [3, 0],
            },
        },
        'insert': {
            'input': {
                'args': ['tester', (1, 'Mike')],
                'kwargs': {
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[100, 1]], [[200, 1]], [[300, 1]]],
                'resp': [1, 'Mike'],
            },
        },
        'replace': {
            'input': {
                'args': ['tester', (1, 'Bill')],
                'kwargs': {
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[100, 1]], [[200, 1]], [[300, 1]]],
                'resp': [1, 'Bill'],
            },
        },
        'update': {
            'input': {
                'args': ['tester', 1],
                'kwargs': {
                    'op_list': [],
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[100, 1]], [[200, 1]], [[300, 1]]],
                'resp': [1, 'Bill'],
            },
        },
        'upsert': {
            'input': {
                'args': ['tester', (1, 'Bill')],
                'kwargs': {
                    'op_list': [],
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[100, 1]], [[200, 1]], [[300, 1]]],
                # resp not used in the test output.
                'resp': None,
            },
        },
        'delete': {
            'input': {
                'args': ['tester', 1],
                'kwargs': {
                    'on_push': push_callback,
                    # on_push_ctx must be set manually when running the test.
                    'on_push_ctx': None,
                }
            },
            'output': {
                'callback_res': [[[100, 1]], [[200, 1]], [[300, 1]]],
                'resp': [1, 'Bill'],
            },
        },
    }

    def test_00_00_push_via_connection(self):
        for case_name in self.push_test_cases.keys():
            with self.subTest(name=case_name):
                callback_res = []
                case = self.push_test_cases[case_name]
                testing_function = getattr(self.conn, case_name)
                case['input']['kwargs']['on_push_ctx'] = callback_res
                resp = testing_function(
                    *case['input']['args'],
                    **case['input']['kwargs']
                )
                self.assertEqual(callback_res, case['output']['callback_res'])
                if case['output']['resp'] is not None:
                    self.assertEqual(resp.data[0], case['output']['resp'])

    def test_00_01_push_via_mesh_connection(self):
        for case_name in self.push_test_cases.keys():
            with self.subTest(name=case_name):
                callback_res = []
                case = self.push_test_cases[case_name]
                testing_function = getattr(self.mesh_conn, case_name)
                case['input']['kwargs']['on_push_ctx'] = callback_res
                resp = testing_function(
                    *case['input']['args'],
                    **case['input']['kwargs']
                )
                self.assertEqual(callback_res, case['output']['callback_res'])
                if case['output']['resp'] is not None:
                    self.assertEqual(resp.data[0], case['output']['resp'])

    def test_00_02_push_via_connection_pool(self):
        for case_name in self.push_test_cases.keys():
            with self.subTest(name=case_name):
                callback_res = []
                case = self.push_test_cases[case_name]
                testing_function = getattr(self.conn_pool, case_name)
                case['input']['kwargs']['on_push_ctx'] = callback_res
                resp = testing_function(
                    *case['input']['args'],
                    **case['input']['kwargs'],
                    mode=tarantool.Mode.RW
                )
                self.assertEqual(callback_res, case['output']['callback_res'])
                if case['output']['resp'] is not None:
                    self.assertEqual(resp.data[0], case['output']['resp'])

    def tearDown(self):
        # Close connection, connection pool and mesh connection to instance.
        self.conn.close()
        self.conn_pool.close()
        self.mesh_conn.close()

    @classmethod
    def tearDownClass(self):
        # Stop instance.
        self.srv.stop()
        self.srv.clean()
