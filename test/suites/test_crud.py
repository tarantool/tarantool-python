import re
import sys
import time
import unittest
import tarantool
from .lib.tarantool_server import TarantoolServer
from tarantool.error import DatabaseError


def create_server():
    srv = TarantoolServer()
    srv.script = 'test/suites/crud_server.lua'
    srv.start()

    return srv


@unittest.skipIf(sys.platform.startswith("win"),
                 "Crud tests on windows platform are not supported: " +
                 "complexity of the vshard replicaset configuration")
class TestSuite_Crud(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print(' CRUD '.center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        # Create server and extract helpful fields for tests.
        self.srv = create_server()
        self.host = self.srv.host
        self.port = self.srv.args['primary']

    def setUp(self):
        time.sleep(1)
        # Open connections to instance.
        self.conn = tarantool.Connection(host=self.host, port=self.port, 
                                         user='guest', password='')
        self.conn_mesh = tarantool.MeshConnection(host=self.host, port=self.port, 
                                         user='guest', password='')
        self.conn_pool = tarantool.ConnectionPool([{'host':self.host, 'port':self.port}], 
                                                     user='guest', password='')
        # Time for vshard group configuration.
        time.sleep(1)

    crud_test_cases = {
        'crud_insert': {
            'success': {
                'input': {
                    'args': ['tester', [1, 100, 'Mike'], {'timeout': 10}],
                },
                'output': {
                    'rows': [[1, 100, 'Mike']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', [1, 100, 'Bob'], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'Duplicate key exists',
                        ],
                },
            },
        },
        'crud_insert_object': {
            'success': {
                'input': {
                    'args': ['tester', {'id': 2, 'bucket_id': 100, 'name': 'Ann'}, {'timeout': 10}],
                },
                'output': {
                    'rows': [[2, 100, 'Ann']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', {'id': 2, 'bucket_id': 100, 'name': 'Logan'}, {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'Duplicate key exists',
                        ],
                },
            },
        },
        'crud_insert_many': {
            'success': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            [3, 100, 'Jacob'],
                            [4, 100, 'Wyatt'],
                            [5, 100, 'David'],
                            [6, 100, 'Leo'],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'rows': [
                                [3, 100, 'Jacob'],
                                [4, 100, 'Wyatt'],
                                [5, 100, 'David'],
                                [6, 100, 'Leo'],
                            ],
                },
            },
            'error': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            [3, 100, 'Julian'],
                            [4, 100, 'Hudson'],
                            [7, 100, 'Grayson'],
                            [8, 100, 'Ezra'],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'str': [
                            r'Duplicate key exists',
                        ],
                    'res_rows': [[7, 100, 'Grayson'], [8, 100, 'Ezra']]
                },
            },
        },
        'crud_insert_object_many': {
            'success': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            {'id': 9, 'bucket_id': 100, 'name': 'Sharar'},
                            {'id': 10, 'bucket_id': 100, 'name': 'Thaddeus'},
                            {'id': 11, 'bucket_id': 100, 'name': 'Tobit'},
                            {'id': 12, 'bucket_id': 100, 'name': 'Zeb'},
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'rows': [
                                [9, 100, 'Sharar'],
                                [10, 100, 'Thaddeus'],
                                [11, 100, 'Tobit'],
                                [12, 100, 'Zeb'],
                            ],
                },
            },
            'error': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            {'id': 9, 'bucket_id': 100, 'name': 'Silvanus'},
                            {'id': 10, 'bucket_id': 100, 'name': 'Timeus'},
                            {'id': 13, 'bucket_id': 100, 'name': 'Uzzi'},
                            {'id': 14, 'bucket_id': 100, 'name': 'Zimiri'},
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'str': [
                            r'Duplicate key exists',
                        ],
                    'res_rows': [[13, 100, 'Uzzi'], [14, 100, 'Zimiri']]
                },
            },
        },
        'crud_get': {
            'success': {
                'input': {
                    'args': ['tester', 1, {'timeout': 10}],
                },
                'output': {
                    'rows': [[1, 100, 'Mike']],
                },
            },
            'error': {
                'input': {
                    'args': ['no-such-space-name', [1, 100, 'Bob'], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'GetError: Space "no-such-space-name" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_update': {
            'success': {
                'input': {
                    'args': ['tester', 1, [['+', 'bucket_id', 1]], {'timeout': 10}],
                },
                'output': {
                    'rows': [[1, 101, 'Mike']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', 1, [['+', 'age', 1]], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r"UpdateError", 
                        ],
                },
            },
        },
        'crud_delete': {
            'success': {
                'input': {
                    'args': ['tester', 1, {'timeout': 10}],
                },
                'output': {
                    'rows': [[1, 101, 'Mike']],
                },
            },
            'error': {
                'input': {
                    'args': ['no-such-space-name', 1, {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'DeleteError: Space "no-such-space-name" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_replace': {
            'success': {
                'input': {
                    'args': ['tester', [2, 100, 'Alice'], {'timeout': 10}],
                },
                'output': {
                    'rows': [[2, 100, 'Alice']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', [1, 100, 0], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'expected string',
                        ],
                },
            },
        },
        'crud_replace_object': {
            'success': {
                'input': {
                    'args': ['tester', {'id': 2, 'bucket_id': 100, 'name': 'Eliza'}, {'timeout': 10}],
                },
                'output': {
                    'rows': [[2, 100, 'Eliza']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', {'id': 2, 'bucket_id': 100, 'name': 0}, {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'expected string',
                        ],
                },
            },
        },
        'crud_replace_many': {
            'success': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            [2, 100, 'Cephus'],
                            [3, 100, 'Esau'],
                            [4, 100, 'Haman'],
                            [5, 100, 'Gershon'],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'rows': [
                                [2, 100, 'Cephus'],
                                [3, 100, 'Esau'],
                                [4, 100, 'Haman'],
                                [5, 100, 'Gershon'],
                            ],
                },
            },
            'error': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            [3, 100, 'Ephron'],
                            [4, 100, 'Ethan'],
                            [7, 100, 0],
                            [8, 100, 0],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'str': [
                            r'expected string',
                        ],
                    'res_rows': [[3, 100, 'Ephron'], [4, 100, 'Ethan']]
                },
            },
        },
        'crud_replace_object_many': {
            'success': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            {'id': 2, 'bucket_id': 100, 'name': 'Cephus'},
                            {'id': 3, 'bucket_id': 100, 'name': 'Esau'},
                            {'id': 4, 'bucket_id': 100, 'name': 'Haman'},
                            {'id': 5, 'bucket_id': 100, 'name': 'Gershon'},
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'rows': [
                                [2, 100, 'Cephus'],
                                [3, 100, 'Esau'],
                                [4, 100, 'Haman'],
                                [5, 100, 'Gershon'],
                            ],
                },
            },
            'error': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            {'id': 3, 'bucket_id': 100, 'name': 'Ephron'},
                            {'id': 4, 'bucket_id': 100, 'name': 'Ethan'},
                            {'id': 7, 'bucket_id': 100, 'name': 0},
                            {'id': 8, 'bucket_id': 100, 'name': 0},
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'str': [
                            r'expected string',
                        ],
                    'res_rows': [[3, 100, 'Ephron'], [4, 100, 'Ethan']]
                },
            },
        },
        'crud_upsert': {
            'success': {
                'input': {
                    'args': ['tester', [2, 100, 'Cephus'], [['+', 'bucket_id', 1]], {'timeout': 10}],
                },
                'output': {
                    'rows': [],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', [2, 100, 'Cephus'], [['+', 'age', 1]], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r"UpsertError", 
                        ],
                },
            },
        },
        'crud_upsert_object': {
            'success': {
                'input': {
                    'args': ['tester', {'id': 2, 'bucket_id': 100, 'name': 'Cephus'}, 
                        [['+', 'bucket_id', 1]], {'timeout': 10}],
                },
                'output': {
                    'rows': [],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', {'id': 2, 'bucket_id': 100, 'name': 'Cephus'}, 
                        [['+', 'age', 1]], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r"UpsertError",
                        ],
                },
            },
        },
        'crud_upsert_many': {
            'success': {
                'input': {
                    'args': [
                        'tester',
                        [
                            [[2, 100, 'Cephus'], [['+', 'bucket_id', 1]]],
                            [[3, 100, 'Esau'], [['+', 'bucket_id', 1]]],
                            [[4, 100, 'Haman'], [['+', 'bucket_id', 1]]],
                            [[5, 100, 'Gershon'], [['+', 'bucket_id', 1]]],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {},
            },
            'error': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            [[3, 100, 'Ephron'], [['+', 'bucket_id', 1]]],
                            [[4, 100, 'Ethan'], [['+', 'bucket_id', 1]]],
                            [[7, 100, 0], [['+', 'bucket_id', 1]]],
                            [[8, 100, 0], [['+', 'bucket_id', 1]]],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'str': [
                            r'expected string',
                        ],
                },
            },
        },
        'crud_upsert_object_many': {
            'success': {
                'input': {
                    'args': [
                        'tester',
                        [
                            [{'id': 2, 'bucket_id': 100, 'name': 'Cephus'}, [['+', 'bucket_id', 1]]],
                            [{'id': 3, 'bucket_id': 100, 'name': 'Esau'}, [['+', 'bucket_id', 1]]],
                            [{'id': 4, 'bucket_id': 100, 'name': 'Haman'}, [['+', 'bucket_id', 1]]],
                            [{'id': 5, 'bucket_id': 100, 'name': 'Gershon'}, [['+', 'bucket_id', 1]]],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {},
            },
            'error': {
                'input': {
                    'args': [
                        'tester', 
                        [
                            [{'id': 3, 'bucket_id': 100, 'name': 'Ephron'}, [['+', 'bucket_id', 1]]],
                            [{'id': 4, 'bucket_id': 100, 'name': 'Ethan'}, [['+', 'bucket_id', 1]]],
                            [{'id': 7, 'bucket_id': 100, 'name': 0}, [['+', 'bucket_id', 1]]],
                            [{'id': 8, 'bucket_id': 100, 'name': 0}, [['+', 'bucket_id', 1]]],
                        ],
                        {'timeout': 10},
                    ],
                },
                'output': {
                    'str': [
                            r'expected string',
                        ],
                },
            },
        },
        'crud_select': {
            'success': {
                'input': {
                    'args': ['tester', [], {'first': 3}],
                },
                'output': {
                    'rows': [[2, 104, 'Cephus'], [3, 104, 'Ephron'], [4, 104, 'Ethan']],
                },
            },
            'error': {
                'input': {
                    'args': ['no-such-space-name'],
                },
                'output': {
                    'str': [
                            r'SelectError: Space "no-such-space-name" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_min': {
            'success': {
                'input': {
                    'args': ['tester', 'bucket_id', {'timeout': 10}],
                },
                'output': {
                    'rows': [[6, 100, 'Leo']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', 'no-idx'],
                },
                'output': {
                    'str': [
                            r'BorderError: Index "no-idx" of space "tester" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_max': {
            'success': {
                'input': {
                    'args': ['tester', 'bucket_id', {'timeout': 10}],
                },
                'output': {
                    'rows': [[4, 104, 'Ethan']],
                },
            },
            'error': {
                'input': {
                    'args': ['tester', 'no-idx', {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'BorderError: Index "no-idx" of space "tester" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_len': {
            'success': {
                'input': {
                    'args': ['tester', {'timeout': 10}],
                },
                'output': {
                    'scalar': 13,
                },
            },
            'error': {
                'input': {
                    'args': ['no-such-space-name', {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'LenError: Space "no-such-space-name" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_count': {
            'success': {
                'input': {
                    'args': ['tester', [['==', 'bucket_id', 100]], {'timeout': 10}],
                },
                'output': {
                    'scalar': 9,
                },
            },
            'error': {
                'input': {
                    'args': ['no-such-space-name', [['==', 'bucket_id', 100]], {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'CountError: Space "no-such-space-name" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_unflatten_rows': {
            'success': {
                'input': {
                    'args': [
                        [
                            [1, 100, 'Mike'], 
                            [2, 100, 'Mike'], 
                            [3, 100, 'Mike'], 
                            [4, 100, 'Mike'], 
                            [5, 200, 'Bill'], 
                            [6, 300, 'Rob'],
                        ],
                        [
                            {'name': 'id', 'type': 'unsigned'}, 
                            {'name': 'bucket_id', 'type': 'unsigned'}, 
                            {'name': 'name', 'type': 'string'}
                        ],
                    ],
                },
                'output': {
                    'scalar': [
                        {'bucket_id': 100, 'name': 'Mike', 'id': 1}, 
                        {'bucket_id': 100, 'name': 'Mike', 'id': 2}, 
                        {'bucket_id': 100, 'name': 'Mike', 'id': 3}, 
                        {'bucket_id': 100, 'name': 'Mike', 'id': 4}, 
                        {'bucket_id': 200, 'name': 'Bill', 'id': 5}, 
                        {'bucket_id': 300, 'name': 'Rob', 'id': 6},
                    ],
                },
            },
            'error': {
                'input': {
                    'args': [[],[]],
                },
                'output': {
                    'str': [],
                },
            },
        },
        'crud_truncate': {
            'success': {
                'input': {
                    'args': ['tester'],
                },
                'output': {
                    'scalar': True,
                },
            },
            'error': {
                'input': {
                    'args': ['no-such-space-name', {'timeout': 10}],
                },
                'output': {
                    'str': [
                            r'"no-such-space-name" doesn\'t exist', 
                        ],
                },
            },
        },
        'crud_stats': {
            'success': {
                'input': {
                    'args': ['tester'],
                },
                'output': {
                    'operations': [
                            'insert', 'replace',
                            'upsert', 'len',
                            'delete', 'get',
                            'select', 'borders',
                            'update', 'count',
                            'truncate',
                        ],
                },
            },
            'error': {
                'input': {
                    'args': [],
                },
                'output': {
                    'str': [],
                },
            },
        },
    }

    def _correct_operation_with_crud(self, testing_function, case, mode=None):
        if mode is not None:
            resp = testing_function(
                *case['success']['input']['args'],
                mode=mode
            )
        else:
            resp = testing_function(
                *case['success']['input']['args'],
            )
        if 'rows' in case['success']['output']:
            # Case for crud responce as tarantool.crud.CrudResult obj.
            self.assertEqual(resp.rows, case['success']['output']['rows'])
        if 'scalar' in case['success']['output']:
            # Case for scalar value as crud responce, not tarantool.crud.CrudResult obj.
            self.assertEqual(resp, case['success']['output']['scalar'])
        if 'operations' in case['success']['output']:
            # Case for statistics testing.
            for operation in case['success']['output']['operations']:
                self.assertEqual(operation in resp.__dict__, True, 
                    'Problem with finding a field with a statistic about operation ' 
                        + operation)

    def _exception_operation_with_crud(self, testing_function, case, mode=None):
        try:
            if mode is not None:
                _ = testing_function(
                    *case['error']['input']['args'],
                    mode=mode
                )
            else:
                _ = testing_function(
                    *case['error']['input']['args'],
                )
        except DatabaseError as e:
            for regexp_case in case['error']['output']['str']:
                if hasattr(e, 'extra_info_error'):
                    # Case for non-batch operations.
                    self.assertNotEqual(re.search(regexp_case, e.extra_info_error.str), None)
                if hasattr(e, 'errors_list'):
                    # Case for *_many() operations.
                    err_sum = str()
                    for err in e.errors_list:
                        err_sum = err_sum + err.str
                    self.assertNotEqual(re.search(regexp_case, err_sum), None)
            if hasattr(e, 'success_list'):
                # Case for *_many() operations.
                if 'res_rows' in case['error']['output']:
                    self.assertEqual(e.success_list.rows, case['error']['output']['res_rows'])

    def test_crud_module_via_connection(self):
        for case_name in self.crud_test_cases.keys():
            with self.subTest(name=case_name):
                case = self.crud_test_cases[case_name]
                testing_function = getattr(self.conn, case_name)
                # Correct try testing.
                self._correct_operation_with_crud(testing_function, case)
                # Exception try testing.
                self._exception_operation_with_crud(testing_function, case)

    def test_crud_module_via_mesh_connection(self):
        for case_name in self.crud_test_cases.keys():
            with self.subTest(name=case_name):
                case = self.crud_test_cases[case_name]
                testing_function = getattr(self.conn_mesh, case_name)
                # Correct try testing.
                self._correct_operation_with_crud(testing_function, case)
                # Exception try testing.
                self._exception_operation_with_crud(testing_function, case)

    def test_crud_module_via_pool_connection(self):
        for case_name in self.crud_test_cases.keys():
            with self.subTest(name=case_name):
                case = self.crud_test_cases[case_name]
                testing_function = getattr(self.conn_pool, case_name)
                # Correct try testing.
                self._correct_operation_with_crud(testing_function, case, mode=tarantool.Mode.RW)
                # Exception try testing.
                self._exception_operation_with_crud(testing_function, case, mode=tarantool.Mode.RW)

    def tearDown(self):
        # Close connections to instance.
        self.conn.close()
        self.conn_mesh.close()
        self.conn_pool.close()

    @classmethod
    def tearDownClass(self):
        # Stop instance.
        self.srv.stop()
        self.srv.clean()
