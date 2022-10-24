import sys
import unittest
import tarantool
from .lib.tarantool_server import TarantoolServer


# FIXME: I'm quite sure that there is a simpler way to count
# a method calls, but I failed to find any. It seems, I should
# look at unittest.mock more thoroughly.
class MethodCallCounter:
    def __init__(self, obj, method_name):
        self._call_count = 0
        self._bind(obj, method_name)

    def _bind(self, obj, method_name):
        self._obj = obj
        self._method_name = method_name
        self._saved_method = getattr(obj, method_name)
        def wrapper(_, *args, **kwargs):
            self._call_count += 1
            return self._saved_method(*args, **kwargs)
        bound_wrapper = wrapper.__get__(obj.__class__, obj)
        setattr(obj, method_name, bound_wrapper)

    def unbind(self):
        if self._saved_method is not None:
            setattr(self._obj, self._method_name, self._saved_method)

    def call_count(self):
        return self._call_count


class TestSuite_Schema_Abstract(unittest.TestCase):
    # Define 'encoding' field in a concrete class.

    @classmethod
    def setUpClass(self):
        params = 'connection.encoding: {}'.format(repr(self.encoding))
        print(' SCHEMA ({}) '.format(params).center(70, '='), file=sys.stderr)
        print('-' * 70, file=sys.stderr)
        self.srv = TarantoolServer()
        self.srv.script = 'test/suites/box.lua'
        self.srv.start()
        self.con = tarantool.Connection(self.srv.host, self.srv.args['primary'],
                                        encoding=self.encoding)
        self.sch = self.con.schema

        # The relevant test cases mainly target Python 2, where
        # a user may want to pass a string literal as a space or
        # an index name and don't bother whether all symbols in it
        # are ASCII.
        self.unicode_space_name_literal = '∞'
        self.unicode_index_name_literal = '→'

        self.unicode_space_name_u = u'∞'
        self.unicode_index_name_u = u'→'
        self.unicode_space_id, self.unicode_index_id = self.srv.admin("""
            do
                local space = box.schema.create_space('\\xe2\\x88\\x9e')
                local index = space:create_index('\\xe2\\x86\\x92')
                return space.id, index.id
            end
        """)

    def setUp(self):
        # prevent a remote tarantool from clean our session
        if self.srv.is_started():
            self.srv.touch_lock()

        # Count calls of fetch methods. See <fetch_count>.
        self.fetch_space_counter = MethodCallCounter(self.sch, 'fetch_space')
        self.fetch_index_counter = MethodCallCounter(self.sch, 'fetch_index')

    def tearDown(self):
        self.fetch_space_counter.unbind()
        self.fetch_index_counter.unbind()

    @property
    def fetch_count(self):
        """Amount of fetch_{space,index}() calls.

           It is initialized to zero before each test case.
        """
        res = 0
        res += self.fetch_space_counter.call_count()
        res += self.fetch_index_counter.call_count()
        return res

    def verify_unicode_space(self, space):
        self.assertEqual(space.sid, self.unicode_space_id)
        self.assertEqual(space.name, self.unicode_space_name_u)
        self.assertEqual(space.arity, 1)

    def verify_unicode_index(self, index):
        self.assertEqual(index.space.name, self.unicode_space_name_u)
        self.assertEqual(index.iid, self.unicode_index_id)
        self.assertEqual(index.name, self.unicode_index_name_u)
        self.assertEqual(len(index.parts), 1)

    def test_00_authenticate(self):
        self.assertIsNone(self.srv.admin("box.schema.user.create('test', { password = 'test' })"))
        self.assertIsNone(self.srv.admin("box.schema.user.grant('test', 'read,write', 'space', '_space')"))
        self.assertIsNone(self.srv.admin("box.schema.user.grant('test', 'read,write', 'space', '_index')"))
        self.assertEqual(self.con.authenticate('test', 'test')._data, None)

    def test_01_space_bad(self):
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_space(0)
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_space(0)
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_space('bad_name')

    def test_02_index_bad(self):
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_index(0, 'primary')
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no space.*'):
            self.sch.get_index('bad_space', 'primary')
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no index.*'):
            self.sch.get_index(280, 'bad_index')
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no index.*'):
            self.sch.get_index(280, 'bad_index')
        with self.assertRaisesRegex(tarantool.SchemaError,
                'There\'s no index.*'):
            self.sch.get_index(280, 3)

    def test_03_01_space_name__(self):
        self.con.flush_schema()
        space = self.sch.get_space('_schema')
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_space')
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_index')
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

        space = self.sch.get_space(self.unicode_space_name_literal)
        self.verify_unicode_space(space)

    def test_03_02_space_number(self):
        self.con.flush_schema()
        space = self.sch.get_space(272)
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space(280)
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space(288)
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

        space = self.sch.get_space(self.unicode_space_id)
        self.verify_unicode_space(space)

    def test_04_space_cached(self):
        space = self.sch.get_space('_schema')
        self.assertEqual(space.sid, 272)
        self.assertEqual(space.name, '_schema')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_space')
        self.assertEqual(space.sid, 280)
        self.assertEqual(space.name, '_space')
        self.assertEqual(space.arity, 1)
        space = self.sch.get_space('_index')
        self.assertEqual(space.sid, 288)
        self.assertEqual(space.name, '_index')
        self.assertEqual(space.arity, 1)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

        space = self.sch.get_space(self.unicode_space_name_literal)
        self.verify_unicode_space(space)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

    def test_05_01_index_name___name__(self):
        self.con.flush_schema()
        index = self.sch.get_index('_index', 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 'name')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_space', 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index('_space', 'name')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_name_literal,
                                   self.unicode_index_name_literal)
        self.verify_unicode_index(index)

    def test_05_02_index_name___number(self):
        self.con.flush_schema()
        index = self.sch.get_index('_index', 0)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_space', 0)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index('_space', 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_name_literal,
                                   self.unicode_index_id)
        self.verify_unicode_index(index)

    def test_05_03_index_number_name__(self):
        self.con.flush_schema()
        index = self.sch.get_index(288, 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(288, 'name')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 'name')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_id,
                                   self.unicode_index_name_literal)
        self.verify_unicode_index(index)

    def test_05_04_index_number_number(self):
        self.con.flush_schema()
        index = self.sch.get_index(288, 0)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(288, 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 0)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        index = self.sch.get_index(self.unicode_space_id,
                                   self.unicode_index_id)
        self.verify_unicode_index(index)

    def test_06_index_cached(self):
        index = self.sch.get_index('_index', 'primary')
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index('_index', 2)
        self.assertEqual(index.space.name, '_index')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 2)
        index = self.sch.get_index(280, 'primary')
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 0)
        self.assertEqual(index.name, 'primary')
        self.assertEqual(len(index.parts), 1)
        index = self.sch.get_index(280, 2)
        self.assertEqual(index.space.name, '_space')
        self.assertEqual(index.iid, 2)
        self.assertEqual(index.name, 'name')
        self.assertEqual(len(index.parts), 1)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

        cases = (
            (self.unicode_space_name_literal, self.unicode_index_name_literal),
            (self.unicode_space_name_literal, self.unicode_index_id),
            (self.unicode_space_id, self.unicode_index_name_literal),
            (self.unicode_space_id, self.unicode_index_id),
        )
        for s, i in cases:
            index = self.sch.get_index(s, i)
            self.verify_unicode_index(index)

        # Verify that no schema fetches occurs.
        self.assertEqual(self.fetch_count, 0)

    def test_07_schema_version_update(self):
        _space_len = len(self.con.select('_space'))
        self.srv.admin("box.schema.create_space('ttt22')")
        self.assertEqual(len(self.con.select('_space')), _space_len + 1)

    @classmethod
    def tearDownClass(self):
        self.con.close()
        self.srv.stop()
        self.srv.clean()


class TestSuite_Schema_UnicodeConnection(TestSuite_Schema_Abstract):
    encoding = 'utf-8'


class TestSuite_Schema_BinaryConnection(TestSuite_Schema_Abstract):
    encoding = None
