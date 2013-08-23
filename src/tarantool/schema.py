# -*- coding: utf-8 -*-
# pylint: disable=C0301,W0105,W0401,W0614
'''
This module provides :class:`~tarantool.schema.Schema` class.
It is a Tarantool schema description.
'''

# Tarantool data types
RAW = 0
NUM = 1
NUM64 = 2
STR = 3

from tarantool.const import (
    struct_L,
    struct_Q
)


class Schema(object):

    '''\
    A class used to describe a data schema.
    Encapsulates the names and types and provides more convenient syntax
    for database operations.
    '''

    def __init__(self, schema):
        '''\
        Create Schema instance.

        :param connection: Object representing connection to the server
        :type connection: :class:`~tarantool.connection.Connection` instance
        :param schema: schema description
        :type schema: dict

        Example schema description:

        >>> schema = {
            0: { # Space description
                'name': 'users', # Space name
                # Type that used to decode fields that are not listed below
                'default_type': tarantool.STR,
                'fields': {
                    0: ('user_id', tarantool.NUM), # (field name, field type)
                    1: ('num64field', tarantool.NUM64),
                    2: ('strfield', tarantool.STR),
                    # Alternative syntax
                    #2: { 'name': 'strfield', 'type': tarantool.STR },
                    #2: tarantool.STR # Alternative syntax
                },
                'indexes': {
                    0: ('pk', [0]), # (name, [field_no])
                    #0: { 'name': 'pk', 'fields': [0]}, # Alternative syntax
                    #0: [0], # Alternative syntax
                }
            }
        }
        '''

        self._space_names = {}
        self._spaces = {}
        if schema is None:
            return

        for (space_no, space_descr) in schema.iteritems():
            if not isinstance(space_no, (int, long)):
                raise ValueError('Invalid space_no: %s' % space_no)

            # Space name
            space_name = space_descr.get('name', None)

            # default_type
            default_type = space_descr.get('default_type', None)
            default_type = self._check_datatype(default_type)

            # fields
            field_descrs = space_descr.get('fields', {})
            max_fieldno = 0
            for field_no in field_descrs.iterkeys():
                if not isinstance(field_no, (int, long)):
                    raise ValueError('Invalid field_no: %s' % field_no)
                max_fieldno = max(max_fieldno, field_no)

            field_defs = [None] * (max_fieldno + 1)
            for (field_no, field_descr) in field_descrs.iteritems():
                assert field_no < len(field_defs)
                if isinstance(field_descr, tuple):
                    (field_name, field_type) = field_descr
                elif isinstance(field_descr, dict):
                    field_name = field_descr.get('name', None)
                    field_type = field_descr.get('type', RAW)
                else:
                    field_name = None
                    field_type = field_descr

                field_type = self._check_datatype(field_type)
                field_defs[field_no] = (field_name, field_type)

            # indexes
            index_descrs = space_descr.get('indexes', {})
            max_indexno = 0
            for index_no in index_descrs.iterkeys():
                if not isinstance(index_no, (int, long)):
                    raise ValueError('Invalid index_no: %s' % index_no)
                max_indexno = max(max_indexno, index_no)

            index_defs = [None] * (max_indexno + 1)
            for (index_no, index_descr) in index_descrs.iteritems():
                if isinstance(index_descr, tuple):
                    (index_name, indexed_fields) = index_descr
                elif isinstance(index_descr, dict):
                    index_name = index_descr.get('name', None)
                    indexed_fields = index_descr.get('fields', [])
                else:
                    index_name = None
                    indexed_fields = index_descr

                if (
                    not isinstance(indexed_fields, list)
                    or (len(indexed_fields) == 0)
                ):
                    raise ValueError(
                        'Invalid index description: %s' % repr(index_descr))

                for field_no in indexed_fields:
                    if not isinstance(field_no, int):
                        raise ValueError(
                            'Invalid index description: %s' % repr(
                                index_descr))
                    if (
                        (default_type is None) and
                        (
                            (field_no < len(field_defs)) or
                            (field_defs[field_no] is None)
                        )
                    ):
                        raise ValueError(
                            'Field is not defined: %s' % repr(index_descr))

                index_def = (index_name, indexed_fields)
                index_defs[index_no] = (index_def)

            space_def = (space_name, field_defs, default_type, index_defs)
            self._spaces[space_no] = space_def
            if space_name is not None:
                self._space_names[space_name] = space_no

    def _check_datatype(self, dtype):
        if dtype in (NUM, NUM64, RAW, STR):
            return dtype
        elif dtype is None:
            return RAW
        elif dtype == int:
            return NUM
        elif dtype == long:
            return NUM64
        elif isinstance(dtype, basestring):
            return STR
        else:
            raise ValueError("Invalid data type: %s" % dtype)

    def space_no(self, space_name):
        '''\
        Return space number by space name

        :param space_name: Space name
        :type space_name: str
        '''

        if isinstance(space_name, int):
            return space_name

        # Space schema must be defined if use want to use space by name
        return self._space_names[space_name]

    def index_no(self, space_name, index_name):
        '''\
        Return index number by space name and index name

        :param space_name: Space name
        :type space_name: str
        :param index_name: Index name
        :type index_name: str
        '''

        if isinstance(index_name, int):
            return index_name

        space_no = self.space_no(space_name)
        # Space must be defined
        space_def = self._spaces[space_no]
        (_name, _field_defs, _default_type, index_defs) = space_def
        # TODO: this loop should be optimized
        index_defs_enum = enumerate(index_defs)
        for (index_no, (index_name2, _indexed_fields)) in index_defs_enum:
            if index_name2 == index_name:
                return index_no
        raise KeyError(index_name)

    def _pack_value_int(self, value):
        if __debug__:
            if not isinstance(value, int):
                raise TypeError(
                    "Invalid argument type '%s'. Only 'int' "
                    "expected" % type(value).__name__)
            if value < 0:
                raise TypeError(
                    "Number %d does not fit into NUM32 type" % value)
        return struct_L.pack(value)

    def _unpack_value_int(self, packed_value):
        if __debug__:
            if len(packed_value) != 4:
                raise TypeError(
                    "Invalid argument length: got %d, "
                    "expected 4" % len(packed_value))
        return struct_L.unpack(packed_value)[0]

    def _pack_value_int64(self, value):
        '''\
        Pack integer64 field
        <field> ::= <int32_varint><data>

        :param value: integer value to be packed
        :type value: int

        :return: packed value
        :rtype: bytes
        '''
        if __debug__:
            if not isinstance(value, (int, long)):
                raise TypeError(
                    "Invalid argument type '%s'. Only 'int' or 'long' "
                    "expected" % type(value).__name__)
            if (value < 0) or (value > 18446744073709551615):
                raise TypeError(
                    "Number %d does not fit into NUM64 type" % value)
        return struct_Q.pack(value)

    def _unpack_value_int64(self, packed_value):
        if __debug__:
            if len(packed_value) != 8:
                raise TypeError(
                    "Invalid argument length: got %d, "
                    "expected 8" % len(packed_value))
        return struct_Q.unpack(packed_value)[0]

    def pack_value(self, value, cast_to=None):
        '''\
        Convert single field from Python type to Tarantol type

        :param value: value to be packed
        :type value: bytes, int, unicode (str for py3k)
        :param cast_to: data type
        :type cast_to: int or a type object (one of bytes, int,
            unicode (str for py3k))

        :return: packed value
        :rtype: bytes
        '''
        if cast_to:
            if cast_to in (NUM, int):
                return self._pack_value_int(value)
            elif cast_to in (STR, RAW, basestring, bytes, None):
                return str(value)
            elif cast_to in (NUM64, long):
                return self._pack_value_int64(value)
            else:
                raise TypeError("Invalid field type %d." % cast_to)
        else:
            # try to autodetect tarantool types based on python types
            if isinstance(value, basestring):
                return value
            elif isinstance(value, int):
                if value > 4294967295:
                    return self._pack_value_int64(value)
                else:
                    return self._pack_value_int(value)
            elif isinstance(value, long):
                return self._pack_value_int64(value)
            else:
                raise TypeError(
                    "Invalid argument type '%s'. Only 'str', 'int' or "
                    "'long' expected" % type(value).__name__)

    def unpack_value(self, packed_value, cast_to):
        '''\
        Convert field type from Tarantool type to Python type

        :param value: raw value from the database
        :type value: bytes
        :param cast_to: data type to cast to
        :type cast_to: int or a type object (one of bytes, int,
            unicode (str for py3k))

        :return: converted value
        :rtype: value of native python type (one of bytes, int,
            unicode (str for py3k))
        '''

        if cast_to in (NUM, int):
            return self._unpack_value_int(packed_value)
        elif cast_to in (RAW, bytes, None):
            return packed_value
        elif cast_to in (STR, basestring):
            return unicode(packed_value)
        elif cast_to in (NUM64, long):
            return self._unpack_value_int64(packed_value)
        else:
            raise TypeError("Invalid field type %s" % (cast_to))

    def pack_values(
        self, values, space_no=None, field_defs=None, default_type=None
    ):
        '''\
        Convert a list of fields from Python to Tarantool types using schema

        :param value: tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)
        :param space_no: space number
        :type space_no: None or int
        :param field_defs: field definitions used for types conversion,
               e.g. [('field0', tarantool.NUM), ('field1', tarantool.STR)]
        :type field_defs: None or  [(name, type) or None]
        :param default_type: None a default type used for result conversion,
            as defined in ``schema[space_no]['default_type']``
        :type default_type: None or int

        :return: packed tuple
        :return: packed values
        '''

        if field_defs is None and space_no is not None:
            assert isinstance(space_no, (int, long))
            # Space schema must be defined if use want to use space by name
            space_def = self._spaces.get(space_no, None)
            if space_def is not None:
                (_name, field_defs, default_type, _index_defs) = space_def

        assert isinstance(field_defs, list) or (field_defs is None)
        assert isinstance(default_type, int) or (default_type is None)

        if field_defs:
            packed_values = []
            for field_no, value in enumerate(values):
                dtype = None
                if (
                    (field_no < len(field_defs)) and
                    (field_defs[field_no] is not None)
                ):
                    (_name, dtype) = field_defs[field_no]
                if dtype is None:
                    dtype = default_type
                packed_values.append(self.pack_value(value, dtype))
        else:
            packed_values = [self.pack_value(value, default_type)
                             for value in values]

        return packed_values

    def unpack_values(
        self, packed_values, space_no=None, field_defs=None, default_type=None
    ):
        '''\
        Convert a list of fields from Tarantool to Python types using schema

        :param packed_values: tuple of the raw database values
        :type packed_values: tuple of bytes
        :param space_no: space number
        :type space_no: None or int
        :param field_defs: field definitions used for types conversion,
               e.g. [('field0', tarantool.NUM), ('field1', tarantool.STR)]
        :type field_defs: None or  [(name, type) or None]
        :param default_type: None a default type used for result conversion,
            as defined in ``schema[space_no]['default_type']``
        :type default_type: None or int

        :return: converted tuple value
        :rtype: unpacked values of native python types (bytes, int,
            unicode (or str for py3k))
        '''

        if field_defs is None and space_no is not None:
            assert isinstance(space_no, (int, long))
            # Space schema must be defined if use want to use space by name
            space_def = self._spaces.get(space_no, None)
            if space_def is not None:
                (_name, field_defs, default_type, _index_defs) = space_def

        assert isinstance(field_defs, list) or (field_defs is None)
        assert isinstance(default_type, int) or (default_type is None)

        if field_defs:
            values = []
            for field_no, value in enumerate(packed_values):
                dtype = None
                if (
                    (field_no < len(field_defs)) and
                    (field_defs[field_no] is not None)
                ):
                    (_name, dtype) = field_defs[field_no]
                if dtype is None:
                    dtype = default_type
                values.append(self.unpack_value(value, dtype))
        else:
            values = [self.unpack_value(value, default_type, )
                      for value in packed_values]

        return tuple(values)

    def pack_key(self, values, space_no, index_no):
        '''\
        Convert a list of keys from Python to Tarantool types using schema

        :param value: key tuple to be packed
        :type value: tuple of scalar values (bytes, str or int)
        :param space_no: space number
        :type space_no: int
        :param index_no: index number
        :type index_no: int

        :return: packed values
        :rtype: bytes
        '''

        space_def = self._spaces.get(space_no, None)
        if space_def:
            (_name, field_defs, default_type, index_defs) = space_def
            packed_items = []
            assert index_no is not None
            # Index must be defined
            (_index_name, indexed_fields) = index_defs[index_no]
            assert isinstance(indexed_fields, list)
            for part, value in enumerate(values):
                field_no = indexed_fields[part]
                # field types must be defined for indexed fields
                (_name, dtype) = field_defs[field_no]
                packed_items.append(self.pack_value(value, dtype))
        else:
            packed_items = [self.pack_value(value) for value in values]

        return packed_items
