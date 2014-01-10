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
