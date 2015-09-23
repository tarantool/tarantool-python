# -*- coding: utf-8 -*-

import six

def check_key(*args, **kwargs):
    if 'first' not in kwargs:
        kwargs['first'] = True
    if 'select' not in kwargs:
        kwargs['select'] = False
    if len(args) == 0 and kwargs['select']:
        return []
    if len(args) == 1:
        if isinstance(args[0], (list, tuple)) and kwargs['first']:
            kwargs['first'] = False
            return check_key(*args[0], **kwargs)
        elif args[0] is None and kwargs['select']:
            return []
    for key in args:
        assert isinstance(key, six.integer_types + six.string_types)
    return list(args)
