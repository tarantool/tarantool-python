def check_key(*args, first=True):
    if len(args) == 1 and isinstance(args[0], (list, tuple)) and first:
        check_key(*args[0], first=False)
    for key in args:
        assert isinstance(key, (int, long, basestring))
