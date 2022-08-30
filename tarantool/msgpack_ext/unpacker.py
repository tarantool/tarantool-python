import tarantool.msgpack_ext.decimal as ext_decimal

def ext_hook(code, data):
    if code == ext_decimal.EXT_ID:
        return ext_decimal.decode(data)
    raise NotImplementedError("Unknown msgpack type: %d" % (code,))
