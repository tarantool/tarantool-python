import tarantool.msgpack_ext.decimal as ext_decimal
import tarantool.msgpack_ext.uuid as ext_uuid
import tarantool.msgpack_ext.datetime as ext_datetime
import tarantool.msgpack_ext.interval as ext_interval

decoders = {
    ext_decimal.EXT_ID : ext_decimal.decode ,
    ext_uuid.EXT_ID    : ext_uuid.decode    ,
    ext_datetime.EXT_ID: ext_datetime.decode,
    ext_interval.EXT_ID: ext_interval.decode,
}

def ext_hook(code, data):
    if code in decoders:
        return decoders[code](data)
    raise NotImplementedError("Unknown msgpack type: %d" % (code,))
