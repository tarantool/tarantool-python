#!/usr/bin/env tarantool

local INSTANCE_ID = string.match(arg[0], "%d")
local SOCKET_DIR = require('fio').cwd()

local function instance_uri(instance_id)
    return SOCKET_DIR..'/instance'..instance_id..'.sock';
end

require('console').listen(os.getenv('ADMIN'))

box.cfg({
    --listen              = os.getenv("LISTEN"),
    listen = instance_uri(INSTANCE_ID),
    memtx_memory        = 107374182,
})
