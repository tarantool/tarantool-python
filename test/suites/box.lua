#!/usr/bin/env tarantool

local os = require('os')

local admin_listen = os.getenv("ADMIN")
local primary_listen = os.getenv("LISTEN")

require('console').listen(admin_listen)
box.cfg{
    listen       = primary_listen,
    memtx_memory = 0.1 * 1024^3, -- 0.1 GiB
    pid_file     = "box.pid",
}
