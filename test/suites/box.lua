#!/usr/bin/env tarantool
local os = require('os')

require('console').listen(os.getenv("ADMIN_PORT"))
box.cfg{
    listen       = os.getenv("PRIMARY_PORT"),
    memtx_memory = 0.1 * 1024^3, -- 0.1 GiB
    pid_file     = "box.pid",
}
