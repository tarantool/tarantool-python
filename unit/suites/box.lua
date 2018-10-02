#!/usr/bin/env tarantool
os = require('os')

require('console').listen(os.getenv("ADMIN_PORT"))
box.cfg{
    listen           = os.getenv("PRIMARY_PORT"),
    slab_alloc_arena = 0.1,
    pid_file         = "box.pid",
    rows_per_wal     = 50
}
