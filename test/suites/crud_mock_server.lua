#!/usr/bin/env tarantool

local admin_listen = os.getenv("ADMIN")
local primary_listen = os.getenv("LISTEN")

require('console').listen(admin_listen)
box.cfg{
    listen       = primary_listen,
    memtx_memory = 0.1 * 1024^3, -- 0.1 GiB
    pid_file     = "box.pid",
}

box.schema.user.grant('guest', 'execute', 'universe', nil, {if_not_exists = true})

local function mock_replace()
    error('Unexpected connection error')
end

rawset(_G, 'crud', {replace = mock_replace})

rawset(_G, 'ready', true)
