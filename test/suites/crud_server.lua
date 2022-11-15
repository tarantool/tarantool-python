#!/usr/bin/env tarantool

local crud = require('crud')
local vshard = require('vshard')

local admin_listen = os.getenv("ADMIN")
local primary_listen = os.getenv("LISTEN")

require('console').listen(admin_listen)
box.cfg{
    listen       = primary_listen,
    memtx_memory = 0.1 * 1024^3, -- 0.1 GiB
    pid_file     = "box.pid",
}

box.schema.user.grant(
    'guest',
    'read,write,execute',
    'universe'
)
box.schema.create_space(
 'tester', {
  format = {
    {name = 'id', type = 'unsigned'},
    {name = 'bucket_id', type = 'unsigned'},
    {name = 'name', type = 'string'},
  }
})
box.space.tester:create_index('primary_index', {
    parts = {
        {field = 1, type = 'unsigned'},
    },
})
box.space.tester:create_index('bucket_id', {
    parts = {
        {field = 2, type = 'unsigned'},
    },
    unique = false,
})

-- Setup vshard.
_G.vshard = vshard
box.once('guest', function()
    box.schema.user.grant('guest', 'super')
end)
local uri = 'guest@0.0.0.0:' .. primary_listen
local cfg = {
    bucket_count = 300,
    sharding = {
        [box.info().cluster.uuid] = {
            replicas = {
                [box.info().uuid] = {
                    uri = uri,
                    name = 'storage',
                    master = true,
                },
            },
        },
    },
}
vshard.storage.cfg(cfg, box.info().uuid)
vshard.router.cfg(cfg)
vshard.router.bootstrap()

-- Initialize crud.
crud.init_storage()
crud.init_router()
crud.cfg{stats = true}
