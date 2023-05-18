#!/usr/bin/env tarantool

local function replicaset_uuid()
    if box.info().replicaset ~= nil and box.info().replicaset.uuid ~= nil then
        return box.info().replicaset.uuid
    end

    return box.info().cluster.uuid
end

local function configure_crud_instance(primary_listen, crud, vshard)
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
            [replicaset_uuid()] = {
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
end

local crud_imported, crud = pcall(require, 'crud')
local vshard_imported, vshard = pcall(require, 'vshard')

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

if crud_imported == false or vshard_imported == false then
    -- Set flag for unittest.
    _G['ROCKS_IMPORT_FAIL'] = true
    local fail_msg = 'The crud/vshard modules are not detected, ' ..
                     'installation via rocks install is required ' ..
                     'for CRUD testing purposes. You can use ' ..
                     '<tarantoolctl rocks install crud> or ' ..
                     '<tt rocks install crud> to install modules'
    -- The print output will be captured in the logs.
    print(fail_msg)
else
    configure_crud_instance(primary_listen, crud, vshard)
end
