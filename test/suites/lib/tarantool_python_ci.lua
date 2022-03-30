#!/usr/bin/env tarantool

local console = require('console')
local clock = require('clock')
local log = require('log')

local CONSOLE_PORT = 3302

box.cfg({})
console.listen(CONSOLE_PORT)

-- forward declarations
local clean
local init

-- {{{ locking

local LOCK_LIFETIME = 30 -- seconds

local locked_by = nil
local locked_at = 0 -- unix time, seconds

local function clean_lock()
    locked_by = nil
    locked_at = 0
    clean()
    init()
end

local function set_lock(who)
    locked_by = who
    locked_at = clock.monotonic()
end

local function clean_dead_lock()
    if locked_by ~= nil and clock.monotonic() - locked_at > LOCK_LIFETIME then
        log.info(('removed dead "%s" lock'):format(tostring(locked_by)))
        clean_lock()
    end
end

local function is_locked_by(who)
    return locked_by == who
end

local function is_locked()
    return locked_by ~= nil
end

local function acquire_lock(who)
    assert(type(who) == 'string')
    clean_dead_lock()
    if is_locked_by(who) then
        -- update lock time
        set_lock(who)
        log.info(('updated "%s" lock'):format(who))
        return true
    end
    if is_locked() then
        local err = 'locked by ' .. tostring(locked_by)
        log.info(('can not update "%s" lock: %s'):format(who, err))
        return false, err
    end
    set_lock(who)
    log.info(('set "%s" lock'):format(who))
    return true
end

local function touch_lock(who)
    assert(type(who) == 'string')
    clean_dead_lock()
    if is_locked_by(who) then
        -- update lock time
        set_lock(who)
        log.info(('updated "%s" lock'):format(who))
        return true
    end
    if is_locked() then
        local err = 'locked by ' .. tostring(locked_by)
        log.info(('can not update "%s" lock: %s'):format(who, err))
        return false, err
    end
    local err = 'is not locked'
    log.info(('can not update "%s" lock: %s'):format(who, err))
    return false, err
end

local function release_lock(who)
    assert(type(who) == 'string')
    if is_locked_by(who) then
        clean_lock()
        log.info(('released "%s" lock'):format(who))
        return true
    end
    clean_dead_lock()
    if is_locked() then
        local err = 'locked by ' .. tostring(locked_by)
        log.info(('can not release "%s" lock: %s'):format(who, err))
        return false, err
    end
    local err = 'is not locked'
    log.info(('can not release "%s" lock: %s'):format(who, err))
    return false, err
end

-- }}}

-- {{{ init

init = function()
    _G.acquire_lock = acquire_lock
    _G.touch_lock = touch_lock
    _G.release_lock = release_lock
end

-- }}}

-- {{{ clean

-- Copy of cleanup_cluster() from test_run.lua.
local function cleanup_cluster()
    local cluster = box.space._cluster:select()
    for _, tuple in pairs(cluster) do
        if tuple[1] ~= box.info.id then
            box.space._cluster:delete(tuple[1])
        end
    end
end

-- Copy of clean() from pretest_clean.lua from test-run.
clean = function()
    local _SPACE_NAME = 3

    box.space._space:pairs():map(function(tuple)
        local name = tuple[_SPACE_NAME]
        return name
    end):filter(function(name)
        -- skip internal spaces
        local first_char = string.sub(name, 1, 1)
        return first_char ~= '_'
    end):each(function(name)
        box.space[name]:drop()
    end)

    local _USER_TYPE = 4
    local _USER_NAME = 3

    local allowed_users = {
        guest = true,
        admin = true,
    }
    box.space._user:pairs():filter(function(tuple)
        local tuple_type = tuple[_USER_TYPE]
        return tuple_type == 'user'
    end):map(function(tuple)
        local name = tuple[_USER_NAME]
        return name
    end):filter(function(name)
        return not allowed_users[name]
    end):each(function(name)
        box.schema.user.drop(name)
    end)

    local allowed_roles = {
        public = true,
        replication = true,
        super = true,
    }
    box.space._user:pairs():filter(function(tuple)
        local tuple_type = tuple[_USER_TYPE]
        return tuple_type == 'role'
    end):map(function(tuple)
        local name = tuple[_USER_NAME]
        return name
    end):filter(function(name)
        return not allowed_roles[name]
    end):each(function(name)
        box.schema.role.drop(name)
    end)

    local _FUNC_NAME = 3
    local _FUNC_LANGUAGE = 5
    local allowed_funcs = {
        ['box.schema.user.info'] = true,
    }
    local allowed_langs = {
        ['SQL_BUILTIN'] = true,
    }
    box.space._func:pairs():map(function(tuple)
        local name = tuple[_FUNC_NAME]
        local lang = tuple[_FUNC_LANGUAGE]
        return { name = name, lang = lang }
    end):filter(function(prop)
        return not allowed_funcs[prop.name]
    end):filter(function(prop)
        return not allowed_langs[prop.lang]
    end):each(function(prop)
        box.schema.func.drop(prop.name)
    end)

    local sql_builtin_func_count = box.space._func:pairs():map(function(tuple)
        local lang = tuple[_FUNC_LANGUAGE]
        if lang == 'SQL_BUILTIN' then
            return 1
        end
        return 0
    end):sum()

    cleanup_cluster()

    local cleanup_list = function(list, allowed)
        for k, _ in pairs(list) do
            if not allowed[k] then
                list[k] = nil
            end
        end
    end

    local allowed_globals = {
        -- modules
        bit = true,
        coroutine = true,
        debug = true,
        io = true,
        jit = true,
        math = true,
        os = true,
        package = true,
        string = true,
        table = true,
        utf8 = true,
        -- variables
        _G = true,
        _VERSION = true,
        arg = true,
        -- functions
        assert = true,
        collectgarbage = true,
        dofile = true,
        error = true,
        gcinfo = true,
        getfenv = true,
        getmetatable = true,
        ipairs = true,
        load = true,
        loadfile = true,
        loadstring = true,
        module = true,
        next = true,
        pairs = true,
        pcall = true,
        print = true,
        rawequal = true,
        rawget = true,
        rawset = true,
        require = true,
        select = true,
        setfenv = true,
        setmetatable = true,
        tonumber = true,
        tonumber64 = true,
        tostring = true,
        type = true,
        unpack = true,
        xpcall = true,
        -- tarantool
        _TARANTOOL = true,
        box = true,
        dostring = true,
        help = true,
        newproxy = true,
        role_check_grant_revoke_of_sys_priv = true,
        tutorial = true,
        update_format = true,
    }
    cleanup_list(_G, allowed_globals)

    local allowed_packages = {
        ['_G'] = true,
        bit = true,
        box = true,
        ['box.backup'] = true,
        ['box.internal'] = true,
        ['box.internal.sequence'] = true,
        ['box.internal.session'] = true,
        ['box.internal.space'] = true,
        buffer = true,
        clock = true,
        console = true,
        coroutine = true,
        crypto = true,
        csv = true,
        debug = true,
        digest = true,
        errno = true,
        ffi = true,
        fiber = true,
        fio = true,
        fun = true,
        help = true,
        ['help.en_US'] = true,
        ['http.client'] = true,
        iconv = true,
        ['internal.argparse'] = true,
        ['internal.trigger'] = true,
        io = true,
        jit = true,
        ['jit.bc'] = true,
        ['jit.bcsave'] = true,
        ['jit.dis_x64'] = true,
        ['jit.dis_x86'] = true,
        ['jit.dump'] = true,
        ['jit.opt'] = true,
        ['jit.p'] = true,
        ['jit.profile'] = true,
        ['jit.util'] = true,
        ['jit.v'] = true,
        ['jit.vmdef'] = true,
        ['jit.zone'] = true,
        json = true,
        log = true,
        math = true,
        msgpack = true,
        msgpackffi = true,
        ['net.box'] = true,
        ['net.box.lib'] = true,
        os = true,
        package = true,
        pickle = true,
        pwd = true,
        socket = true,
        strict = true,
        string = true,
        table = true,
        ['table.clear'] = true,
        ['table.new'] = true,
        tap = true,
        tarantool = true,
        title = true,
        uri = true,
        utf8 = true,
        uuid = true,
        xlog = true,
        yaml = true,
    }
    cleanup_list(package.loaded, allowed_packages)

    local user_count = box.space._user:count()
    assert(user_count == 4 or user_count == 5,
        'box.space._user:count() should be 4 (1.10) or 5 (2.0)')
    assert(box.space._func:count() == 1 + sql_builtin_func_count,
        'box.space._func:count() should be 1 (1.10 and >= 2.10)' ..
        ' or 1 + count of SQL_BUILTIN functions (>= 2.2.1, < 2.10)')
    assert(box.space._cluster:count() == 1,
        'box.space._cluster:count() should be only one')

    box.cfg({listen = box.NULL})
end

-- }}}

clean()
init()
