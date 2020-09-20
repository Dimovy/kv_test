#!/usr/bin/env tarantool

local json = require "lunajson"
local log = require "lualog"

box.cfg {
    listen = 3301,
    background = true,
    log = 'storage.log',
    pid_file = 'storage.pid'
}
box.once('storage', function()
    storage = box.schema.space.create('storage')
    storage:format({{
        name = 'key',
        type = 'string'
    }, {
        name = 'value',
        type = 'string'
    }})
    box.space.storage:create_index('primary', {
        type = 'hash',
        parts = {'key'}
    })
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

Handler = {}
function Handler:new()
    local private = {}

    local public = {}

    function public:post(reqBody)
        log.info("Post request")
        local status, body = pcall(reqBody.json, reqBody)
        local key, value = body["key"], body["value"]

        if not status or type(key) ~= "string" or type(value) ~= "table" then
            log.error("Error 400: Incorrect input")
            return {
                status = 400,
                body = '{"error": "Incorrect input"}'
            }
        end

        local status, data = pcall(box.space.storage.insert, box.space.storage, {key, value})

        if status then
            return {
                status = 200
            }
        else
            log.error("Error 409: " .. data)
            return {
                status = 409,
                body = '{"error": "' .. data .. '"}'
            }
        end
    end

    function public:put(reqBody)
        local key = reqBody:stash("key")
        log.info("Put request, key:" .. key)
        local status, body = pcall(reqBody.json, reqBody)
        local value = body["value"]

        if not status or type(value) ~= "table" then
            log.error("Error 400: Incorrect input")
            return {
                status = 400,
                body = '{"error": "Incorrect input"}'
            }
        end
        local status, data = pcall(box.space.storage.update, box.space.storage, key, {{"=", 2, value}})

        if data == nil then
            log.error("Error 404: Undefined key: " .. key)
            return {
                status = 404,
                body = '{"error": "Undefined key"}'
            }
        elseif status then
            return {
                status = 200
            }
        end
    end

    function public:get(reqBody)
        local key = reqBody:stash("key")
        log.info("Get request, key:" .. key)
        local status, data = pcall(box.space.storage.get, box.space.storage, key)

        if status and data then
            return {
                status = 200,
                body = json.encode(data[2])
            }
        elseif data == nil then
            log.error("Error 404: Undefined key: " .. key)
            return {
                status = 404,
                body = '{"error": "Undefined key"}'
            }
        end
    end

    function public:delete(reqBody)
        local key = reqBody:stash("key")
        log.info("Request for deletion " .. key)
        local status, data = pcall(box.space.storage.delete, box.space.storage, key)
        if status and data then
            return {
                status = 200,
                body = json.encode(data[2])
            }
        elseif data == nil then
            log.error("Error 404: Undefined key: " .. key)
            return {
                status = 404,
                body = '{"error": "Undefined key"}'
            }
        end
    end

    setmetatable(public, self)
    self.__index = self;
    return public
end

request = Handler:new()

local server = require("http.server").new(localhost, 10888)
router = require("http.router").new({
    charset = "application/json"
})
server:set_router(router)

router:route({
    path = "/kv",
    method = "POST"
}, request:post())

router:route({
    path = "/kv/:key",
    method = "PUT"
}, request:put())

router:route({
    path = "/kv/:key",
    method = "GET"
}, request:get())

router:route({
    path = "/kv/:key",
    method = "DELETE"
}, request:delete())

server:start()
