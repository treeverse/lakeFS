local random = math.random

math.randomseed(os.time())

local function uuid()
    local template ='xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    return string.gsub(template, '[xy]', function (c)
        local v = (c == 'x') and random(0, 0xf) or random(8, 0xb)
        return string.format('%x', v)
    end)
end



local test_id = uuid()
local counter = 1

function setup(thread)
    thread:set("id", counter)
    counter = counter + 1
end

function init(args)
    requests  = 0
    --local msg = "thread %d created"
    --print(msg:format(id))
end

function request()
    requests = requests + 1
    local req_id = test_id .. "_" .. id .. "_" .. requests
    local body = [[-----------------------abcdefg123456789
Content-Disposition: form-data; name="content"; filename="file"
Content-Type: text/plain

content_]] .. req_id .. [[

-----------------------abcdefg123456789--]]
    local headers = {}
    headers["Content-Length"] = tostring(body:len())
    headers["Content-Type"] = "multipart/form-data; boundary=---------------------abcdefg123456789"
    headers["Authorization"] = "Basic QUtJQUpWRDVQM1dUQUZIN0lONVE6a0srSFdBWXkwY1hOelhIUm9QUVRqZ1hSVUVJeDlLUGhSR1pHNXN5WQ=="
    headers["Accept"] = "*/*"
    headers["Connection"] = "Keep-Alive"
    local path = "/api/v1/repositories/repo1/branches/master/objects?loader-request-type=createFile&path=file_" .. req_id .. "_" .. requests
    return wrk.format("POST", path, headers, body)
end
