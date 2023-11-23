local utils = require("lakefs/catalogexport/internal")
local pathlib = require("path")
local url = require("net/url")

--[[
    ### Default exported table structure:

    ${storageNamespace}
    _lakefs/
        exported/
            ${ref}/
                ${commitId}/
                    ${tableName}/
                        ...
]]
local function get_storage_uri_prefix(storage_ns, commit_id, action_info)
    local branch_or_tag = utils.ref_from_branch_or_tag(action_info)
    local sha = utils.short_digest(commit_id)
    return pathlib.join("/", storage_ns, string.format("_lakefs/exported/%s/%s/", branch_or_tag, sha))
end

local function parse_storage_uri(uri)
    local u = url.parse(uri)
    return {
        protocol = u.scheme,
        bucket = u.host,
        key = (u.path:sub(0, 1) == "/") and u.path:sub(2) or u.path,
    }
end

return {
    get_storage_uri_prefix = get_storage_uri_prefix,
    parse_storage_uri = parse_storage_uri
}
