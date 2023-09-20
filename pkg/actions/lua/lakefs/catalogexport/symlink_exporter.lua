local extractor = require("lakefs/catalogexport/table_extractor")
local hive = require("lakefs/catalogexport/hive")
local utils = require("lakefs/catalogexport/internal")
local url = require("net/url")
local pathlib = require("path")
local strings = require("strings")

-- make it a function so that @nopcoder can ask why we bloat the code? 
local function parse_storage_uri(uri)
    local parsed_uri = url.parse(uri)
    local storage = parsed_uri.scheme
    local bucket = parsed_uri.host
    local key = parsed_uri.path
    if key ~= nil then
        key = key:sub(2) -- remove the leading slash
    end
    return storage, bucket, key
end

--[[
    ### Default Symlink File(s) structure:
    
    ${storageNamespace}
    _lakefs/
        exported/
            ${ref}/
                ${commitId}/
                    ${tableName}/
                        p1=v1/symlink.txt
                        p1=v2/symlink.txt
                        p1=v3/symlink.txt
                        ...
]]
local function gen_storage_uri_prefix(storage_ns, commit_id, action_info)
    local branch_or_tag = utils.ref_from_branch_or_tag(action_info)
    return pathlib.join("/", storage_ns, "_lakefs/exported", branch_or_tag, utils.short_digest(commit_id), "/")
end

local function new_std_table_writer(export_base_uri)
    local storage, bucket, key_prefix = parse_storage_uri(export_base_uri)
    return function(suffix_key, data)
        local key = pathlib.join("/", key_prefix, suffix_key)
        print("[STD Writer Put Object] Bucket " .. bucket .. " key: " .. key .. "  content: \n" .. data)
    end
end

local function new_s3_table_writer(client, export_base_uri)
    local storage, bucket, key_prefix = parse_storage_uri(export_base_uri)
    return function(suffix_key, data)
        local key = pathlib.join("/", key_prefix, suffix_key)
        client.put_object(bucket, key, data)
        print("S3 Put: bucket: " .. bucket .. " key: " .. key)
    end
end

local function export(lakefs_client, repo_id, commit_id, table_src_path, blockstore_write, trim_obj_base_path)
    local descriptor = extractor.get_table_descriptor(lakefs_client, repo_id, commit_id, table_src_path)
    if descriptor.type == "hive" then
        local base_path = descriptor.path
        local pager = hive.extract_partition_pager(lakefs_client, repo_id, commit_id, base_path,
            descriptor.partition_columns)
        local sha = utils.short_digest(commit_id)
        print(string.format('%s table `lakefs://%s/%s/%s`', descriptor.type, repo_id, sha, descriptor.path))
        for part_key, entries in pager do
            local symlink_data = ""
            for _, entry in ipairs(entries) do
                symlink_data = symlink_data .. entry.physical_address .. "\n"
            end
            -- create key suffix for symlink file 
            local storage_key_suffix = part_key
            if #descriptor.partition_columns == 0 then 
                storage_key_suffix = pathlib.join("/", descriptor.name, "symlink.txt")
            else
                if trim_obj_base_path then
                    storage_key_suffix = strings.replace(part_key, base_path .. "/", "", 1) -- remove base_path prefix from partition path
                end
                -- append to partition path to suffix 
                storage_key_suffix = pathlib.join("/", descriptor.name, storage_key_suffix, "symlink.txt")
            end
            -- write symlink file to blockstore 
            blockstore_write(storage_key_suffix, symlink_data)
        end
    else
        error("table " .. descriptor.type .. " in path " .. table_src_path .. " not supported")
    end
end

return {
    export = export,
    new_s3_table_writer = new_s3_table_writer,
    new_std_table_writer = new_std_table_writer,
    gen_storage_uri_prefix = gen_storage_uri_prefix
}
