local extractor = require("lakefs/catalogexport/table_extractor")
local hive = require("lakefs/catalogexport/hive")
local utils = require("lakefs/catalogexport/internal")
local pathlib = require("path")
local strings = require("strings")
local lakefs = require("lakefs")

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
local function get_storage_uri_prefix(storage_ns, commit_id, action_info)
    local branch_or_tag = utils.ref_from_branch_or_tag(action_info)
    local sha = utils.short_digest(commit_id)
    return pathlib.join("/", storage_ns, string.format("_lakefs/exported/%s/%s/", branch_or_tag, sha))
end

--[[
    @repo_id: repository id
    @commit_id: commit id of the current table
    @table_src_path: path to table spec (i.e _lakefs_tables/my_table.yaml)
    @options:
    - skip_trim_obj_base_path(boolean) if true will skip removing the prefix path before the partition path. 
]]
local function export_it(repo_id, commit_id, table_src_path, options)
    local opts = options or {}
    local descriptor = extractor.get_table_descriptor(lakefs, repo_id, commit_id, table_src_path)
    if descriptor.type ~= "hive" then
        error("table " .. descriptor.type .. " in path " .. table_src_path .. " not supported")
    end
    if opts.debug then
        print(string.format('%s table `lakefs://%s/%s/%s`', descriptor.type, repo_id, utils.short_digest(commit_id),
            descriptor.path))
    end
    local base_path = descriptor.path
    local cols = descriptor.partition_columns
    local pager = hive.extract_partition_pager(lakefs, repo_id, commit_id, base_path, cols)
    return function()
        local part_key, entries = pager()
        if part_key == nil then
            return nil
        end
        local symlink_data = ""
        for _, entry in ipairs(entries) do
            symlink_data = symlink_data .. entry.physical_address .. "\n"
        end
        -- create key suffix for symlink file 
        local storage_key_suffix = part_key
        if #descriptor.partition_columns == 0 then
            storage_key_suffix = descriptor.name .. "/" .. "symlink.txt"
        else
            if not opts.skip_trim_obj_base_path then
                storage_key_suffix = strings.replace(part_key, base_path .. "/", "", 1) -- remove base_path prefix from partition path
            end
            -- append to partition path to suffix 
            storage_key_suffix = pathlib.join("/", descriptor.name, storage_key_suffix, "symlink.txt")
        end
        return {
            key_suffix = storage_key_suffix,
            data = symlink_data
        }
    end
end

--[[
    export a Symlinks that represent a table to S3 
    @s3_client: configured client 
    @table_src_path: object path to the table spec (_lakefs_tables/my_table.yaml)
    @action_info: the global action object 
    @options: 
    - debug(boolean)
    - export_base_uri(string): override the prefix in S3 i.e s3://other-bucket/path/ 
    - writer(function(bucket, key, data)): if passed then will not use s3 client, helpful for debug
]]
local function export_s3(s3_client, table_src_path, action_info, options)
    local opts = options or {}
    local repo_id = action_info.repository_id
    local commit_id = action_info.commit_id
    local base_prefix = opts.export_base_uri or action_info.storage_namespace
    local export_base_uri = get_storage_uri_prefix(base_prefix, commit_id, action_info)
    local location = utils.parse_storage_uri(export_base_uri)
    local put_object = opts.writer or s3_client.put_object
    local it = export_it(repo_id, commit_id, table_src_path, opts)
    for symlink in it do
        local key = pathlib.join("/", location.key, symlink.key_suffix)
        if opts.debug then
            print("S3 writing bucket: " .. location.bucket .. " key: " .. key)
        end
        put_obj(location.bucket, key, symlink.data)
    end
    return {
        location = location
    }
end

return {
    export_s3 = export_s3
}
