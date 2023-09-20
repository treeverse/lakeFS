--[[
    ### Symlink File(s) structure:
    
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
    
    ### Inputs:

    - export_storage_type (string): s3
    - aws_access_key_id, aws_secret_access_key, aws_region (string): configuration passed to the s3 client that writes symlinks
    - export_bucket (string): Optional bucket to write symlinks to 
    - export_path (string): Optional path in the bucket to write symlinks to
    - table_source: (string) name of the file YAML to load that defines the table _lakefs_tables/example.yaml
        - Alternative: sources ([]string): lakeFS paths that should be written as symlinks de-coupled from table concept
    - Runtime Inputs: repository, ref, commit

    ### Steps pseudo - With Hive Partitions: 
    ```
    -- TODO what about clean mode if the location is dirty?
    blockstore = Blockstore.new(...)
    table_obj = extract_table(table_source)
    -- TODO how to get the storage namespace automatically?
    export_path = ${storageNamespace}/_lakefs/exported/args.ref/short_sha(args.commit_id)/table.name || args.export_path
    for partition, entries in table_obj.partition_it():
        export_path = pathlib.join(export_path, partition/, symlink.txt) 
        -- TODO how to get separation between bucket and full uri? 
        blockstore.put_object(export_bucket, export_path, table.concat(toFiles(entries), "\n") )
    ```
]] -- strings = require("strings")
-- local SymlinkExporter = {}
-- SymlinkExporter.__index = SymlinkExporter
-- function get_bucket_name_and_path_params(uri)
--     local parsed_uri = require("url").parse(uri)
--     local bucket_name = parsed_uri.host
--     local path_params = parsed_uri.path
--     if path_params ~= nil then
--       path_params = path_params:sub(2) -- remove the leading slash
--     end
--     return bucket_name, path_params
--   end
-- -- returns {bucket, path} in the blockstore for outputs
-- -- if export_bucket and export_path are set: will use those otherwise default
-- function _resolve_export_base_path(repo_storage_ns, repo_id, table_name, export_bucket, export_path)
--     if not export_bucket and not export_path then
--         if strings.has_prefix(repo_storage_ns, "s3://")
--         local b = ""
--         return {
--             -- TODO(isan) extract bucket name from storage namespace that's not real here
--             bucket=repo_storage_ns, 
--             base_path=string.format("${storageNamespace}/_lakefs/exported/args.ref/short_sha(args.commit_id)/table.name")
--         }
--     elseif export_bucket and export_path then
--         return {
--             bucket=export_path, 
--             base_path=export_path
--         }
--     end
--     error(string.format("export validation: export_bucket(%s) and export_path(%s) must be set or left empty", tostring(export_bucket), tostring(export_path)))
-- -- Factory function to create new instances
-- function export_symlinks(lakefs, blockstore, export_bucket, export_path, table_source, repo_id, ref_id, commit_id)
--    print("") 
-- end
local extractor = require("lakefs/catalogexport/table_extractor")
local hive = require("lakefs/catalogexport/hive")
local utils = require("lakefs/catalogexport/internal")
local url = require("net/url")
local pathlib = require("path")
local strings = require("strings")
-- ${storageNamespace}
-- _lakefs/
--     exported/
--         ${ref}/
--             ${commitId}/
--                 ${tableName}/
--                     p1=v1/symlink.txt
--                     p1=v2/symlink.txt
--                     p1=v3/symlink.txt
--                     ...

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
            if trim_obj_base_path then
                storage_key_suffix = strings.replace(part_key, base_path .. "/", "",1)
            end
            storage_key_suffix = pathlib.join("/", descriptor.name, storage_key_suffix, "symlink.txt")
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
