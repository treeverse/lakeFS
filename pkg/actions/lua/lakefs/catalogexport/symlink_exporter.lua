local extractor = require("lakefs/catalogexport/table_extractor")
local hive = require("lakefs/catalogexport/hive")
local utils = require("lakefs/catalogexport/internal")

local function export(lakefs_client, repo_id, ref, commit_id, table_src_path, blockstore_write)
    local descriptor = extractor.get_table_descriptor(lakefs_client, repo_id, commit_id, table_src_path)
    if descriptor.type == "hive" then
        local base_path = descriptor.path
        local pager = hive.extract_partition_pager(lakefs_client, repo_id, commit_id, base_path, descriptor.partition_columns)
        local sha = utils.short_digest(commit_id)
        print(string.format('%s table representation for `lakefs://%s/%s/%s` digest: %s',descriptor.type,  repo_id, ref,descriptor.path, sha))
        for part_key, entries in pager do
            print("Partition Result: " .. part_key .. " #: " .. tostring(#entries))
            for _, entry in ipairs(entries) do
                print(" > path: " .. entry.path .. " physical: " .. entry.physical_address)
            end 
        end
    else
        error("table " .. descriptor.type .. " in path " .. table_src_path .. " not supported")
    end
end

return {
    export = export
}
