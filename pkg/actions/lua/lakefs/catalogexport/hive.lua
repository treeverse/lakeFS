local pathlib = require("path")
local utils = require("lakefs/catalogexport/internal")
local strings = require("strings")
local DEFAULT_PAGE_SIZE = 30 

-- extract partition prefix from full path
local function extract_partitions_path(partitions, path)
    if partitions == nil or #partitions == 0 then
        return ""
    end
    local idx = 1
    local is_partition_prefix = strings.has_prefix(path, partitions[1])
    for part_idx, partition in ipairs(partitions) do
        local col_substr = "/" ..  partition .. "="
        -- if partition is the path prefix and we are the that first partition remove /
        if part_idx == 1 and is_partition_prefix then
            col_substr = partition .. "="
        end
        local i, j = string.find(path, col_substr, idx)
        if i == nil then
            return nil
        end
        local separator_idx = string.find(path, "/", j+1)
        -- verify / found and there is something in between = ... / 
        if separator_idx == nil or separator_idx <= (j + 1) then
            return nil
        end
        idx = separator_idx
    end
    return string.sub(path, 1, idx)
end

-- Hive format partition iterator each result set is a collection of files under the same partition
local function extract_partition_pager(client, repo_id, commit_id, base_path, partition_cols, page_size)
    local target_partition = ""
    local pager = utils.lakefs_object_pager(client, repo_id, commit_id, "", base_path,"", page_size or DEFAULT_PAGE_SIZE)
    local page = pager()
    return function()
        if page == nil then
            return nil
        end
        local partition_entries = {}
        while true do
            if #page == 0 then
                page = pager()
                if page == nil then -- no more records
                    return target_partition, partition_entries
                end
            end
            local entry = page[1]
            if not pathlib.is_hidden(entry.path) then
                local partition_key = extract_partitions_path(partition_cols, entry.path)
                -- first time: if not set, assign current object partition as the target_partition key
                if target_partition == "" then
                    target_partition = partition_key
                end
                -- break if current entry does not belong to the target_partition
                if partition_key ~= target_partition then
                    local partition_result = target_partition
                    target_partition = partition_key
                    return partition_result, partition_entries
                end
                table.insert(partition_entries, {
                    physical_address = entry.physical_address,
                    path = entry.path,
                    size = entry.size_bytes,
                    checksum = entry.checksum
                })
                -- remove entry only if its part of the current partition
                table.remove(page, 1)
            end
        end
    end
end

return {
    extract_partition_pager=extract_partition_pager,
}