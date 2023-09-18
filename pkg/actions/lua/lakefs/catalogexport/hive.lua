local regexp = require("regexp")
local pathlib = require("path")
local common = require("lakefs/catalogexport/common")

-- extract partition prefix from full path
function extract_partitions_path(partitions, path)
    local idx = 0
    for _, partition in ipairs(partitions) do
        local col_substr = partition .. "="
        local i, j = string.find(path, col_substr, idx)
        if i == nil then
            return "nil"
        end
        local start_val, end_val = string.find(path, "/", j+1)
        idx = end_val 
    end
    return string.sub(path, 1, idx)
end

-- Hive format partition iterator each result set is a collection of files under the same partition
function lakefs_hive_partition_pager(client, repo_id, commit_id, base_path, page_size, partition_cols)
    local prefix = base_path
    local target_partition = ""
    local pager = common.lakefs_object_pager(client, repo_id, commit_id, "", prefix, page_size, "")
    local page = pager()
    return function()
        if page == nil then
            return nil
        end
        local partition_entries = {}
        while(true) do
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

-- Define the HiveTableExtractor class
local HiveTableExtractor = {}
HiveTableExtractor.__index = HiveTableExtractor

-- Factory function to create new instances
function HiveTableExtractor.new(lakefs_client, repo_id, ref_id, commit_id, descriptor)
    local self = setmetatable({}, HiveTableExtractor)
    self.lakefs_client = lakefs_client
    self.repo_id = repo_id
    self.ref_id = ref_id
    self.commit_id = commit_id
    self.descriptor = descriptor
    self.path = descriptor.path
    self.name = descriptor.name
    self.schema = descriptor.schema
    self.partition_cols = descriptor.partition_columns or {}
    return self
end

-- Define methods
function HiveTableExtractor:description()
    return string.format('Hive table representation for `lakefs://%s/%s/%s` digest: %s', self.repo_id, self.ref_id,
        tostring(self.path), common.short_digest(self.commit_id))
end

function HiveTableExtractor:version()
    return 0
end

function HiveTableExtractor:partition_pager(page_size)
    return lakefs_hive_partition_pager(self.lakefs_client, self.repo_id, self.commit_id, self.path, page_size,
        self.partition_cols)
end

return {
    HiveTableExtractor = HiveTableExtractor,
    lakefs_hive_partition_it = lakefs_hive_partition_it
}
