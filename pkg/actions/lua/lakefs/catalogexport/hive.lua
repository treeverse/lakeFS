local regexp = require("regexp")
local pathlib = require("path")
local common = require("lakefs/catalogexport/common")

-- extract partition prefix from full path
function extract_partitions_path(partition_cols, path)
    -- list of columns to pattern {a,b,c} -> a=*/b=*/c=*/
    local partition_pattern = table.concat(partition_cols, "=[^/]*/") .. "=[^/]*/"
    local re = regexp.compile(partition_pattern)
    local match = re.find(path, partition_pattern)
    if match == "" then
        return nil
    end
    return match
end

-- Hive format partition iterator each result set is a collection of files under the same partition
function lakefs_hive_partition_pager(client, repo_id, commit_id, base_path, page_size, partition_cols)
    local after = ""
    local has_more = true
    local prefix = base_path
    local target_partition = ""
    local page = {}
    return function()
        if not has_more then
            return nil
        end
        local partition_entries = {}
        repeat
            if #page == 0 then
                local nextPage = common.lakefs_object_pager(client, repo_id, commit_id, after, prefix, page_size, "")
                page = nextPage()
                if page == nil or #page == 0 then -- no more records
                    has_more = false
                    return target_partition, partition_entries
                else -- set next offset
                    after = page[#page].path
                end
            end
            repeat
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
            until page == nil or #page == 0
        until not has_more
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
