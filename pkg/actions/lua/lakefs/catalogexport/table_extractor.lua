local regexp = require("regexp")
local pathlib = require("path")
local strings = require("strings")
local yaml = require("encoding/yaml")
local lakefs = require("lakefs")
local json = require("encoding/json")
local common = require("lakefs/catalogexport/common")

-- extract partition prefix from full path
function extract_partitions_path(partition_cols, path)
    local idx = 0
    for _, partition in ipairs(partition_cols) do
        local pattern = partition .. "=[^/]*"
        local re = regexp.compile(pattern)
        local match = re.find(path, pattern)
        if match == "" then
            return nil
        end
        -- expanding the pattern to a match regex because string.find() does not implement pattern matching https://github.com/Shopify/go-lua/blob/main/string.go#L37  
        local i, j = string.find(path, match, idx)
        idx = j + 1
    end
    return path:sub(1, idx)
end

-- Hive format partition iterator each result set is a collection of files under the same partition
function lakefs_hive_partition_pager(client, repo_id, commit_id, base_path, page_size, partition_cols)
    local after = ""
    local has_more = true
    local prefix = base_path
    local target_partition = ""
    return function()
        if not has_more then
            return nil
        end
        local partition_entries = {}
        local iter = common.lakefs_object_pager(client, repo_id, commit_id, after, prefix, page_size, "")
        for entries in iter do
            for _, entry in ipairs(entries) do
                if not pathlib.is_hidden(pathlib.parse(entry.path).base_name) then
                    local partition_key = extract_partitions_path(partition_cols, entry.path)
                    -- first time: if not set, assign current object partition as the target_partition key 
                    if target_partition == "" then
                        target_partition = partition_key
                    end
                    -- break if current entry does not belong to the target_partition
                    if partition_key ~= target_partition then
                        -- next time start searching AFTER the last used key 
                        after = partition_entries[#partition_entries].path
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
                end
            end
        end
        has_more = false
        return target_partition, partition_entries
    end
end

-- Define the HiveTable class
local HiveTable = {}
HiveTable.__index = HiveTable

-- Factory function to create new instances
function HiveTable.new(repo_id, ref_id, commit_id, path, name, partition_cols, schema, iter_page_size)
    local self = setmetatable({}, HiveTable)
    self.repo_id = repo_id
    self.ref_id = ref_id
    self.commit_id = commit_id
    self._path = path
    self._name = name
    self.schema = schema
    self.partition_cols = partition_cols
    self._iter_page_size = iter_page_size
    return self
end

-- Define methods
function HiveTable:name()
    return self._name
end

function HiveTable:description()
    return string.format('Hive table representation for `lakefs://%s/%s/%s` digest: %s', self.repo_id, self.ref_id,
        tostring(self._path), common.short_digest(self.commit_id))
end

function HiveTable:path()
    return self._path
end

function HiveTable:partition_columns()
    return self.partition_cols
end

function HiveTable:version()
    return 0
end

function HiveTable:partition_pager()
    return lakefs_hive_partition_pager(lakefs, self.repo_id, self.commit_id, self._path, self._iter_page_size,
        self.partition_cols)
end

local TableExtractor = {}
TableExtractor.__index = TableExtractor

function TableExtractor.new(repository_id, ref, commit_id, tables_iter_page_size, export_iter_page_size)
    local self = setmetatable({}, TableExtractor)
    self.tables_registry_base = '_lakefs_tables/'
    self.repository_id = repository_id
    self.commit_id = commit_id
    self.ref = ref
    -- object iterator when listing _lakefs_tables/*
    self._iter_page_size = tables_iter_page_size
    -- object iteration when listing partitions to export (table objects)
    self._export_iter_page_size = export_iter_page_size
    return self
end

function TableExtractor:_is_table_obj(entry)
    if entry == nil or entry.path_type ~= "object" then
        return false 
    end
    -- remove _lakefs_tables/ from path 
    local suffix = entry.path:sub(#self.tables_registry_base, #entry.path)
    local is_hidden = pathlib.is_hidden(suffix)
    local is_yaml = strings.has_suffix(entry.path, ".yaml")
    return not is_hidden and is_yaml
end
-- list all YAML files in _lakefs_tables
function TableExtractor:list_table_definitions()
    local table_entries = {}
    local iter = common.lakefs_object_pager(lakefs, self.repository_id, self.commit_id, "", self.tables_registry_base,
        self._iter_page_size, "")
    for entries in iter do
        for _, entry in ipairs(entries) do
            if self:_is_table_obj(entry) then
                table.insert(table_entries, {
                    physical_address = entry.physical_address,
                    path = entry.path
                })
            end
        end
    end
    return table_entries
end

-- return concrete table instance 
function TableExtractor:get_table(logical_path)
    code, content = lakefs.get_object(self.repository_id, self.commit_id, logical_path)
    if code ~= 200 then
        error("could not fetch data file: HTTP " .. tostring(code))
    end

    descriptor = yaml.unmarshal(content)
    -- TODO(isan) decide where to handle different table parsing? (delta table / glue table etc)
    if descriptor.type == 'hive' then
        return HiveTable.new(self.repository_id, self.ref, self.commit_id, descriptor.path, descriptor.name,
            descriptor.partition_columns or {}, descriptor.schema, self._export_iter_page_size)
    end
    error("NotImplemented: table type: " .. descriptor.type .. " path: " .. logical_path)
end

return {
    TableExtractor = TableExtractor,
    lakefs_hive_partition_pager = lakefs_hive_partition_pager
}
