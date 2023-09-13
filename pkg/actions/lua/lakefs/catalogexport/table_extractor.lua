local pathlib = require("path")
local strings = require("strings")
local yaml = require("encoding/yaml")
local lakefs = require("lakefs")
local json = require("encoding/json")
local common = require("lakefs/catalogexport/common")

-- return partition table from path by based on columns in partition_cols
function get_partition_values(partition_cols, path)
    local vals = {}
    splitted_path = strings.split(path, pathlib.default_separator())
    for _, part in pairs(splitted_path) do
        for _, col in ipairs(partition_cols) do
            local prefix = col .. "="
            if strings.has_prefix(part, prefix) then
                vals[col] = part:sub(#prefix + 1)
            end
        end
    end
    return vals
end

-- extract partition substr from full path
function extract_partition_prefix_from_path(partition_cols, path)
    local partitions = get_partition_values(partition_cols, path)
    local partitions_list = {}
    -- iterate partition_cols to maintain order  
    for _, col in pairs(partition_cols) do
        table.insert(partitions_list, col .. "=" .. partitions[col])
    end
    local partition_key = table.concat(partitions_list, pathlib.default_separator())
    return partition_key
end

-- Hive format partition iterator each result set is a collection of files under the same partition
function lakefs_hive_partition_it(client, repo_id, commit_id, base_path, page_size, delimiter, partition_cols)
    local after = ""
    local has_more = true
    local prefix = base_path
    local target_partition = ""
    return function()
        if not has_more then
            return nil
        end
        local partition_entries = {}
        local iter = common.lakefs_object_it(client, repo_id, commit_id, after, prefix, page_size, delimiter)
        for entries in iter do
            for _, entry in ipairs(entries) do
                is_hidden = pathlib.is_hidden(pathlib.parse(entry.path).base_name)
                if not is_hidden and entry.path_type == "object" then
                    local partition_key = extract_partition_prefix_from_path(partition_cols, entry.path)
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
function HiveTable.new(repo_id, ref_id, commit_id, path, name, partition_cols, schema)
    local self = setmetatable({}, HiveTable)
    self.repo_id = repo_id
    self.ref_id = ref_id
    self.commit_id = commit_id
    self._path = path
    self._name = name
    self.schema = schema
    self.partition_cols = partition_cols
    self._iter_page_size = common.LAKEFS_DEFAULT_PAGE_SIZE
    return self
end

-- Define methods
function HiveTable:name()
    return self._name
end

function HiveTable:description()
    return string.format('Hive table representation for `lakefs://%s/%s/%s` digest: %s', self.repo_id, self.ref_id,
        tostring(self._path), self.commit_id:sub(1, common.SHORT_DIGEST_LEN))
end

function HiveTable:path()
    return self._path
end

function HiveTable:min_reader_version()
    return 1
end

function HiveTable:schema_string()
    return json.marshal(self.schema)
end

function HiveTable:partition_columns()
    return self.partition_cols
end

function HiveTable:version()
    return 0
end

function HiveTable:partition_iterator()
    return lakefs_hive_partition_it(lakefs, self.repo_id, self.commit_id, self._path, self._iter_page_size, "",
        self.partition_cols)
end

local TableExtractor = {}
TableExtractor.__index = TableExtractor

function TableExtractor.new(repository_id, ref, commit_id)
    local self = setmetatable({}, TableExtractor)
    self.tables_registry_base = pathlib.join(pathlib.default_separator(), '_lakefs_tables/')
    self.repository_id = repository_id
    self.commit_id = commit_id
    self.ref = ref
    self._iter_page_size = common.LAKEFS_DEFAULT_PAGE_SIZE
    return self
end

-- list all YAML files in _lakefs_tables
function TableExtractor:list_table_definitions()
    local table_entries = {}
    local iter = common.lakefs_object_it(lakefs, self.repository_id, self.commit_id, "", self.tables_registry_base,
        self._iter_page_size, "")
    for entries in iter do
        for _, entry in ipairs(entries) do
            is_hidden = pathlib.is_hidden(pathlib.parse(entry.path).base_name)
            is_yaml = strings.has_suffix(entry.path, ".yaml")
            if not is_hidden and is_yaml and entry.path_type == "object" then
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
            descriptor.partition_columns or {}, descriptor.schema), nil
    end
    return nil, "NotImplemented: table type: " .. descriptor.type .. " path: " .. logical_path
end

return {
    TableExtractor = TableExtractor,
    lakefs_hive_partition_it = lakefs_hive_partition_it
}
