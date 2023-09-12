local pathlib = require("path")
local strings = require("strings")
local yaml = require("encoding/yaml")
local success, lakefs = pcall(require, "lakefs")
local json = require("encoding/json")
local common = require("lakefs/catalog_export/common")
-- TDOO(isan) configure somewhere else 

-- TODO(isan) this is for development remove before merging 
if success then
    print("lakefs module is available")
else
    print("Error loading lakefs module:", lakefs)
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
    return self
end

-- Define methods
function HiveTable:name()
    return self._name
end

function HiveTable:size()
    return nil
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

function HiveTable:_get_partition_values(path)
    local vals = {}
    splitted_path = strings.split(path, pathlib.default_separator())
    for _, part in pairs(splitted_path) do
        for _, col in ipairs(self.partition_cols) do
            local prefix = col .. "="
            if strings.has_prefix(part, prefix) then
                vals[col] = part:sub(#prefix + 1)
            end
        end
    end
    return vals
end

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



function HiveTable:list_objs_and_partitions_test()
    local has_more = true
    local after = ""
    local delimiter = ""
    local count = 1
    local partitions_to_files = {}
    print("searching objects partitions based in base path " .. self._path)
    repeat
        local code, resp = lakefs.list_objects(self.repo_id, self.commit_id, after, self._path, delimiter)
        if code ~= 200 then
            -- TODO(isan) return error to the caller
            error("lakeFS: could not list tables in: " .. self._path .. ", error: " .. resp.message)
        end
        for _, entry in ipairs(resp.results) do
            is_hidden = pathlib.is_hidden(pathlib.parse(entry.path).base_name)
            if not is_hidden and entry.path_type == "object" then
                entry_partitions = self:_get_partition_values(entry.path)
                -- start mapp 
                local partitions = {}
                for k, v in pairs(entry_partitions) do
                    table.insert(partitions, k .. "=" .. v)
                end
                local partition_key = table.concat(partitions, "/")

                if not partitions_to_files[partition_key] then
                    partitions_to_files[partition_key] = {}
                end

                table.insert(partitions_to_files[partition_key], {
                    physical_address = entry.physical_address,
                    path = entry.path,
                    size = entry.size_bytes,
                    checksum = entry.checksum
                })
                -- end map 
            end
        end
        -- check if has more pages 
        has_more = resp.pagination.has_more
        after = resp.pagination.next_offset
    until not has_more
    return partitions_to_files
end

function HiveTable:list_objects()
    local fs = {}
    local pre_signed_urls = _signed_urls_by_path(self._lakefs, self.repo_id, self.ref_id, self._path)
    for _, s in pairs(pre_signed_urls) do
        if string.match(s.path, "%.parquet$") then
            local file_proto = {
                url = s.physical_address,
                id = s.checksum,
                entry_partitions = self:_get_partition_values(s.path),
                size = s.size_bytes
            }
            if s.physical_address_expiry then
                file_proto.expirationTimestamp = math.floor(s.physical_address_expiry * 1000)
            end
            table.insert(fs, {
                file = file_proto
            })
        end
    end
    return fs
end

local TableExtractor = {}
TableExtractor.__index = TableExtractor

function TableExtractor.new(repository_id, ref, commit_id)
    local self = setmetatable({}, TableExtractor)
    -- TODO(isan) use or remove lakefs client injection
    self.lakefs = lakefs
    self.tables_registry_base = pathlib.join(pathlib.default_separator(), '_lakefs_tables/')
    self.repository_id = repository_id
    self.commit_id = commit_id
    self.ref = ref
    return self
end

function TableExtractor:_list_objects(repository_id, commit_id, location, handle_entries)
    local has_more = true
    local after = ""
    local delimiter = ""
    local count = 1
    repeat
        local code, resp = self.lakefs.list_objects(repository_id, commit_id, after, location, delimiter)
        -- handle paged entiries 
        if not handle_entries(code, resp) then
            return
        end
        -- check if has more pages 
        has_more = resp.pagination.has_more
        after = resp.pagination.next_offset
    until not has_more
end

function TableExtractor:list_table_definitions()
    local table_entries = {}
    self:_list_objects(self.repository_id, self.commit_id, self.tables_registry_base, function(code, resp)
        if code ~= 200 then
            -- TODO(isan) return error to the caller
            error("lakeFS: could not list tables in: " .. self.tables_registry_base .. ", error: " .. resp.message)
        end
        for _, entry in ipairs(resp.results) do
            is_hidden = pathlib.is_hidden(pathlib.parse(entry.path).base_name)
            is_yaml = strings.has_suffix(entry.path, ".yaml")
            if not is_hidden and is_yaml and entry.path_type == "object" then
                table.insert(table_entries, {
                    physical_address = entry.physical_address,
                    path = entry.path
                })
            end
        end
        return true
    end)
    return table_entries
end

function TableExtractor:get_table(logical_path)
    code, content = lakefs.get_object(self.repository_id, self.commit_id, logical_path)
    if code ~= 200 then
        -- TODO(isan) propagate error to the caller
        error("could not fetch data file: HTTP " .. tostring(code))
    end

    descriptor = yaml.unmarshal(content)
    -- TODO(isan) implement other tables or handle unsupported
    if descriptor.type == 'hive' then
        return HiveTable.new(self.repository_id, self.ref, self.commit_id, descriptor.path, descriptor.name,
            descriptor.partition_columns or {}, descriptor.schema), nil
    end
    return nil, "NotImplemented: table type: " .. descriptor.type .. " path: " .. descriptor.path
end

return {
    TableExtractor = TableExtractor,
    HiveTable = HiveTable,
    lakefs_hive_partition_it = lakefs_hive_partition_it,
}
