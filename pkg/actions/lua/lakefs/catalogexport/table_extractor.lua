local pathlib = require("path")
local strings = require("strings")
local yaml = require("encoding/yaml")
local common = require("lakefs/catalogexport/common")
local hive = require("lakefs/catalogexport/hive")

local LAKEFS_TABLES_BASE = "_lakefs_tables/"

function _is_table_obj(entry, tables_base)
    if entry == nil or entry.path_type ~= "object" then
        return false
    end
    -- remove _lakefs_tables/ from path 
    local suffix = entry.path:sub(#tables_base, #entry.path)
    local is_hidden = pathlib.is_hidden(suffix)
    local is_yaml = strings.has_suffix(entry.path, ".yaml")
    return not is_hidden and is_yaml
end

-- list all YAML files under _lakefs_tables/*
function list_table_descriptor_files(client, repo_id, commit_id)
    local table_entries = {}
    local page_size = 30
    local iter = common.lakefs_object_pager(client, repo_id, commit_id, "", LAKEFS_TABLES_BASE, page_size, "")
    for entries in iter do
        for _, entry in ipairs(entries) do
            if _is_table_obj(entry, LAKEFS_TABLES_BASE) then
                table.insert(table_entries, {
                    physical_address = entry.physical_address,
                    path = entry.path
                })
            end
        end
    end
    return table_entries
end

-- table as parsed YAML object
function get_table_descriptor(client, repo_id, commit_id, logical_path)
    code, content = client.get_object(repo_id, commit_id, logical_path)
    if code ~= 200 then
        error("could not fetch data file: HTTP " .. tostring(code))
    end
    descriptor = yaml.unmarshal(content)
    return descriptor
end

return {
    list_table_descriptor_files = list_table_descriptor_files,
    get_table_descriptor = get_table_descriptor,
    HiveTableExtractor = hive.HiveTableExtractor,
}
