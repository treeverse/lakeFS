local pathlib = require("path")
local json = require("encoding/json")
local utils = require("lakefs/catalogexport/internal")

package.loaded.lakefs = {
    stat_object = function(repo, commit_id, path)
        return 200, json.marshal({
            physical_address = "s3://bucket/but-the-real-one/" .. path ,
        })
    end
}

local delta_export = require("lakefs/catalogexport/delta_exporter")

local table_paths = {"path/to/table1", "path/to/table2"}

local table_content = {
    ["00000000000000000000.json"] = {
        "{\"commitInfo\":\"some info\"}",
        "{\"add\": {\"path\":\"part-c000.snappy.parquet\"}}",
        "{\"remove\": {\"path\":\"part-c001.snappy.parquet\"}}",
        "{\"protocol\":\"the protocol\"}",
    },
    ["00000000000000000001.json"] = {
        "{\"metaData\":\"some metadata\"}",
        "{\"add\": {\"path\":\"part-c002.snappy.parquet\"}}",
        "{\"remove\": {\"path\":\"part-c003.snappy.parquet\"}}",
    },
}

local mock_delta_client = {
    get_table = function (repo, commit_id, path)
        local content = {}
        for _, v in pairs(table_content) do
            table.insert(content, v)
        end
        return content
    end
}

-- Validates that the given key exists in the table.
local function mock_object_writer(bucket, key, data)
    local parsed_key = pathlib.parse(key)["base_name"]
    assert(table_content[parsed_key], "unfamiliar key: " .. parsed_key)
end

local function assert_physical_address(delta_table_locations)
    local ns = action.storage_namespace
    local commit_id = action.commit_id
    local table_export_prefix = utils.get_storage_uri_prefix(ns, commit_id, action)

    for _, table_path in ipairs(table_paths) do
        local table_name = pathlib.parse(table_path)["base_name"]
        local table_loc = delta_table_locations[table_name]
        if table_loc == nil then
            error("missing table location: " .. table_name)
        end
        local expected_location = pathlib.join("/", table_export_prefix, table_name)
        if expected_location ~= table_loc then
            error(string.format("unexpected table location \"%s\".\nexpected: \"%s\"", table_loc, expected_location))
        end
    end
end

-- Run delta export
local delta_table_locations = delta_export.export_delta_log(action, table_paths, mock_object_writer, mock_delta_client)

-- Test results
assert_physical_address(delta_table_locations)
