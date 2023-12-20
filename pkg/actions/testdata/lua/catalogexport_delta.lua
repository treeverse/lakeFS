local pathlib = require("path")
local json = require("encoding/json")
local utils = require("lakefs/catalogexport/internal")


local test_data = {
    --[[
        Used to mark the objects to which a stat_object request was issued.
        {
            "table_path1": { "file1.parquet" = true, "file2.parquet" = true, ...},
            "table_path2": { "file1.parquet" = true, "file2.parquet" = true, ...}
        }
    ]]
    table_to_objects = {},
    --[[
        Used to validate the expected Delta Log content
        {
            "<physical_table_log_entry_address>" = "<physical log content>",
            ...
        }
    ]]
    output_delta_log = {},
    --[[ Used to return a mock response from "delta_client.get_table()"
        {
           "<n>" = {<initial log content>},
           "<n+1>" = {<initial log content>},
        }
    ]]
    table_logs_content = {},
    --[[ Used to validate the expected log content for a given table.
        {<table_name1> = {
           "<n>" = {<expected log content>},
           "<n+1>" = {<expected log content>},
        }},
        ...
    ]]
    table_expected_log = {},
}

local function generate_physical_address(path)
    return "s3://" .. path
end

package.loaded["lakefs/catalogexport/table_extractor"] = {
    get_table_descriptor = function(_, _, _, table_src_path)
        local t_name_yaml = pathlib.parse(table_src_path)
        assert(t_name_yaml["base_name"] == ".yaml")
        local t_name = pathlib.parse(t_name_yaml["parent"])
        return {
            name = t_name["base_name"]
        }
    end
}

package.loaded.lakefs = {
    stat_object = function(_, _, path)
        local parsed_path = pathlib.parse(path)
        local table_path_base = parsed_path["parent"]
        if not test_data.table_to_objects[table_path_base] then
            test_data.table_to_objects[table_path_base] = {}
        end
        -- mark the given parquet file path under a specific table as requested.
        test_data.table_to_objects[table_path_base][parsed_path["base_name"]] = true
        return 200, json.marshal({
            physical_address = generate_physical_address(path) ,
        })
    end
}

local delta_export = require("lakefs/catalogexport/delta_exporter")

local function mock_delta_client(table_logs_content)
    return {
        get_table = function (_, _, path)
            --[[ For the given table's path:
                {"0" = <logical log content>, "1" = <logical log content>}
            ]]
            return table_logs_content[path]
        end
    }
end

local function mock_object_writer(_, key, data)
    test_data.output_delta_log[key] = data
end

local function assert_physical_address(delta_table_locations, table_paths)
    local ns = action.storage_namespace
    local commit_id = action.commit_id
    local table_export_prefix = utils.get_storage_uri_prefix(ns, commit_id, action)

    for _, table_path in ipairs(table_paths) do
        local table_name = pathlib.parse(table_path)["base_name"]
        local table_loc = delta_table_locations[table_path]
        if table_loc == nil then
            error("missing table location: " .. table_path)
        end
        local expected_location = pathlib.join("/", table_export_prefix, table_name)
        if expected_location ~= table_loc then
            error(string.format("unexpected table location \"%s\".\nexpected: \"%s\"", table_loc, expected_location))
        end
    end
end

local function assert_lakefs_stats(table_paths, content_paths)
    for _, table_path in ipairs(table_paths) do
        local table = test_data.table_to_objects[table_path]
        if not table then
            error("missing lakeFS stat_object call for table path: " .. table_path .. "\n")
        end
        for _, data_path in ipairs(content_paths) do
            if not table[data_path] then
                error("missing lakeFS stat_object call for data path: " .. data_path .. " in table path: " .. table_path .. "\n")
            end
        end
    end
end

local function assert_delta_log_content(delta_table_locations, table_to_physical_content)
    for table_path, table_loc in pairs(delta_table_locations) do
        local table_name = pathlib.parse(table_path)["base_name"]
        local table_loc_key = utils.parse_storage_uri(table_loc).key
        local content_table = table_to_physical_content[table_name]
        if not content_table then
            error("unknown table " .. table_name)
        end
        for entry, content in pairs(content_table) do
            local full_key = table_loc_key .. "/" .. entry
            local output_content = test_data.output_delta_log[full_key]
            if not output_content then
                error("missing log file for path: " .. full_key .. "\n")
            end
            local str_content = ""
            for _, row in ipairs(content) do
                str_content = str_content .. row .. "\n"
            end
            if output_content ~= str_content then
                error("expected content:\n" .. str_content .. "\n\nactual content:\n" .. output_content)
            end
        end
    end
end

-- Test data
local data_paths = { "part-c000.snappy.parquet", "part-c001.snappy.parquet", "part-c002.snappy.parquet", "part-c003.snappy.parquet" }
local test_table_paths = {"path/to/table1/", "path/to/table2/"}

for _, table_path in ipairs(test_table_paths) do
    local table_name = pathlib.parse(table_path)["base_name"]
    test_data.table_logs_content[table_path] = {
        ["_delta_log/00000000000000000000.json"] = {
            "{\"commitInfo\":\"some info\"}",
            "{\"add\": {\"path\":\"part-c000.snappy.parquet\"}}",
            "{\"remove\": {\"path\":\"part-c001.snappy.parquet\"}}",
            "{\"protocol\":\"the protocol\"}",
        },
        ["_delta_log/00000000000000000001.json"] = {
            "{\"metaData\":\"some metadata\"}",
            "{\"add\": {\"path\":\"part-c002.snappy.parquet\"}}",
            "{\"remove\": {\"path\":\"part-c003.snappy.parquet\"}}",
        }
    }
    test_data.table_expected_log[table_name] = {
        ["_delta_log/00000000000000000000.json"] = {
            "{\"commitInfo\":\"some info\"}",
            "{\"add\":{\"path\":\"" .. generate_physical_address(table_path .. "part-c000.snappy.parquet") .. "\"}}",
            "{\"remove\":{\"path\":\"" .. generate_physical_address(table_path .. "part-c001.snappy.parquet") .. "\"}}",
            "{\"protocol\":\"the protocol\"}",
        },
        ["_delta_log/00000000000000000001.json"] = {
            "{\"metaData\":\"some metadata\"}",
            "{\"add\":{\"path\":\"" .. generate_physical_address(table_path .. "part-c002.snappy.parquet") .. "\"}}",
            "{\"remove\":{\"path\":\"" .. generate_physical_address(table_path .. "part-c003.snappy.parquet") .. "\"}}",
        }
    }
end


-- Run Delta export test
local delta_table_locations = delta_export.export_delta_log(
        action,
        test_table_paths,
        mock_object_writer,
        mock_delta_client(test_data.table_logs_content),
        "some_path"
)

-- Test results
assert_lakefs_stats(test_table_paths, data_paths)
assert_physical_address(delta_table_locations, test_table_paths)
assert_delta_log_content(delta_table_locations, test_data.table_expected_log)
