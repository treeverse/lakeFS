local action = {
    repository_id = "myRepo",
    commit_id = "myCommit",
    branch_id = "myBranch",
}

-- table names must be unique
local test_cases = {
    {
        name = "failed_not_delta_type",
        tables = {
            ["my_table_not_delta"] = {
                td = {
                    type = "notDelta",
                    catalog = "ok",
                    name = "notDelta",
                },
            },
        },
        error = "registration failed",
    },
    {
        name = "failed_no_catalog_name",
        tables = {
            ["my_table_no_catalog"] = {
                td = {
                    type = "delta",
                    name = "noCatalog",
                },
            },
        },
        error = "catalog name is required",
    },
    {
        name = "failed_no_name",
        tables = {
            ["my_table_no_name"] = {
                td = {
                    type = "delta",
                    catalog = "ok",
                },
            },
        },
        error = "table name is required",
    },
    {
        name = "failed_schema_creation",
        tables = {
            ["my_table_schema_failure"] = {
                td = {
                    type = "delta",
                    catalog = "ok",
                    name = "schemaFailure",
                },
            },
        },
        schema_failure = true,
        error = "failed creating/getting catalog's schema",
    },
    {
        name = "success_all_tables",
        tables = {
            ["my_table_success"] = {
                status = "SUCCEEDED",
            },
            ["my_table2_success"] = {
                status = "SUCCEEDED",
            },
            ["my_table3_success"] = {
                status = "SUCCEEDED",
            },
        },
    },
    {
        name = "mixed_statuses",
        tables = {
            ["my_table_failure"] = {
                status = "FAILED",
            },
            ["my_table2_success_2"] = {
                status = "SUCCEEDED",
            },
            ["my_table3_failure"] = {
                status = "FAILED",
            },
        },
    },
}

-- Loads a mock table (descriptor) extractor
local function load_table_descriptor(tables)
    package.loaded["lakefs/catalogexport/table_extractor"] = {
        get_table_descriptor = function(_, _, _, table_src_path)
            local examined_tables = {}
            for name, t in pairs(tables) do
                table.insert(examined_tables, name)
                if string.find(table_src_path, name) then
                    if not t.td then
                        return {
                            type = "delta",
                            catalog = "ok",
                            name = name
                        }
                    end
                    return t.td
                end
            end
            error("test was configured incorrectly. expected to find a table descriptor for table \"" .. table_src_path .. "\" but no such was found." )
        end
    }
end

-- Generates a mock databricks client
local function db_client(schema_failure, tables)
    return {
        create_or_get_schema = function(branch_id, catalog)
            if schema_failure then
                return nil
            end
            return catalog .. "." .. branch_id
        end,
        register_external_table = function(table_name, _, _, _, _)
            for name, t in pairs(tables) do
                if name == table_name then
                    return t.status
                end
            end
        end
    }
end

-- Start tests
for _, test in ipairs(test_cases) do
    package.loaded["lakefs/catalogexport/unity_exporter"] = nil
    load_table_descriptor(test.tables)
    local unity_export = require("lakefs/catalogexport/unity_exporter")
    local err = test.error
    local schema_failure = test.schema_failure
    local test_tables = test.tables
    local table_paths = {}
    for name, _ in pairs(test_tables) do
        table_paths[name] = "s3://physical/" .. name
    end

    local db = db_client(schema_failure, test_tables)
    -- Run test:
    local s, resp = pcall(unity_export.register_tables, action, "_lakefs_tables", table_paths, db, "id")
    if err ~= nil then
        if s ~= false then -- the status is true which means no error was returned
            local str_resp = ""
            for k, v in pairs(resp) do
                str_resp = str_resp .. k .. " = " .. v .. "\n"
            end
            error("test " .. test.name .. " expected an error:\n" .. err .. "\nbut returned status: \"" .. tostring(s) .. "\"\nresponse:\n" .. str_resp)
        end
        -- status is false as expected -> error returned
        if string.find(resp, err) == nil then
            error("test " .. test.name .. " returned incorrect error.\nexpected:\n" .. err .. "\nactual:\n" .. resp)
        end
    else
        for table_name, status in pairs(resp) do
            local expected_status = test.tables[table_name].status
            if expected_status ~= status then
                error("test " .. test.name .. " returned incorrect status for table \"" .. table_name .."\"\nexpected: \"" .. expected_status .. "\"\nactual:\n\"" .. status .. "\"")
            end
        end
    end
end
