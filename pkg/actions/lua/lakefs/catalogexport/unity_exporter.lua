--[[ TABLE SPECIFICATION:   _lakefs_tables/<table path>
name: <table name>
type: delta
catalog: <catalog name>
]]
local strings = require("strings")
local pathlib = require("path")
local lakefs = require("lakefs")
local extractor = require("lakefs/catalogexport/table_extractor")
--[[
    - table_descriptors_path: the path under which the table descriptors reside (e.g. "_lakefs_tables").
      It's necessary that every <table path> in the provided `table_paths` will have a complementary
      `<table_descriptors_path>/<table path>.yaml` file describing the used Delta Table.
    - delta_table_paths: a mapping of Delta Lake table descriptors yaml name (with or without ".yaml" extension) to their locations in the object storage
        { <delta table name yaml>: <physical location in the object storage> }
    - databricks_client: a client to interact with databricks.
    - warehouse_id: Databricks warehouse ID

    Returns a "<table name>: status" map for registration of provided tables.
]]
local function register_tables(action, table_descriptors_path, delta_table_paths, databricks_client, warehouse_id)
    local repo = action.repository_id
    local commit_id = action.commit_id
    if not commit_id then
        error("missing commit id")
    end
    local branch_id = action.branch_id
    local response = {}
    for table_name_yaml, table_details in pairs(delta_table_paths) do
        local tny  = table_name_yaml
        if not strings.has_suffix(tny, ".yaml") then
            tny = tny .. ".yaml"
        end
        local table_src_path = pathlib.join("/", table_descriptors_path, tny)
        local table_descriptor = extractor.get_table_descriptor(lakefs, repo, commit_id, table_src_path)
        local table_name = table_descriptor.name
        if not table_name then
            error("table name is required to proceed with unity catalog export")
        end
        if table_descriptor.type ~= "delta" then
            error("unity exporter supports only table descriptors of type 'delta'. registration failed for table " .. table_name)
        end
        local catalog = table_descriptor.catalog
        if not catalog then
            error("catalog name is required to proceed with unity catalog export")
        end
        local get_schema_if_exists = true
        local schema_name = databricks_client.create_schema(branch_id, catalog, get_schema_if_exists)
        if not schema_name then
            error("failed creating/getting catalog's schema: " .. catalog .. "." .. branch_id)
        end
        local physical_path = table_details.path
        local table_metadata = table_details.metadata
        local status = databricks_client.register_external_table(table_name, physical_path, warehouse_id, catalog, schema_name, table_metadata)
        response[table_name_yaml] = status
    end
    return response
end


return {
    register_tables = register_tables,
}
