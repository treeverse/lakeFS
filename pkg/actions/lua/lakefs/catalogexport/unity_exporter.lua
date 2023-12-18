--[[ TABLE SPECIFICATION:   _lakefs_tables/<table path>
name: <table name>
type: delta
catalog: <catalog name>
]]

local pathlib = require("path")
local lakefs = require("lakefs")
local extractor = require("lakefs/catalogexport/table_extractor")
--[[
    - table_descriptors_path: the path under which the table descriptors reside (e.g. "_lakefs_tables").
      It's necessary that every <table path> in the provided `table_paths` will have a complementary
      `<table_descriptors_path>/<table path>.yaml` file describing the used Delta Table.
    - delta_table_paths: a mapping of exported Delta table names to their locations in the object storage
        { <delta table name>: <physical location in the object storage> }
    - databricks_client: a client to interact with databricks.
    - warehouse_id: Databricks warehouse ID

    Returns a "<table name>: status" map for registration of provided tables.
]]
local function register_tables(action, table_descriptors_path, delta_table_paths, databricks_client, warehouse_id)
    local repo = action.repository_id
    local commit_id = action.commit_id
    local branch_id = action.branch_id
    local response = {}
    for table_name, physical_path in pairs(delta_table_paths) do
        local table_src_path = pathlib.join("/", table_descriptors_path, table_name .. ".yaml")
        local table_descriptor = extractor.get_table_descriptor(lakefs, repo, commit_id, table_src_path)
        if table_descriptor.type ~= "delta" then
            error("unity exporter supports only table descriptors of type 'delta'. registration failed for table " .. table_name)
        end
        local catalog = table_descriptor.catalog
        if not catalog then
            error("catalog name is required to proceed with unity catalog export")
        end
        local schema_name = databricks_client.create_or_get_schema(branch_id, catalog)
        local status = databricks_client.register_external_table(table_name, physical_path, warehouse_id, catalog, schema_name)
        response[table_name] = status
    end
    return response
end


return {
    register_tables = register_tables,
}
