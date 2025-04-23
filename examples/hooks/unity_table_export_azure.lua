--[[
 As an exhaustive example, it will first start off with a Delta Lake tables export, then continue to register the table
 with Unity Catalog
]]

local azure = require("azure")
local formats = require("formats")
local databricks = require("databricks")
local delta_exporter = require("lakefs/catalogexport/delta_exporter")
local unity_exporter = require("lakefs/catalogexport/unity_exporter")

local table_descriptors_path = "_lakefs_tables"
local sc = azure.blob_client(args.azure.storage_account, args.azure.access_key)
local function write_object(_, key, buf)
    return sc.put_object(key,buf)
end

--find the tables that changed
local ref = action.commit.parents[1]
local compare_ref = action.commit_id
local changed_table_defs = delta_exporter.changed_table_defs(args.table_defs, args.table_descriptors_path, action.repository_id, ref, compare_ref)

-- Export Delta Lake tables export:
local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key)
local delta_table_details = delta_exporter.export_delta_log(action, changed_table_defs, write_object, delta_client, table_descriptors_path, azure.abfss_transform_path)

-- Register the exported table in Unity Catalog:
local databricks_client = databricks.client(args.databricks_host, args.databricks_token)
local registration_statuses = unity_exporter.register_tables(action, "_lakefs_tables", delta_table_details, databricks_client, args.warehouse_id)
for t, status in pairs(registration_statuses) do
    print("Unity catalog registration for table \"" .. t .. "\" completed with status: " .. status .. "\n")
end
