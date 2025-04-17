--[[
 As an exhaustive example, it will first start off with a Delta Lake tables export, then continue to register the table
 with Unity Catalog
]]

local aws = require("aws")
local formats = require("formats")
local databricks = require("databricks")
local delta_export = require("lakefs/catalogexport/delta_exporter")
local unity_export = require("lakefs/catalogexport/unity_exporter")

local sc = aws.s3_client(args.aws.access_key_id, args.aws.secret_access_key, args.aws.region)

NS EXAMPLE HERE
-- Export Delta Lake tables export:
local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key, args.aws.region)
local delta_table_locations = delta_export.export_delta_log(action, args.table_defs, sc.put_object, delta_client, "_lakefs_tables")

-- Register the exported table in Unity Catalog:
local databricks_client = databricks.client(args.databricks_host, args.databricks_token)
local registration_statuses = unity_export.register_tables(action, "_lakefs_tables", delta_table_locations, databricks_client, args.warehouse_id)

for t, status in pairs(registration_statuses) do
    print("Unity catalog registration for table \"" .. t .. "\" completed with status: " .. status .. "\n")
end
