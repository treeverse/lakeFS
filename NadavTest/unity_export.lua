--[[
 As an exhaustive example, it will first start off with a Delta Lake tables export, then continue to register the table
 with Unity Catalog
]]

--local aws = require("aws")
--local formats = require("formats")
--local databricks = require("databricks")
local delta_export = require("lakefs/catalogexport/delta_exporter")
--local unity_export = require("lakefs/catalogexport/unity_exporter")

--local sc = aws.s3_client(args.aws.access_key_id, args.aws.secret_access_key, args.aws.region)

print("args.table_defs")
local tab_defs = args.table_defs
for i = 1, #tab_defs do
    print(tab_defs[i])
end
print("args.table_descriptors_path ", args.table_descriptors_path)
print("action.action_name ", action.action_name)
print("action.branch_id ", action.branch_id)
print("action.commit_id ", action.commit_id)
print("action.merge_source ", action.merge_source)
print("action.repository_id ", action.repository_id)
print("action.source_ref ", action.source_ref)
print("action.commit.parents[1] ", action.commit.parents[1])
print("action.commit.parents[2] ", action.commit.parents[2])
local left_ref = action.commit.parents[1]
local right_ref = action.commit_id
--local right_ref = action.commit.parents[2]
local table_def_changes = delta_export.table_def_changes(args.table_defs, args.table_descriptors_path, action.repository_id, action.commit_id,  left_ref, right_ref)
print("table_def_changes")
for i = 1, #table_def_changes do
    print(table_def_changes[i])
end

-- Export Delta Lake tables export:
--local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key, args.aws.region)
--local delta_table_locations = delta_export.export_delta_log(action, args.table_defs, sc.put_object, delta_client, "_lakefs_tables")

-- Register the exported table in Unity Catalog:
--local databricks_client = databricks.client(args.databricks_host, args.databricks_token)
--local registration_statuses = unity_export.register_tables(action, "_lakefs_tables", delta_table_locations, databricks_client, args.warehouse_id)

--for t, status in pairs(registration_statuses) do
--    print("Unity catalog registration for table \"" .. t .. "\" completed with status: " .. status .. "\n")
--end
