--[[
  unity_table_export_shallow_clone.lua
  ─────────────────────────────────────────────────────────────────────────────
  Hook: export Delta tables from lakeFS → external storage → Unity Catalog
  using Databricks SHALLOW CLONE instead of delta-go log rewriting.

  Why shallow clone instead of delta-go?
    delta-go must be updated for every new Delta protocol feature (deletion
    vectors, variants, liquid clustering, …). Delegating to Databricks' own
    DeltaTable.clone() gives correct protocol handling for free, permanently.

  Flow:
    1. Find which table descriptors changed in this commit (skip unchanged tables)
    2. For each changed table, submit a Databricks notebook job that runs
       DeltaTable.clone(source_lakeFS_path → target_external_path)
    3. Register each cloned table as an external table in Unity Catalog

  Prerequisites (one-time setup):
    a. A Databricks cluster with per-bucket S3A config pointing at lakeFS:
         spark.hadoop.fs.s3a.bucket.<repo>.endpoint           = https://<lakefs-host>
         spark.hadoop.fs.s3a.bucket.<repo>.access.key         = <lakefs-access-key>
         spark.hadoop.fs.s3a.bucket.<repo>.secret.key         = <lakefs-secret-key>
         spark.hadoop.fs.s3a.bucket.<repo>.path.style.access  = true
    b. A Databricks notebook at <args.databricks.notebook_path> containing:
         from delta.tables import DeltaTable
         source = dbutils.widgets.get("source_path")
         target = dbutils.widgets.get("target_path")
         DeltaTable.forPath(spark, source).clone(
             target=target, isShallow=True, replace=True)

  args (passed via the hook configuration):
    args.table_defs                - list of table descriptor yaml names
    args.table_descriptors_path    - prefix for .yaml descriptors (e.g. "_lakefs_tables")
    args.target_base_location      - writable external storage base URI
                                     e.g. "abfss://container@account.dfs.core.windows.net/lakefs-exports"
    args.databricks_host           - Databricks workspace URL
    args.databricks_token          - Databricks personal access token
    args.cluster_id                - cluster ID to run the clone notebook
    args.notebook_path             - workspace path to the shallow clone notebook
    args.warehouse_id              - SQL warehouse ID for Unity Catalog registration
]]

local databricks   = require("databricks")
local delta_export = require("lakefs/catalogexport/delta_exporter")
local shallow_clone = require("lakefs/catalogexport/shallow_clone_exporter")
local unity_export = require("lakefs/catalogexport/unity_exporter")

-- ── 1. Find changed tables ────────────────────────────────────────────────────
local ref         = action.commit.parents[1]
local compare_ref = action.commit_id

local changed_table_defs = delta_export.changed_table_defs(
    args.table_defs,
    args.table_descriptors_path,
    action.repository_id,
    ref,
    compare_ref)

if #changed_table_defs == 0 then
    print("No table descriptors changed in this commit — nothing to export.")
    return
end

-- ── 2. Shallow clone each changed table to external storage ───────────────────
local databricks_client = databricks.client(args.databricks_host, args.databricks_token)

local cloned_table_locations = shallow_clone.clone_tables(
    action,
    changed_table_defs,
    databricks_client,
    args.cluster_id,
    args.notebook_path,
    args.target_base_location,
    args.table_descriptors_path)

-- ── 3. Register cloned tables in Unity Catalog ────────────────────────────────
local registration_statuses = unity_export.register_tables(
    action,
    args.table_descriptors_path,
    cloned_table_locations,
    databricks_client,
    args.warehouse_id)

for t, status in pairs(registration_statuses) do
    print(string.format(
        "Unity Catalog registration for table \"%s\" completed with status: %s",
        t, status))
end
