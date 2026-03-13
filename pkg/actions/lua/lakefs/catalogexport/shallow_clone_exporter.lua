--[[
    shallow_clone_exporter: exports Delta tables from lakeFS to an external location
    by running a Databricks shallow clone job, then returns the table locations in a
    format compatible with unity_exporter.register_tables.

    Instead of reading and rewriting the Delta log in Go (delta-go), this module
    delegates to Databricks' own DeltaTable.clone() implementation, which correctly
    handles all Delta protocol features (deletion vectors, variants, liquid clustering,
    etc.) without any Go-side protocol maintenance.

    Architecture:
      lakeFS commit (pinned)  →  Databricks notebook (DeltaTable.clone)  →  external location
                                         ↑
                               cluster_id + notebook_path (one-time setup by user)

    Source path format: s3a://{repo}/{commit_id}/{table_path}
      - Uses lakeFS S3 gateway with per-bucket Hadoop config on the Databricks cluster
      - commit_id pins the read to an immutable snapshot

    Target path format: {target_base_location}/{branch_id}/{table_name}
      - Written to a writable external location (ADLS, S3, GCS)
      - Unity Catalog can register this as an external table
]]

local lakefs = require("lakefs")
local pathlib = require("path")
local strings = require("strings")
local extractor = require("lakefs/catalogexport/table_extractor")

local function get_table_descriptor(repo, ref, table_name_yaml, table_descriptors_path)
    local tny = table_name_yaml
    if not strings.has_suffix(tny, ".yaml") then
        tny = tny .. ".yaml"
    end
    local table_src_path = pathlib.join("/", table_descriptors_path, tny)
    return extractor.get_table_descriptor(lakefs, repo, ref, table_src_path)
end

--[[
    clone_tables: shallow-clones a list of Delta tables from lakeFS to an external
    location using a Databricks notebook job.

    Parameters:
      action                - the hook action object (repository_id, commit_id, branch_id)
      table_def_names       - list of table descriptor yaml names, e.g. {"my_table.yaml", "other"}
      databricks_client     - a databricks client (from databricks.client())
      cluster_id            - Databricks cluster ID to run the notebook on
      notebook_path         - absolute Databricks workspace path to the shallow clone notebook
                              The notebook must accept "source_path" and "target_path" as
                              base parameters and run DeltaTable.clone().
      target_base_location  - writable external storage base, e.g.
                              "abfss://container@account.dfs.core.windows.net/lakefs-exports"
      table_descriptors_path - path prefix for table YAML descriptors, e.g. "_lakefs_tables"

    Returns a map compatible with unity_exporter.register_tables:
      { <table_name_yaml>: { path = <target_path>, metadata = nil }, ... }

    The caller is responsible for ensuring the Databricks cluster has per-bucket S3A config
    pointing at the lakeFS S3 gateway, e.g.:
      spark.hadoop.fs.s3a.bucket.<repo>.endpoint           = https://<lakefs-host>
      spark.hadoop.fs.s3a.bucket.<repo>.access.key         = <access-key>
      spark.hadoop.fs.s3a.bucket.<repo>.secret.key         = <secret-key>
      spark.hadoop.fs.s3a.bucket.<repo>.path.style.access  = true
]]
local function clone_tables(action, table_def_names, databricks_client, cluster_id,
                             notebook_path, target_base_location, table_descriptors_path)
    local repo      = action.repository_id
    local commit_id = action.commit_id
    if not commit_id then
        error("missing commit id")
    end
    local branch_id = action.branch_id

    local response = {}
    for _, table_name_yaml in ipairs(table_def_names) do
        local table_descriptor = get_table_descriptor(repo, commit_id, table_name_yaml, table_descriptors_path)

        local table_path = table_descriptor.path
        if not table_path then
            error("table path is required in descriptor for shallow clone export")
        end
        local table_name = table_descriptor.name
        if not table_name then
            error("table name is required in descriptor for shallow clone export")
        end

        -- Source: lakeFS S3 gateway, pinned to the immutable commit SHA.
        -- Using commit_id (not branch) guarantees we clone the exact snapshot that
        -- triggered this hook, even if the branch moves during the job run.
        local source_path = "s3a://" .. repo .. "/" .. commit_id .. "/" .. table_path

        -- Target: external storage location keyed by branch + table name.
        -- Branch-keyed so Unity Catalog sees one stable table per branch.
        local target_path = target_base_location .. "/" .. branch_id .. "/" .. table_name

        -- Submit the Databricks job and wait for completion.
        -- ShallowCloneTable returns the result state string on success,
        -- or raises a Lua error on failure.
        local _status = databricks_client.shallow_clone_table(
            cluster_id, notebook_path, source_path, target_path)

        response[table_name_yaml] = {
            path     = target_path,
            metadata = nil,
        }
    end
    return response
end

return {
    clone_tables = clone_tables,
}
