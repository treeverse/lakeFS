# Unity Table Export via Shallow Clone

Export Delta tables from lakeFS to Unity Catalog using Databricks `DeltaTable.clone()` instead of delta-go log rewriting.

## Why shallow clone instead of delta-go?

The delta-go approach requires updating the Go library for every new Delta protocol feature (deletion vectors, variants, liquid clustering, …). Delegating to Databricks' own `DeltaTable.clone()` implementation handles all current and future protocol versions automatically, with no Go-side maintenance.

## How it works

On every lakeFS commit, the hook:
1. Finds which table descriptors changed (skips unchanged tables)
2. Submits a Databricks notebook job that shallow-clones each table from the lakeFS S3 gateway to an external storage location
3. Registers the cloned table as an external table in Unity Catalog

The source path is pinned to the **immutable commit SHA** (`s3a://{repo}/{commit_id}/{table_path}`), so the clone always captures the exact snapshot that triggered the hook, even if the branch moves during the job run.

The target path is keyed by branch: `{target_base_location}/{branch_id}/{table_name}`, so each branch has its own stable table in Unity Catalog.

## One-time Setup

### 1. Cluster Spark Config

Add per-bucket S3A config to your Databricks cluster (**Edit cluster → Advanced options → Spark → Spark config**):

```
spark.hadoop.fs.s3a.bucket.<REPO_NAME>.endpoint           https://<lakefs-host>
spark.hadoop.fs.s3a.bucket.<REPO_NAME>.access.key         <lakefs-access-key>
spark.hadoop.fs.s3a.bucket.<REPO_NAME>.secret.key         <lakefs-secret-key>
spark.hadoop.fs.s3a.bucket.<REPO_NAME>.path.style.access  true
```

Replace `<REPO_NAME>` with your lakeFS repository name (the S3 "bucket" in the S3 gateway URL).

**Why per-bucket, not global?**
`fs.s3a.bucket.<name>.*` applies only to `s3a://<REPO_NAME>/...` traffic. Setting the global `fs.s3a.endpoint` would reroute all S3A traffic — including Databricks' own Unity Catalog storage — through lakeFS, causing 403 errors.

Add one block per lakeFS repo you want to read from.

### 2. Shallow Clone Notebook

Create a notebook in your Databricks workspace (e.g. `/Shared/lakefs/shallow_clone`) with two cells:

**Cell 1 — read parameters:**
```python
source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")
```

**Cell 2 — clone:**
```python
from delta.tables import DeltaTable

DeltaTable.forPath(spark, source_path).clone(
    target=target_path,
    isShallow=True,
    replace=True,
)
```

The hook passes `source_path` and `target_path` as `BaseParameters` via the Databricks Jobs API, which maps them to `dbutils.widgets.get()`.

`replace=True` ensures that re-running the hook on the same branch after a new commit overwrites the previous clone rather than failing.

> **Note:** The cluster does not need to be running at all times. The Jobs API will start a stopped cluster automatically (expect a ~2–5 minute cold-start delay). For hooks that run on every commit, an always-on or auto-scaling cluster minimises latency.

### 3. Hook Configuration

Reference the hook in your lakeFS action YAML:

```yaml
name: unity-catalog-export-shallow-clone
on:
  branches:
    - main
hooks:
  - id: unity_export_shallow_clone
    type: lua
    properties:
      script_path: examples/hooks/unity_table_export_shallow_clone.lua
      args:
        table_defs:
          - my_table.yaml
        table_descriptors_path: _lakefs_tables
        target_base_location: "abfss://container@account.dfs.core.windows.net/lakefs-exports"
        databricks:
          host: "https://your-workspace.azuredatabricks.net"
          token: "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
          cluster_id: "0123-456789-abcdefgh"
          notebook_path: "/Shared/lakefs/shallow_clone"
          warehouse_id: "abcdef1234567890"
```

## Files

| File | Purpose |
|------|---------|
| `unity_table_export_shallow_clone.lua` | The hook entry point |
| `pkg/actions/lua/lakefs/catalogexport/shallow_clone_exporter.lua` | Core clone logic; returns `{path, metadata}` per table |
| `pkg/actions/lua/lakefs/catalogexport/unity_exporter.lua` | Unity Catalog registration (unchanged, reused) |
| `pkg/actions/lua/databricks/client.go` | Go binding: `shallow_clone_table` → Jobs API submit + wait |

## Comparison with delta-go export

| | delta-go (`unity_table_export.lua`) | Shallow clone (this hook) |
|---|---|---|
| Delta protocol support | Manual — must update delta-go per feature | Automatic — Databricks handles all versions |
| Deletion vectors | Not supported in delta-go `AddFile` | Supported (proven via MVP test) |
| Requires S3 bucket write access | Yes (writes delta log files) | No (Databricks writes the clone) |
| Requires Databricks cluster | No | Yes |
| Requires notebook | No | Yes (one-time setup) |
| Export latency | Fast (log rewrite only) | Slower (job submit + cluster start + clone) |
