---
title: Unity Catalog
description: Accessing lakeFS-exported Delta Lake tables from Unity Catalog.
parent: Integrations
redirect_from: /using/unity_catalog
redirect_from: /integrations/unity_catalog
---

# Using lakeFS with the Unity Catalog

{% include toc_2-3.html %}

## Overview

Databricks Unity Catalog serves as a centralized data governance platform for your data lakes.
Through the Unity Catalog, you can search for and locate data assets across workspaces via a unified catalog.
Leveraging the external tables feature within Unity Catalog, you can register a Delta Lake table exported from lakeFS and
access it through the unified catalog. 
The subsequent step-by-step guide will lead you through the process of configuring a [Lua hook]({% link howto/hooks/lua.md %})
that exports Delta Lake tables from lakeFS, and subsequently registers them in Unity Catalog.

{: .note}
> Currently, Unity Catalog export feature exclusively supports AWS S3 as the underlying storage solution. It's planned to [support other cloud providers soon](https://github.com/treeverse/lakeFS/issues/7199).

## Prerequisites

Before starting, ensure you have the following:

1. Access to Unity Catalog
2. An active lakeFS installation with S3 as the backing storage, and a repository in this installation.
3. A Databricks SQL warehouse.
4. AWS Credentials with S3 access.
5. lakeFS credentials with access to your Delta Tables.

{: .note}
> Supported from lakeFS v1.4.0

### Databricks authentication

Given that the hook will ultimately register a table in Unity Catalog, authentication with Databricks is imperative.
Make sure that:

1. You have a Databricks [Service Principal](https://docs.databricks.com/en/dev-tools/service-principals.html).
2. The Service principal has [token usage permissions](https://docs.databricks.com/en/dev-tools/service-principals.html#step-3-assign-workspace-level-permissions-to-the-databricks-service-principal),
   and an associated [token](https://docs.databricks.com/en/dev-tools/service-principals.html#step-4-generate-a-databricks-personal-access-token-for-the-databricks-service-principal)
   configured.
3. The service principal has the `Service principal: Manager` privilege over itself (Workspace: Admin console -> Service principals -> `<service principal>` -> Permissions -> Grant access (`<service principal>`:
   `Service principal: Manager`), with `Workspace access` and `Databricks SQL access` checked (Admin console -> Service principals -> `<service principal>` -> Configurations).
4. Your SQL warehouse allows the service principal to use it (SQL Warehouses -> `<SQL warehouse>` -> Permissions -> `<service principal>`: `Can use`).
5. The catalog grants the `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA` permissions to the service principal(Catalog -> `<catalog name>` -> Permissions -> Grant -> `<service principal>`: `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA`).
6. You have an _External Location_ configured, and the service principal has the `CREATE EXTERNAL TABLE` permission over it (Catalog -> External Data -> External Locations -> Create location).

## Guide

### Table descriptor definition

To guide the Unity Catalog exporter in configuring the table in the catalog, define its properties in the Delta Lake table descriptor. 
The table descriptor should include (at minimum) the following fields:
1. `name`: The table name.
2. `type`: Should be `delta`.
3. `catalog`: The name of the catalog in which the table will be created.
4. `path`: The path in lakeFS (starting from the root of the branch) in which the Delta Lake table's data is found.

Let's define the table descriptor and upload it to lakeFS:

Save the following as `famous-people-td.yaml`:

```yaml
---
name: famous_people
type: delta
catalog: my-catalog-name
path: tables/famous-people
```

{: .note}
> It's recommended to create a Unity catalog with the same name as your repository

Upload the table descriptor to `_lakefs_tables/famous-people-td.yaml` and commit:

```bash
lakectl fs upload lakefs://repo/main/_lakefs_tables/famous-people-td.yaml -s ./famous-people-td.yaml && \
lakectl commit lakefs://repo/main -m "add famous people table descriptor"
```

### Write some data

Insert data into the table path, using your preferred method (e.g. [Spark]({% link integrations/spark.md %})), and commit upon completion.

We shall use Spark and lakeFS's S3 gateway to write some data as a Delta table:
```bash
pyspark --packages "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider='org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider' \
  --conf spark.hadoop.fs.s3a.endpoint='<LAKEFS_SERVER_URL>' \
  --conf spark.hadoop.fs.s3a.access.key='<LAKEFS_ACCESS_KEY>' \
  --conf spark.hadoop.fs.s3a.secret.key='<LAKEFS_SECRET_ACCESS_KEY>' \
  --conf spark.hadoop.fs.s3a.path.style.access=true
```

```python
data = [
   ('James','Bond','England','intelligence'),
   ('Robbie','Williams','England','music'),
   ('Hulk','Hogan','USA','entertainment'),
   ('Mister','T','USA','entertainment'),
   ('Rafael','Nadal','Spain','professional athlete'),
   ('Paul','Haver','Belgium','music'),
]
columns = ["firstname","lastname","country","category"]
df = spark.createDataFrame(data=data, schema = columns)
df.write.format("delta").mode("overwrite").partitionBy("category", "country").save("s3a://repo/main/tables/famous-people")
```

### The Unity Catalog exporter script

{: .note}
> For code references check [delta_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportdelta_exporter) and 
[unity_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportunity_exporter) docs.

Create `unity_exporter.lua`:

```lua
local aws = require("aws")
local formats = require("formats")
local databricks = require("databricks")
local delta_export = require("lakefs/catalogexport/delta_exporter")
local unity_export = require("lakefs/catalogexport/unity_exporter")

local sc = aws.s3_client(args.aws.access_key_id, args.aws.secret_access_key, args.aws.region)

-- Export Delta Lake tables export:
local delta_client = formats.delta_client(args.lakefs.access_key_id, args.lakefs.secret_access_key, args.aws.region)
local delta_table_details = delta_export.export_delta_log(action, args.table_defs, sc.put_object, delta_client, "_lakefs_tables")

-- Register the exported table in Unity Catalog:
local databricks_client = databricks.client(args.databricks_host, args.databricks_token)
local registration_statuses = unity_export.register_tables(action, "_lakefs_tables", delta_table_details, databricks_client, args.warehouse_id)

for t, status in pairs(registration_statuses) do
    print("Unity catalog registration for table \"" .. t .. "\" completed with commit schema status : " .. status .. "\n")
end
```

Upload the lua script to the `main` branch under `scripts/unity_exporter.lua` and commit:

```bash
lakectl fs upload lakefs://repo/main/scripts/unity_exporter.lua -s ./unity_exporter.lua && \
lakectl commit lakefs://repo/main -m "upload unity exporter script"
```

### Action configuration

Define an action configuration that will run the above script after a commit is completed (`post-commit`) over the `main` branch.

Create `unity_exports_action.yaml`:

```yaml
---
name: unity_exports
on:
  post-commit:
     branches: ["main"]
hooks:
  - id: unity_export
    type: lua
    properties:
      script_path: scripts/unity_exporter.lua
      args:
        aws:
          access_key_id: <AWS_ACCESS_KEY_ID>
          secret_access_key: <AWS_SECRET_ACCESS_KEY>
          region: <AWS_REGION>
        lakefs: # provide credentials of a user that has access to the script and Delta Table
          access_key_id: <LAKEFS_ACCESS_KEY_ID> 
          secret_access_key: <LAKEFS_SECRET_ACCESS_KEY>
        table_defs: # an array of table descriptors used to be defined in Unity Catalog
          - famous-people-td
        databricks_host: <DATABRICKS_HOST_URL>
        databricks_token: <DATABRICKS_SERVICE_PRINCIPAL_TOKEN>
        warehouse_id: <WAREHOUSE_ID>
```

Upload the action configurations to `_lakefs_actions/unity_exports_action.yaml` and commit:

{: .note}
> Once the commit will finish its run, the action will start running since we've configured it to run on `post-commit` 
events on the `main` branch.

```bash
lakectl fs upload lakefs://repo/main/_lakefs_actions/unity_exports_action.yaml -s ./unity_exports_action.yaml && \
lakectl commit lakefs://repo/main -m "upload action and run it"
```

The action has run and exported the `famous_people` Delta Lake table to the repo's storage namespace, and has register 
the table as an external table in Unity Catalog under the catalog `my-catalog-name`, schema `main` (as the branch's name) and 
table name `famous_people`: `my-catalog-name.main.famous_people`.

![Hooks log result in lakeFS UI]({{ site.baseurl }}/assets/img/unity_export_hook_result_log.png)

### Databricks Integration

After registering the table in Unity, you can leverage your preferred method to [query the data](https://docs.databricks.com/en/query/index.html) 
from the exported table under `my-catalog-name.main.famous_people`, and view it in the Databricks's Catalog Explorer, or
retrieve it using the Databricks CLI with the following command: 
```bash
databricks tables get my-catalog-name.main.famous_people
```

![Unity Catalog Explorer view]({{ site.baseurl }}/assets/img/unity_exported_table_columns.png)
