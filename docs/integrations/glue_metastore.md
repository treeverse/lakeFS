---
title: Glue Data Catalog
description: Query data from lakeFS branches in AWS Athena or other services backed by Glue Data Catalog.
redirect_from: /using/glue_metastore.html
---


# Using lakeFS with the Glue Catalog

{% include toc_2-3.html %}

## Overview

The integration between Glue and lakeFS is based on [Data Catalog Exports]({% link howto/catalog_exports.md %}).

This guide describes how to use lakeFS with the Glue Data Catalog.
You'll be able to query your lakeFS data by specifying the repository, branch and commit in your SQL query.
Currently, only read operations are supported on the tables.
You will set up the automation required to work with lakeFS on top of the Glue Data Catalog, including:
1. Create a table descriptor under `_lakefs_tables/<your-table>.yaml`. This will represent your table schema.
2. Write an exporter script that will:
   * Mirror your branch's state into [Hive Symlink](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html) files readable by Athena.
   * Export the table descriptors from your branch to the Glue Catalog.
3. Set up lakeFS [hooks]({% link howto/catalog_exports.md %}#running-an-exporter) to trigger the above script when specific events occur.
  
## Example: Using Athena to query lakeFS data

### Prerequisites

Before starting, make sure you have:
1. An active lakeFS installation with S3 as the backing storage, and a repository in this installation.
2. A database in Glue Data Catalog (lakeFS does not create one).
3. AWS Credentials with permission to manage Glue, Athena Query and S3 access.

### Add table descriptor

Let's define a table, and commit it to lakeFS. 
Save the YAML below as `animals.yaml` and upload it to lakeFS. 

```bash
lakectl fs upload lakefs://catalogs/main/_lakefs_tables/animals.yaml -s ./animals.yaml && \
lakectl commit lakefs://catalogs/main -m "added table"
```

```yaml 
name: animals
type: hive
# data location root in lakeFS
path: tables/animals
# partitions order
partition_columns: ['type', 'weight']
schema:
  type: struct
  # all the columns spec
  fields:
    - name: type
      type: string
      nullable: true
      metadata:
        comment: axolotl, cat, dog, fish etc
    - name: weight
      type: integer
      nullable: false
      metadata: {}
    - name: name
      type: string
      nullable: false
      metadata: {}
```

### Write some table data

Insert data into the table path, using your preferred method (e.g. [Spark]({% link integrations/spark.md %})), and commit upon completion.
This example uses CSV files, and the files added to lakeFS should look like this:

![lakeFS Uploaded CSV Files]({{ site.baseurl }}/assets/img/csv_export_hooks_data.png)

### The exporter script

Upload the following script to your main branch under `scripts/animals_exporter.lua` (or a path of your choice).

{: .note}
> For code references check [symlink_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportsymlink_exporter) and [glue_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportglue_exporter) docs.


```lua 
local aws = require("aws")
local symlink_exporter = require("lakefs/catalogexport/symlink_exporter")
local glue_exporter = require("lakefs/catalogexport/glue_exporter")
-- settings 
local access_key = args.aws.aws_access_key_id
local secret_key = args.aws.aws_secret_access_key
local region = args.aws.aws_region
local table_path = args.table_source -- table descriptor 
local db = args.catalog.db_name -- glue db
local table_input = args.catalog.table_input -- table input (AWS input spec) for Glue
-- export symlinks 
local s3 = aws.s3_client(access_key, secret_key, region)
local result = symlink_exporter.export_s3(s3, table_path, action, {debug=true})
-- register glue table
local glue = aws.glue_client(access_key, secret_key, region)
local res = glue_exporter.export_glue(glue, db, table_path, table_input, action, {debug=true})
```

### Configure Action Hooks

Hooks serve as the mechanism that triggers the execution of the exporter.
For more detailed information on how to configure exporter hooks, you can refer to [Running an Exporter]({% link howto/catalog_exports.md %}#running-an-exporter).

{: .note}
> The `args.catalog.table_input` argument in the Lua script is assumed to be passed from the action arguments, that way the same script can be reused for different tables. Check the [example]({% link howto/hooks/lua.md %}#lakefscatalogexportglue_exporterexport_glueglue-db-table_src_path-create_table_input-action_info-options) to construct the table input in the lua code.


<div class="tabs">
  <ul>
    <li><a href="#single-hook-csv">Hook CSV Glue Table</a></li>
    <li><a href="#single-hook">Hook Parquet Glue Table</a></li>
    <li><a href="#multiple-hooks">Multiple Hooks / Inline script</a></li>
  </ul> 
  <div markdown="1" id="single-hook-csv">

#### Single hook with CSV Table

Upload to `_lakefs_actions/animals_glue.yaml`: 

```yaml
name: Glue Exporter
on:
  post-commit:
    branches: ["main"]
hooks:
  - id: animals_table_glue_exporter
    type: lua
    properties:
      script_path: "scripts/animals_exporter.lua"
      args:
        aws:
          aws_access_key_id: "<AWS_ACCESS_KEY_ID>"
          aws_secret_access_key: "<AWS_SECRET_ACCESS_KEY>"
          aws_region: "<AWS_REGION>"
        table_source: '_lakefs_tables/animals.yaml'
        catalog:
          db_name: "my-glue-db"
          table_input:
            StorageDescriptor: 
              InputFormat: "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
              OutputFormat: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
              SerdeInfo:
                SerializationLibrary: "org.apache.hadoop.hive.serde2.OpenCSVSerde"
                Parameters:
                  separatorChar: ","
            Parameters: 
              classification: "csv"
              "skip.header.line.count": "1"
```

  </div>
  <div markdown="1" id="single-hook">
#### Spark Parquet Example

When working with Parquet files, upload the following to `_lakefs_actions/animals_glue.yaml`:

```yaml
name: Glue Exporter
on:
   post-commit:
      branches: ["main"]
hooks:
   - id: animals_table_glue_exporter
     type: lua
     properties:
        script_path: "scripts/animals_exporter.lua"
        args:
           aws:
              aws_access_key_id: "<AWS_ACCESS_KEY_ID>"
              aws_secret_access_key: "<AWS_SECRET_ACCESS_KEY>"
              aws_region: "<AWS_REGION>"
           table_source: '_lakefs_tables/animals.yaml'
           catalog:
              db_name: "my-glue-db"
              table_input:
                 StorageDescriptor:
                   InputFormat: "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
                   OutputFormat: "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
                   SerdeInfo:
                       SerializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                 Parameters:
                   classification: "parquet"
                   EXTERNAL: "TRUE"
                   "parquet.compression": "SNAPPY"
```

  </div>
  <div markdown="1" id="multiple-hooks">
#### Multiple Hooks / Inline script

The following example demonstrates how to separate the symlink and glue exporter into building blocks running in separate hooks.
It also shows how to run the lua script inline instead of a file, depending on user preference.

```yaml
name: Animal Table Exporter
on:
  post-commit:
    branches: ["main"]
hooks:
  - id: symlink_exporter
    type: lua
    properties:
      args:
        aws:
          aws_access_key_id: "<AWS_ACCESS_KEY_ID>"
          aws_secret_access_key: "<AWS_SECRET_ACCESS_KEY>"
          aws_region: "<AWS_REGION>"
        table_source: '_lakefs_tables/animals.yaml'
      script: |
        local exporter = require("lakefs/catalogexport/symlink_exporter")
        local aws = require("aws")
        local table_path = args.table_source
        local s3 = aws.s3_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
        exporter.export_s3(s3, table_path, action, {debug=true})
  - id: glue_exporter
    type: lua
    properties:
      args:
        aws:
          aws_access_key_id: "<AWS_ACCESS_KEY_ID>"
          aws_secret_access_key: "<AWS_SECRET_ACCESS_KEY>"
          aws_region: "<AWS_REGION>"
        table_source: '_lakefs_tables/animals.yaml'
        catalog:
          db_name: "my-glue-db"
          table_input: # add glue table input here 
      script: |
        local aws = require("aws")
        local exporter = require("lakefs/catalogexport/glue_exporter")
        local glue = aws.glue_client(args.aws.aws_access_key_id, args.aws.aws_secret_access_key, args.aws.aws_region)
        exporter.export_glue(glue, args.catalog.db_name, args.table_source, args.catalog.table_input, action, {debug=true})  
```

  </div>
</div>

Adding the script and the action files to the repository and commit it. This is a post-commit action, meaning it will be executed after the commit operation has taken place. 

```bash
lakectl fs upload lakefs://catalogs/main/scripts/animals_exporter.lua -s ./animals_exporter.lua
lakectl fs upload lakefs://catalogs/main/_lakefs_actions/animals_glue.yaml -s ./animals_glue.yaml
lakectl commit lakefs://catalogs/main -m "trigger first export hook"
```

Once the action has completed its execution, you can review the results in the action logs.

![Hooks log result in lakeFS UI]({{ site.baseurl }}/assets/img/glue_export_hook_result_log.png)

### Use Athena 

We can use the exported Glue table with any tool that supports Glue Catalog (or Hive compatible) such as Athena, Trino, Spark and others.
To use Athena we can simply run `MSCK REPAIR TABLE` and then query the tables.

In Athena, make sure that the correct database (`my-glue-db` in the example above) is configured, then run: 

```sql
MSCK REPAIR TABLE `animals_catalogs_main_9255e5`; -- load partitions for the first time 
SELECT * FROM `animals_catalogs_main_9255e5` limit 50;
```

![Athena SQL Result]({{ site.baseurl }}/assets/img/catalog_export_athena_aws_ui_sql.png)

### Cleanup

Users can use additional hooks / actions to implement a custom cleanup logic to delete the symlink in S3 and Glue Tables. 

```lua
glue.delete_table(db, '<glue table name>')
s3.delete_recursive('bucket', 'path/to/symlinks/of/a/commit/')
```
