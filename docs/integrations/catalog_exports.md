---
title: Catalog Export Hooks
description: This section explains how lakeFS can integrate with external Data Catalogs via metastore update operations. 
parent: Integrations
redirect_from: /using/catalog_exports.html
---

# Data Catalog Exports

{% include toc_2-3.html %}

## About Data Catalog Exports

Data Catalog Export is all about integrating external Data Warehouses (i.e AWS Athena) with lakeFS.

Data Catalogs such as Hive, Glue and other store metadata for services (such as Spark, Trino and Athena). They contain metadata such as the location of the table, information about columns, partitions and much more.
Given the native integration between Spark and SQL, it’s most common that you’ll interact with tables in Spark environment.

While different systems support reading directly from lakefs (i.e Gateway endpoint `s3a://`) many systems only support direct access to blockstore (i.e S3 bucket). 

With Data Catalog Exports, one can leverage the versioning capabilities of lakeFS in external data warehouses and manage tables with branches and commits. 

Once hooks are set up, querying lakeFS data from e.g. Athena, Trino and other catalog-dependant tools looks like this:

```sql
use main;
use my_branch; -- any branch
use v101; -- or tag

SELECT * FROM users 
INNER JOIN events 
ON users.id = events.user_id; -- SQL stays the same, branch or tag exist as schema
```

## How it works 

There are several well known formats that exist today that could help export existing tables in lakeFS into a "native" object store representation
which don't require copying the data outside of lakeFS.

Since these are metadata representations, they are applied using hooks to automate the process.

### Table Decleration 

Once a a lakeFS repository is created, the tables should be configured as a table descriptor object on the repository on the path `_lakefs_tables/TABLE.yaml`.
Note: only tables of `type: hive` are supported and more are expected to be added. 

#### Hive tables

Hive metadata server tables are essentially just a set of objects that share a prefix, with no table metadata stored on the object store.  You need to configure prefix, partitions, and schema.
{: .note }

```yaml
name: animals
type: hive
path: path/to/animals/
partition_columns: ['year']
schema:
  type: struct
  fields:
    - name: year
      type: integer
      nullable: false
      metadata: {}
    - name: page
      type: string
      nullable: false
      metadata: {}
    - name: site
      type: string
      nullable: true
      metadata:
        comment: a comment about this column
```

Note: Useful types recognized by DataBricks Photon include `integer`, `long`, `short`, `string`, `double`, `float`, `date`, and `timestamp`.
{: .note }

### Catalog Exporters 

Exporters are code packages written in lua, each exporter is exposed as a lua function under the package namespace `lakefs/catalogexport`, they are reusable hooks to connect various types of tables to different catalogs.
[configuration]({% link reference/configuration.md %})
Note: Check the [lua Library reference]({% link howto/hooks/lua.md %}#lua-library-reference) library for code reference, specifically everything under the prefix `lakefs/catalogexport`. 
{: .note }

#### Currently supported Exporters: 

- Symlink Exporter: Writes metadata for the table using Hive's [SymlinkTextInputFormat](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html)
- AWS Glue Catalog (+ Athena) Exporter: Creates a table in Glue using Hive's format and updates the location to symlink files (reuses Symlink Exporter).

#### Running an Exporter  

Exporters are meant to run as [Lua hooks]({% link howto/hooks/lua.md %}).
                                                                                         
Actions trigger can be configured with [events and branches]({% link howto/hooks/index.md %}#action-file-schema), incase custom filtering logic is required it can be achieved in the Lua script itself.
The default table name when exported is `${repository_id}_${_lakefs_tables/TABLE.md(name field)}_${ref_name}_${short_commit}`.

Example of an action that will be triggered when a `post-commit` event happens in the `export_table` branch.

```yaml
name: Glue Table Exporter
description: export my table to glue  
on:
  post-commit:
    branches: ["export_table"]
hooks:
  - id: my_exporter
    type: lua
    properties:
      # exporter script location
      script_path: "scripts/my_export_script.lua"
      args:
        # table descriptor
        table_source: '_lakefs_tables/my_table.yaml'
```

Tip: Actions can be extended to customize any desired behavior, for example validating branch names since they are part of the table name: 

```yaml
# _lakefs_actions/validate_branch_name.yaml
name: validate-lower-case-branches 
on:
  pre-create-branch:
hooks:
  - id: check_branch_id
    type: lua
    properties:
      script: |
        regexp = require("regexp")
        if not regexp.match("^[a-z0-9\\_\\-]+$", action.branch_id) then
          error("branches must be lower case, invalid branch ID: " .. action.branch_id)
        end
```

### Flow

The following diagram demonstrates what happens when a lakeFS Action triggers runs a lua hook that calls an exporter.

```mermaid 
sequenceDiagram
    note over Lua Hook: lakeFS Action trigger. <br> Pass Context for the export.
    Lua Hook->>Exporter: export request
    note over Table Registry: _lakefs_tables/TABLE.yaml
    Exporter->>Table Registry: Get table descriptor
    Table Registry->>Exporter: Parse table structure
    Exporter->>Object Store: materialize an exported table
    Exporter->>Catalog: register object store location
    Query Engine-->Catalog: Query
    Query Engine-->Object Store: Query
```