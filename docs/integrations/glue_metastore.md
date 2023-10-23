---
title: Glue / Athena support
description: This section explains how to query data from lakeFS branches in services backed by Glue Metastore in particular Athena.
parent: Integrations
redirect_from: /using/glue_metastore.html
---


# Using lakeFS with the Glue Metastore


{% include toc_2-3.html %}

## About Glue Metastore 

This part explains about how Glue Metastore work with lakeFS.

[AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html) has a metastore that can store metadata related to Hive and other services (such as Spark and Trino). It has metadata such as the location of the table, information about columns, partitions and many more.


## Support in lakeFS

The integration between Glue and lakeFS is based on [Data Catalog Exports](({% link integrations/catalog_exports.md %})).

### What is supported 

- Creating a uniqueue table in Glue Catalog per lakeFS repository / ref / commit. 
- No data copying is required, the table location is a path to a symlinks structure in S3 based on Hive's [SymlinkTextInputFormat](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html) and the [table partitions](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html#tables-partition) are maintained.
- Tables are described via [Hive format in _lakefs_tables/<my_table>.yaml](({% link integrations/catalog_exports.md %}#hive-tables))
- Currently the data query in Glue metastore is Read-Only operation and mutating data requires writting to lakeFS and letting the export hook run.

### How it works 

Based on event such post-commit an Action will run a lua package that will create Symlink structures in S3 and then will register a table in Glue.   

There are 4 key pieces:

1. Table description at `_lakefs_tables/<your-table>.yaml`
2. Lua script that will do the export using [symlink_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportsymlink_exporter) and [glue_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportglue_exporter) packages.
3. [Action Lua Hook](({% link integrations/catalog_exports.md %}#eunning-an-exporter)) to execute the lua hook. 
4. Write some lakeFS table data (i.e [Spark]({% link integrations/spark.md %}) or some CSV)

To read more check [Data Catalog Exports](({% link integrations/catalog_exports.md %})).

## Example: Using Athena to query lakeFS data

### Pre-requisite 

1. Glue Database to use (lakeFS does not create a database).
2. AWS Credentials with permission to manage Glue, Athena Query and S3 access.
3. lakeFS [Actions]({% link howto/hooks/index.md %}) enabled and configured with S3 blockstore.

### Create lakeFS repository 

Let's create a repository named `catalogs`. 

```bash
lakectl repo create lakefs://catalogs s3://<lakefs-bucket>/<repo-prefix>
```
### Add table descriptor

Let's define a table and commit to lakeFS. 
Save the YAML below as `animals.yaml` and upload it to lakeFS. 

```bash
lakectl fs upload lakefs://catalogs/main/_lakefs_tables/animals.yaml -s ./animals.yaml
lakectl commit lakefs://lua/main -m "added table"
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



```

```