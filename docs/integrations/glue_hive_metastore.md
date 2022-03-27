---
layout: default
title: Glue / Hive metastore
description: Query data from lakeFS branches in services backed by Glue/Hive Metastore.
parent: Integrations
nav_order: 65
has_children: false
redirect_from: ../using/glue_hive_metastore.html
---

{% include toc.html %}

# Glue / Hive Metastore Intro
This part contains a brief explanation about how Glue/Hive metastore work with lakeFS

Glue and Hive Metastore stores metadata related to Hive and other services (such as Spark and Trino).
They contain metadata such as the location of the table, information about columns, partitions and many more.

## Without lakeFS
{: .no_toc }
In order to query the table `my_table`, Spark will:
* Request the metadata from Hive metastore (steps 1,2)
* Use the location from the metadata to access the data in S3 (steps 3,4).
![metastore with S3]({{ site.baseurl }}/assets/img/metastore-S3.svg)

<br/><br/>

## With lakeFS
{: .no_toc }
When using lakeFS, the flow stays exactly the same. Note that the location of the table `my_table` now contains the branch `s3://example/main/path/to/table`
![metastore with S3]({{ site.baseurl }}/assets/img/metastore-lakefs.svg)


<br/><br/><br/>

# Managing Tables With lakeFS Branches
## Motivation
When creating a table in Glue/Hive metastore (using a client such as Spark, Hive, Presto), we specify the table location.
Consider the table `my_table` which was created with the location `s3://example/main/path/to/table`.

Assume we created a new branch called `DEV` with `main` as the source branch.
The data from `s3://example/main/path/to/table` is now accessible in `s3://example/DEV/path/to/table`.
The metadata is not managed in lakeFS, meaning we don't have any table pointing to `s3://example/DEV/path/to/table`.

To address this, lakeFS introduces `lakectl metastore` commands. The case above could be handled using the copy command: it can create a copy of `my_table` with data located in `s3://example/DEV/path/to/table`. Note that this is a fast, metadata-only operation.


## Configurations
The `lakectl metastore` commands could run on Glue or Hive metastore.<br/>
Add the following to the lakectl configuration file (by default `~/.lakectl.yaml`):

### Hive
{: .no_toc }

``` yaml
metastore:
  type: hive
  hive:
    uri: hive-metastore:9083
```

### Glue
{: .no_toc }

``` yaml
metastore:
  type: glue
  glue:
    catalog-id: 123456789012
    region: us-east-1
    profile: default # optional, implies using a credentials file
    credentials:
      access_key_id: AKIAIOSFODNN7EXAMPLE
      secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

**Notice:** It's recommended to set type and catalog-id/metastore-uri in the lakectl configuration file.
{: .note .pb-3 }

## Suggested Model

For simplicity, we recommend creating a schema for each branch, this way you can use the same table name across different schemas.

For example:
after creating branch `example_branch` also create a schema named `example_branch`.
For a table named `my_table` under the schema `main`, create a new table by the same name under the schema `example_branch`. You now have two `my_table`s, one in the main schema and one, in the branch schema.


## Commands

Metastore tools support three commands: `copy`, `diff` and `create-symlink`.
copy and diff could work both on Glue and on Hive.
create-symlink works only on Glue.


**Notice:** If `to-schema` or `to-table` are not specified, the destination branch and source table names will be used as per the [suggested model](#suggested-model){: .button-clickable}.
{: .note .pb-3 }

**Notice:** Metastore commands can only run on tables located in lakeFS, you should not use tables that are not located in lakeFS.
{: .note .pb-3 }

### Copy

The `copy` command creates a copy of a table pointing to the defined branch.
In case the destination table already exists, the command will only merge the changes.

Example:

Suppose we created the table `inventory` on branch `main` on schema `default`.
```sql
CREATE EXTERNAL TABLE `inventory`(
        `inv_item_sk` int,
        `inv_warehouse_sk` int,
        `inv_quantity_on_hand` int)
    PARTITIONED BY (
        `inv_date_sk` int) STORED AS ORC
    LOCATION
        's3a://my_repo/main/path/to/table';
```

We create a new lakeFS branch `example_branch`:

```shell
lakectl branch create lakefs://my_repo/example_branch --source lakefs://my_repo/main
```

The data from `s3://my_repo/main/path/to/table` is now accessible in `s3://my_repo/DEV/path/to/table`.
In order to query the data in `s3://my_repo/DEV/path/to/table`
we would like to create a copy of the table `inventory` in schema `example_branch` pointing to the new branch.

```bash
lakectl metastore copy --from-schema default --from-table inventory --to-schema example_branch --to-table inventory --to-branch example_branch
```

After running this command, query the table `example_branch.inventory` to get the data from `s3://my_repo/DEV/path/to/table`

#### Copy Partition
{: .no_toc }

After adding a partition to the branch table, we may want to copy the partition to the main table.
For example, for the new partition `2020-08-01`, run the following in order to copy the partition to the main table:

```bash
lakectl metastore copy --type hive --from-schema example_branch --from-table inventory --to-schema default --to-table inventory --to-branch main -p 2020-08-01
```

For a table partitioned by more than one column, specify the partition flag for every column. For example for the partition `(year='2020',month='08',day='01')`:

```bash
lakectl metastore copy --from-schema example_branch --from-table branch_inventory --to-schema default --to-branch main -p 2020 -p 08 -p 01
```

### Diff

Provides a 2-way diff between two tables.
Shows added`+` , removed`-` and changed`~` partitions and columns.


Example:

Suppose that we made some changes on the copied table `inventory` on schema `example_branch` and we want to view the changes before merging back to `inventory` on schema `default`.

Hive:
```bash
lakectl metastore diff --type hive --address thrift://hive-metastore:9083 --from-schema example_branch --from-table branch --to-schema default --to-table inventory
```

The output will be something like:

```
Columns are identical
Partitions
- 2020-07-04
+ 2020-07-05
+ 2020-07-06
~ 2020-07-08
```
