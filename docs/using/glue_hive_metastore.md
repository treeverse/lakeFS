---
layout: default
title: Glue / Hive metastore 
parent: Using lakeFS with...
nav_order: 11
has_children: false
---

# Glue / Hive metastore 
{: .no_toc }
When working with Glue or Hive metastore, each table can point only to one lakeFS branch.
When creating a new branch, you may want to have a similar table pointing to the new branch.  
To support this, lakeFS comes with cli commands which enable copying metadata between branches.


## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}



# Access to metastores
The commands could run on Glue or Hive metastore, each of them have different parameters and different configurations.
## Hive
To run a command on Hive metastore we will need to:

Specify that the type is Hive ``` --type hive```

Insert the Hive metastore uri in the metastore-uri flag ``` --metastore-uri thrift://hive-metastore:9083```

It's recommended to configure these fields in the lakectl configuration file:

``` yaml
metastore:
  type: hive
  hive:
    uri: thrift://hive-metastore:9083
```



## Glue
To run a command on Glue metastore we will need to:

Specify that the type is Glue ``` --type glue```

Insert the catalog ID (aws numerical account-id) in the catalog-id flag ``` --catalog-id 123456789012```

Configure aws credentials for accessing Glue catalog:

In the lakectl configuration file add the credentials:

``` yaml
metastore:
  glue:
    region: us-east-1
    profile: default # optional, implies using a credentials file
    credentials:
      access_key_id: AKIAIOSFODNN7EXAMPLE
      secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

It is recommended to set the type and catalog-id in the configuration file:

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

# Suggested model:
For simplicity, we recommend creating a schema for each branch, this way you can use the same table name across different schemas.

For example:
after creating branch `example_branch` also create a schema named `example_branch`.
for a table named `example_table` under the schema `master` we would like to create a new table called `example_table` under the schema `example_branch`  
 

# Commands:
Metastore tools supports three commands: ```copy```, ```diff``` and ```create-symlink```.
copy and diff could work both on Glue and on Hive.
create-symlink works only on Glue.

**Notice:** It's recommended to set type and catalog-id/metastore-uri in the lakectl configuration file.
{: .note .pb-3 }
**Notice:** If `to-schema` or `to-table` are not specified, the destination branch and source table names will be used as per the [suggested model](#suggested-model).
{: .note .pb-3 }


## Copy
The `copy` command creates a copy of a table pointing to the defined branch.
In case the destination table already exists, the command will only merge the changes.

Example:

Suppose we have the table `example_by_dt` on branch `master` on schema `default`.
We create a new branch `exmpale_branch` .
we would like to create a copy of the table `example_by_dt` in schema `example_branch` pointing to the new branch.   

Recommended:
``` bash
lakectl metastore copy  --from-schema default --from-table exmpale_by_dt --to-branch example_branch 
```

Glue:
``` bash
lakectl metastore copy --type glue --address 123456789012 --from-schema default --from-table exmpale_by_dt --to-schema default --to-table branch_example_by_dt --to-branch example_branch 
```

Hive:
``` bash
lakectl metastore copy --type hive --address thrift://hive-metastore:9083 --from-schema default --from-table example_by_dt --to-schema default --to-table branch_example_by_dt --to-branch exmample-branch
```

### Copy partition
Copying a single partition is also supported.
we could specify the partition using the partition flag.

Example:

So no we have the copied table `example_by_dt` on schema `example_branch` and we've added a new partition `2020-08-01`.

Suppose we merged back the data to master, and we want the data to be available on our original table on master `example_by_dt` on schema `default`.

We would like to merge back the partition:  

Recommended:
``` bash
lakectl metastore copy --from-schema example_branch --from-table example_by_dt --to-schema default  --to-branch master -p 2020-08-01 
```

Glue:
``` bash
lakectl metastore copy --type glue --address 123456789012 --from-schema example_branch --from-table example_by_dt --to-schema default --to-table example_by_dt --to-branch master -p 2020-08-01
```

Hive:
``` bash
lakectl metastore copy --type hive --address thrift://hive-metastore:9083 --from-schema example_branch --from-table example_by_dt --to-schema default --to-table example_by_dt --to-branch master -p 2020-08-01
```

In case our table is partitioned by more than one value, for example partitioned by year/month/day for year ```2020``` month ```08``` day ```01``` 

``` bash
lakectl metastore copy --from-schema example_branch --from-table branch_example_by_dt --to-schema default --to-branch master -p 2020 -p 08 -p 01
```


## Diff
Provides a 2-way diff between two tables.
Shows added`+` , removed`-` and changed`~` partitions and columns.


Example:

Suppose that we made some changes on the copied table `exmample_by_dt` on schema `example_branch` and we want to see the changes before merging back to `example_by_dt` on schema `default`. 

Recommended:
``` bash
lakectl metastore diff --from-schema default --from-table branch_example_by_dt --to-schema example_branch 
```

Glue:
``` bash
lakectl metastore diff --type glue --address 123456789012 --from-schema default --from-table branch_example_by_dt --to-schema default --to-table example_by_dt
```

Hive:
``` bash
lakectl metastore diff --type hive --address thrift://hive-metastore:9083 --from-schema default --from-table branch_example_by_dt --to-schema default --to-table example_by_dt
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

## Create Symlink  
Some tools (such as Athena) don't support configuring the endpoint-uri.
meaning we can't use them above lakeFS, and can only use them above S3.
 
In order to enable accessing partitioned data we could use create-symlink.

create-symlink receives a source table, destination table and the location of the table and does two actions:
1. Creates partitioned directories with symlink files in the underlying s3 bucket.
2. Creates a table in glue catalog with symlink format type and location pointing to the created symlinks.


Example:

Let's assume we have the table `example_by_dt` in glue.
The table is pointing to repo `example-repo` branch `master` and the data is located at `path/to/table/in/lakeFS`

We want to query the table using Amazon Athena.

To do this, we run the command:
``` bash
lakectl metastore create-symlink --address 123456789012 --branch master --from-schema default --from-table branch_example_by_dt --to-schema default --to-table sym_example_by_dt --repo example-repository --path path/to/table/in/lakeFS
```

Now we can use  Amazon Athena and query the created table `sym_example_by_dt`
