---
layout: default
title: dbt
description: This guide covers maintaining environments with dbt and lakeFS.
parent: Integrations
nav_order: 110
has_children: false
redirect_from: /using/dbt.html
---

## Maintaining environments with dbt and lakeFS

dbt can run on lakeFS with a Spark adapter or Presto/Trino adapter. 
Both Spark and Presto use Hive metastore or Glue to manage tables and views.
When creating a branch in lakeFS, you receive a logical copy of the data that can be accessed by `s3://my-repo/branch/...` 
To run a dbt project on a newly created branch, you need to have a copy of the metadata as well.

The lakectl dbt command generates all the metadata needed in order to work on the newly created branch,
continuing from the last state in the source branch.
The dbt lakectl command does this using dbt commands and lakectl metastore commands.

{% include toc.html %}

## Configuration 

To run the lakectl-dbt commands you need to configure both dbt and lakectl. 
Assuming dbt is already configured, using either a Spark or Presto/Trino target 
you'll need to add configurations to give lakeFS access to your catalog (metastore).
This is done by adding the following configurations to the lakectl configuration file (by default `~/.lakectl.yaml`)

### Hive metastore

```yaml
metastore:
  type: hive
  hive:
    uri: hive-metastore:9083
```

### Glue

```yaml
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
 
## Views

lakectl copies all the models materialized as tables and incremental directly on your metastore.
However, copying views should be done manually or with lakectl.

### Using lakectl

The `generate_schema_name` macro could be used by lakectl to create models using dbt on a dynamic schema.
The following command will add a macro to your project, allowing lakectl to run dbt on the destination schema using an environment variable.

```bash
lakectl dbt generate-schema-macro
```

### Manual configuration

If you don't want to add the `generate_schema_name` macro to your project,
you can create the views on the destination schema manually.

For every run:
- use the `--skip-views` flag,
- change the default schema to be the branch schema in your dbt configuration file,
- run dbt on all views.

```bash
dbt run --select config.materialized:view
```

## Create Schema

Creating the schema 
From your dbt project run:
```bash
lakectl dbt create-branch-schema --branch my-branch --to-schema my_branch   
```

You can find more advanced options [here](../reference/cli.html#lakectl-dbt-create-branch-schema).
 

