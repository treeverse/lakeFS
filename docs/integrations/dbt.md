---
layout: default
title: DBT
description: Maintaining environments with DBT and lakeFS.
parent: Integrations
nav_order: 64
has_children: false
redirect_from: ../using/dbt.html
---

## Maintaining environments with DBT and lakeFS
{: .no_toc }

DBT could run on lakeFS with the Spark adapter or the Presto/Trino adapter. 
Both Spark and Presto use Hive metastore or Glue in order to manage tables and views.
When creating a branch in lakeFS we receive a logical copy of the data that could be accessed by `s3://my-repo/branch/...` 
In order to run our DBT project on a new created branch we need to have a copy of the metadata as well.

The lakectl dbt command generates all the metadata needed in order to work on the new created branch,
continuing from the last state in the source branch.
The dbt lakectl command does this using dbt commands and lakectl metastore commands.

{% include toc.html %}

## Configuration 

In order to run the lakectl-dbt commands we need to configure both dbt and lakectl. 
Assuming dbt is already configured using either a Spark or Presto/Trino target 
you will need to add configurations to give access to your catalog (metastore).
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

lakectl copies all models materialized as tables and incremental directly on your metastore.
copying views should be done manually or with lakectl

### Using lakectl

The `generate_schema_name` macro could be used by lakectl to create models using dbt on a dynamic schema.
The following command will add a macro to your project allowing lakectl to run dbt on the destination schema using an environment variable

```bash
lakectl dbt generate-schema-macro
```

### Manually 

In case you don't want to add the `generate_schema_name` macro to your project
you could create the views on the destination schema manually.
For every run:
- use the `--skip-views` flag
- change the default schema to be the branch schema in your dbt configuration file
- run dbt on all views 

```bash
dbt run --select config.materialized:view
```

## Create Schema

Creating the schema 
From your dbt project run:
```bash
lakectl dbt create-branch-schema --branch my-branch --to-schema my_branch   
```

Advanced options could be found [here](../reference/commands.md#lakectl-dbt-create-branch-schema){: .button-clickable}
 

