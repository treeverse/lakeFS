---
layout: default
title: Presto/Trino
description: This section covers how you can start using lakeFS with Presto/Trino, an open source distributed SQL query engine
parent: Integrations
nav_order: 40
has_children: false
redirect_from:
    - /integrations/presto.html
    - /using/presto.html
---

# Using lakeFS with Presto/Trino

{: .no_toc }
[Presto](https://prestodb.io){:target="_blank" .button-clickable} and [Trino](https://trinodb.io){:target="_blank" .button-clickable} are a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.
{: .pb-5 }

## Table of contents

{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }

Querying data in lakeFS from Presto/Trino is the same as querying data in S3 from Presto/Trino. It is done using the [Presto Hive connector](https://prestodb.io/docs/current/connector/hive.html){:target="_blank" .button-clickable} or [Trino Hive connector](https://trino.io/docs/current/connector/hive.html){:target="_blank" .button-clickable}.

 **Note** 
 In the following examples we set AWS credentials at runtime, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank" .button-clickable}. 
 {: .note}

## Configuration

### Configure Hive connector

Create `/etc/catalog/hive.properties` with the following contents to mount the `hive-hadoop2` connector as the `hive` catalog, replacing `example.net:9083` with the correct host and port for your Hive metastore Thrift service:
```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://example.net:9083
```

Add to `/etc/catalog/hive.properties` the lakeFS configurations in the corresponding S3 configuration properties:
```properties
hive.s3.aws-access-key=AKIAIOSFODNN7EXAMPLE
hive.s3.aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
hive.s3.endpoint=https://lakefs.example.com
hive.s3.path-style-access=true
```

### Configure Hive

Presto/Trino uses Hive metastore service (HMS), or a compatible implementation of the Hive metastore, such as AWS Glue Data Catalog to write data to S3.
In case you are using Hive metastore, you will need to configure Hive as well.
In file `hive-site.xml` add to the configuration:
```xml
<configuration>
    ...
    <property>
        <name>fs.s3a.access.key</name>
        <value>AKIAIOSFODNN7EXAMPLE</value></property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>https://lakefs.example.com</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
</configuration>
```
 

## Examples

Here are some examples based on examples from the [Presto Hive connector examples](https://prestodb.io/docs/current/connector/hive.html#examples){:target="_blank" .button-clickable} and [Trino Hive connector examples](https://trino.io/docs/current/connector/hive.html#examples){:target="_blank" .button-clickable}

### Example with schema

Create a new schema named `main` that will store tables in a lakeFS repository named `example` branch: `master`:
```sql
CREATE SCHEMA main
WITH (location = 's3a://example/main')
```

Create a new Hive table named `page_views` in the `web` schema that is stored using the ORC file format,
 partitioned by date and country, and bucketed by user into `50` buckets (note that Hive requires the partition columns to be the last columns in the table):
```sql
CREATE TABLE main.page_views (
  view_time timestamp,
  user_id bigint,
  page_url varchar,
  ds date,
  country varchar
)
WITH (
  format = 'ORC',
  partitioned_by = ARRAY['ds', 'country'],
  bucketed_by = ARRAY['user_id'],
  bucket_count = 50
)
```

### Example with External table

Create an external Hive table named `request_logs` that points at existing data in lakeFS:

```sql
CREATE TABLE main.request_logs (
  request_time timestamp,
  url varchar,
  ip varchar,
  user_agent varchar
)
WITH (
  format = 'TEXTFILE',
  external_location = 's3a://example/main/data/logs/'
)
```

### Example of copying a table with [metastore tools](glue_hive_metastore.md){: .button-clickable}:

Copy the created table `page_views` on schema `main` to schema `example_branch` with location `s3a://example/example_branch/page_views/` 
```shell
lakectl metastore copy --from-schema main --from-table page_views --to-branch example_branch 
```
