---
layout: default
title: Presto
parent: Using lakeFS with...
nav_order: 7
has_children: false
---

# Using lakeFS with Presto
{: .no_toc }
[Presto](https://prestodb.io/) is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.
{: .pb-5 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }

Querying data in lakeFS from Presto is the same as querying data in S3 from Presto.
It is done using the [Hive connector](https://prestodb.io/docs/current/connector/hive.html).

 **Note** 
 In the following examples we set AWS credentials at runtime, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
 {: .note}
## Configuration

### Configure Hive connector
Create ```/etc/catalog/hive.properties``` with the following contents to mount the ```hive-hadoop2``` connector as the ```hive``` catalog, replacing ```example.net:9083``` with the correct host and port for your Hive metastore Thrift service:
```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://example.net:9083
```

Add to ```/etc/catalog/hive.properties``` the lakeFS configurations in the corresponding S3 configuration properties:
```properties
hive.s3.aws-access-key=AKIAIOSFODNN7EXAMPLE
hive.s3.aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
hive.s3.endpoint=https://s3.lakefs.example.com
```

### Configure Hive
Presto uses Hive to write data to S3.
If we want to be able to write data to lakeFS we will need to configure hive as well.
In file ``` hdfs-site.xml``` add to the configuration:
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
        <value>https://s3.lakefs.example.com</value>
    </property>
</configuration>
```
 

## Examples

Here are some examples based on examples from the [Presto Hive connector documentation](https://prestodb.io/docs/current/connector/hive.html#examples)

### Example with schema
Create a new schema named ```master``` that will store tables in a lakeFS repository named ```example``` branch: ```master```:
```sql
CREATE SCHEMA master
WITH (location = 's3a://example/master')
```

Create a new Hive table named ```page_views``` in the ```web``` schema that is stored using the ORC file format,
 partitioned by date and country, and bucketed by user into ```50``` buckets (note that Hive requires the partition columns to be the last columns in the table):
```sql
CREATE TABLE master.page_views (
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
Create an external Hive table named ```request_logs``` that points at existing data in lakeFS:

```sql
CREATE TABLE master.request_logs (
  request_time timestamp,
  url varchar,
  ip varchar,
  user_agent varchar
)
WITH (
  format = 'TEXTFILE',
  external_location = 's3a://example/master/data/logs/'
)
```

### Example of copying a table with [metastore tools](glue_hive_metastore.md):
Copy the created table `page_views` on schema `master` to schema `example_branch` with location `s3a://example/example_branch/page_views/` 
```shell
$ lakectl metastore copy --from-schema master --from-table page_views   --to-branch example_branch 
```



