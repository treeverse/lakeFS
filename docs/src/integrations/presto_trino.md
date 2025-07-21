---
title: Presto / Trino
description: This section explains how you can start using lakeFS with Presto and Trino, an open-source distributed SQL query engine.
---

# Using lakeFS with Presto/Trino

[Presto](https://prestodb.io){:target="_blank"} and [Trino](https://trinodb.io){:target="_blank"} are a distributed SQL query engines designed to query large data sets distributed over one or more heterogeneous data sources.

Querying data in lakeFS from Presto/Trino is similar to querying data in S3 from Presto/Trino. It is done using the [Presto Hive connector](https://prestodb.io/docs/current/connector/hive.html){:target="_blank"} or [Trino Hive connector](https://trino.io/docs/current/connector/hive.html){:target="_blank"}.



!!! warning "Credentials"
    In the following examples, we set AWS credentials at runtime for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 

## Configuration

### Configure the Hive connector

Create `/etc/catalog/hive.properties` with the following contents to mount the `hive-hadoop2` connector as the Hive catalog, replacing `example.net:9083` with the correct host and port for your Hive Metastore Thrift service:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://example.net:9083
```

Add the lakeFS configurations to `/etc/catalog/hive.properties` in the corresponding S3 configuration properties:

```properties
hive.s3.aws-access-key=AKIAIOSFODNN7EXAMPLE
hive.s3.aws-secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
hive.s3.endpoint=https://lakefs.example.com
hive.s3.path-style-access=true
```

### Configure Hive

Presto/Trino uses Hive Metastore Service (HMS) or a compatible implementation of the Hive Metastore such as AWS Glue Data Catalog to write data to S3.
In case you are using Hive Metastore, you will need to configure Hive as well.

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
 


## Integration with lakeFS Data Catalogs

For advanced integration with lakeFS that supports querying different branches as schemas, see the [Data Catalog Exports documentation](../howto/catalog_exports.md). This approach allows you to:

- Query data from specific lakeFS branches using branch names as schemas
- Automate table metadata synchronization through hooks
- Support multiple table formats (Hive, Delta Lake, etc.)

For AWS Glue users, see the detailed [Glue Data Catalog integration guide](./glue_metastore.md) which provides step-by-step instructions for setting up automated exports.
