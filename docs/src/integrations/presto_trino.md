---
title: Trino / Presto
description: This section explains how you can start using lakeFS with the Trino and Presto open-source distributed SQL query engines.
---

# Using lakeFS with Trino / Presto 

 [Trino](https://trinodb.io){:target="_blank"} and [Presto](https://prestodb.io){:target="_blank"} are distributed SQL query engines designed to query large data sets distributed over one or more heterogeneous data sources.



## Iceberg REST Catalog 

lakeFS Iceberg REST Catalog allow you to use lakeFS as a [spec-compliant](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) Apache [Iceberg REST catalog](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml), 
allowing Trino/Presto to manage and access tables using a standard REST API. 

![lakeFS Iceberg REST Catalog](../assets/img/lakefs_iceberg_rest_catalog.png)

This is the recommended way to use lakeFS with Trino/Presto, as it allows lakeFS to stay completely outside the data path: data itself is read and written by Trino/Presto executors, directly to the underlying object store. Metadata is managed by Iceberg at the table level, while lakeFS keeps track of new snapshots to provide versioning and isolation.

[Read more about using the Iceberg REST Catalog](./iceberg.md#iceberg-rest-catalog).

### Configuration

To use the Iceberg REST Catalog, you need to configure Trino/Presto to use the [Iceberg REST catalog endpoint](https://trino.io/docs/current/object-storage/metastores.html#iceberg-rest-catalog):

!!! tip
    To learn more about the Iceberg REST Catalog, see the [Iceberg REST Catalog](./iceberg.md#iceberg-rest-catalog) documentation.


```properties
# example: /etc/trino/catalog/lakefs.properties
connector.name=iceberg

# REST Catalog connection
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://lakefs.example.com/iceberg/api
iceberg.rest-catalog.nested-namespace-enabled=true

# REST Catalog authentication
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=${ENV:LAKEFS_CREDENTIALS}
iceberg.rest-catalog.oauth2.server-uri=https://lakefs.example.com/iceberg/api/v1/oauth/tokens

# Object storage access to underlying tables (modify this to match your storage provider)
fs.hadoop.enabled=false
fs.native-s3.enabled=true
s3.region=us-east-1
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
```

### Usage

Once configured, you can use the Iceberg REST Catalog to query and update Iceberg tables.

```sql
USE "repo.main.inventory";
SHOW TABLES;
SELECT * FROM books LIMIT 100;
```

```sql
USE "repo.new_branch.inventory";
SHOW TABLES;
SELECT * FROM books LIMIT 100;
```

## Using Presto/Trino with the S3 Gateway

Using the S3 Gateway allows reading and writing data to lakeFS from Presto/Trino, in any format supported by Presto/Trino (i.e. not just Iceberg tables). 

While flexible, this approach requires lakeFS to be involved in the data path, which can be less efficient than the Iceberg REST Catalog approach, since lakeFS has to proxy all data operations through the lakeFS server. This is particularly true for large data sets where network bandwidth might incur some overhead.

### Configuration

!!! warning "Credentials"
    In the following examples, we set AWS credentials at runtime for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 


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

#### Configure Hive

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
 
 