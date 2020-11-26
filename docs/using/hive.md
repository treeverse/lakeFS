---
layout: default
title: Hive
description: This section covers how you can start using lakeFS with Apache Hive, a distributed data warehouse system that enables analytics at a massive scale.
parent: Using lakeFS with...
nav_order: 4
has_children: false
---

# Using lakeFS with Hive
{: .no_toc }
The [Apache Hive ™](https://hive.apache.org/) data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Structure can be projected onto data already in storage. A command line tool and JDBC driver are provided to connect users to Hive.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }


## Configuration
In order to configure hive to work with lakeFS we will set the lakeFS credentials in the corresponding S3 credential fields.
    
lakeFS endpoint: ```fs.s3a.endpoint``` 

lakeFS access key: ```fs.s3a.access.key```

lakeFS secret key: ```fs.s3a.secret.key```

 **Note** 
 In the following examples we set AWS credentials at runtime, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
 {: .note}
 
For example, we could add the configurations to the file ``` hdfs-site.xml```:
```xml
<configuration>
    ...
    <property>
        <name>fs.s3a.secret.key</name>
        <value>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>AKIAIOSFODNN7EXAMPLE</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>https://s3.lakefs.example.com</value>
    </property>
</configuration>
```

## Examples

### Example with schema

```hql
CREATE  SCHEMA example LOCATION 's3a://example/master/' ;
CREATE TABLE example.request_logs (
    request_time timestamp,
    url string,
    ip string,
    user_agent string
);
```
### Example with external table

```hql
CREATE EXTERNAL TABLE request_logs (
    request_time timestamp,
    url string,
    ip string,
    user_agent string
) LOCATION 's3a://example/master/request_logs' ;
```




