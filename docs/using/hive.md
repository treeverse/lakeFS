---
layout: default
title: Hive
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
 
For example we could add the configurations to thie file ``` hdfs-site.xml```:
```xml
<configuration>
...
...
<property><name>fs.s3a.secret.key</name><value>dq50hnfLB1qd1zQ8I9l3TeqKnP+1wKr81Bw1BSz1</value></property>
<property><name>fs.s3a.access.key</name><value>AKIAJF4EV2DBC56IOAOQ</value></property>
<property><name>fs.s3a.endpoint</name><value>http://s3.localdev.treeverse.io:8000</value></property>
</configuration>
```

## Examples

### Example with schema

```hiveql
CREATE  SCHEMA example  LOCATION 's3a://example/master/' ;
CREATE TABLE example.request_logs (request_time timestamp, url string, ip string, user_agent string );
```
### Example with external table

```hiveql
CREATE EXTERNAL TABLE request_logs (request_time timestamp, url string, ip string, user_agent string ) LOCATION 's3a://example/master/request_logs' ;
```




