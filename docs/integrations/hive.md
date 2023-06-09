---
layout: default
title: Hive
description: This section covers how you can start using lakeFS with Apache Hive, a distributed data warehouse system that enables analytics at a massive scale.
parent: Integrations
nav_order: 140
has_children: false
redirect_from: /using/hive.html
---

# Using lakeFS with Hive
The [Apache Hive ™](https://hive.apache.org/) data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Structure can be projected onto data already in storage. A command line tool and JDBC driver are provided to connect users to Hive.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }


## Configuration
To configure Hive to work with lakeFS, you need to set the lakeFS credentials in the corresponding S3 credential fields.
    
lakeFS endpoint: ```fs.s3a.endpoint``` 

lakeFS access key: ```fs.s3a.access.key```

lakeFS secret key: ```fs.s3a.secret.key```

 **Note** 
In the following examples, we set AWS credentials at runtime for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
 {: .note}
 
For example, you can add the configurations to the file ``` hdfs-site.xml```:
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
        <value>https://lakefs.example.com</value>
    </property>
    <property>
       <name>fs.s3a.path.style.access</name>
       <value>true</value>
    </property>
</configuration>
```

**Note**
In this example, we set `fs.s3a.path.style.access` to true to remove the need for additional DNS records for [virtual hosting](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)
`fs.s3a.path.style.access` that was introduced in Hadoop 2.8.0
{: .note}

## Examples

### Example with schema

```hql
CREATE  SCHEMA example LOCATION 's3a://example/main/' ;
CREATE TABLE example.request_logs (
    request_time timestamp,
    url string,
    ip string,
    user_agent string
);
```
### Example with an external table

```hql
CREATE EXTERNAL TABLE request_logs (
    request_time timestamp,
    url string,
    ip string,
    user_agent string
) LOCATION 's3a://example/main/request_logs' ;
```




