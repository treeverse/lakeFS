---
layout: default
title: Spark
description: Accessing data in lakeFS from Apache Spark is the same as accessing S3 data from Apache Spark
parent: Using lakeFS with...
nav_order: 3
has_children: false
---

# Using lakeFS with Spark
{: .no_toc }
[Apache Spark](https://spark.apache.org/) is a unified analytics engine for big data processing, with built-in modules for streaming, SQL, machine learning and graph processing.
{: .pb-5 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }

Accessing data in lakeFS from Spark is the same as accessing S3 data from Spark.
The only changes we need to consider are:
1. Setting the configurations to access lakeFS
2. Accessing Objects with the lakeFS path convention



## Configuration
In order to configure Spark to work with lakeFS we will set the lakeFS credentials in the corresponding S3 credential fields.
    
lakeFS endpoint: ```fs.s3a.endpoint``` 

lakeFS access key: ```fs.s3a.access.key```

lakeFS secret key: ```fs.s3a.secret.key```

**Note** 
In the following examples we set AWS credentials at runtime, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
{: .note}

For example if we would like to Specify the credentials at run time 
```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "https://s3.lakefs.example.com")
```

   
   
## Accessing Objects
In order for us to access objects in lakeFS we will need to use the lakeFS path conventions:
    ```s3a://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT```

For example: 
Lets assume we want to read a parquet file: 

from repository: ```example-repo```
branch: ```master```
in the path: ```example-path```
      
```scala
val repo = "example-repo"
val branch = "master"
val dataPath = s"s3a://${repo}/${branch}/example-path/example-file.parquet"
...
...
val basics = spark.read.option("header", "true").parquet(dataPath)
```

## Creating Objects

If we would like to create new parquet files partitioned by column `example-column`
```scala
basics.toDF().write.partitionBy("example-column").parquet(outputPath)
```