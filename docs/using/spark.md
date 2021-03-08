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
1. Setting the configurations to access lakeFS.
1. Accessing objects using the lakeFS path convention.

## Configuration
In order to configure Spark to work with lakeFS, we set S3 Hadoop configuration to the lakeFS endpoint and credentials:

| Hadoop Configuration | Value                        |
|----------------------|------------------------------|
| `fs.s3a.endpoint`    | Set to the lakeFS endpoint   |
| `fs.s3a.access.key`  | Set to the lakeFS access key |
| `fs.s3a.secret.key`  | Set to the lakeFS secret key |

**Note** 
In the following example we set AWS credentials at runtime, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
{: .note}

Here is how to do it in Spark code: 
```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "https://s3.lakefs.example.com")
```
  
## Reading Data
In order for us to access objects in lakeFS we will need to use the lakeFS path conventions:
    ```s3a://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT```

Here is an example for reading a parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "master"
val dataPath = s"s3a://${repo}/${branch}/example-path/example-file.parquet"

val df = spark.read.parquet(dataPath)
```

You can now use this DataFrame like you would normally do.

## Writing Data

Now simply write your results back to a lakeFS path:
```scala
df.write.partitionBy("example-column").parquet(s"s3a://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes, or revert them.
