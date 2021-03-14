---
layout: default
title: Databricks
description: Interact with your lakeFS data from Databricks
parent: Using lakeFS with...
nav_order: 4
has_children: false
---

# Using lakeFS with Databricks
{: .no_toc }
[Databricks](https://databricks.com/) is an Apache Spark-based analytics platform.  
{: .pb-5 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Configuration

For Databricks to work with lakeFS, set the S3 Hadoop configuration to the lakeFS endpoint and credentials:
1. In databricks, go to your cluster configuration page.
1. Click **Edit**.
1. Expand **Advanced Options**
1. Under the **Spark** tab, add the following configurations:

```
spark.hadoop.fs.s3a.access.key AKIAIOSFODNN7EXAMPLE
spark.hadoop.fs.s3a.secret.key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
spark.hadoop.fs.s3a.endpoint https://s3.lakefs.example.com
```

For more information, see the [documentation](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html#configuration) from Databricks.

### Per-bucket configuration

The above configuration will use lakeFS as the sole S3 endpoint. To use lakeFS in parallel with S3, you can configure Spark to use lakeFS only for specific bucket names.
For example, to configure only `example-repo` to use lakeFS, set the following configurations:

```
spark.hadoop.fs.s3a.bucket.example-repo.endpoint AKIAIOSFODNN7EXAMPLE
spark.hadoop.fs.s3a.bucket.example-repo.access.key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
spark.hadoop.fs.s3a.bucket.example-repo.secret.key https://s3.lakefs.example.com
```

With this configuration set , reading s3a paths with `example-repo` as the bucket will use lakeFS, while all other buckets will use AWS S3.

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
