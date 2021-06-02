---
layout: default
title: Delta
description: This section explains how to interact with your lakeFS when using delta format
parent: Integrations
nav_order: 62
has_children: false
---

# Using lakeFS with Delta Lake
{: .no_toc }

[Delta Lake](https://delta.io/) is an open source project that enables building a Lakehouse architecture on top of data lakes.  

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}
   
## Use-cases
Using lakeFS with Delta tables allows supercharging Delta Lake to enable much better data safety guarantees, while simplifying operations:

1. ACID operations can now span across many Delta tables.
1. [CI/CD hooks](../setup/hooks.md) can be used to validate data quality and even ensure referential integrity.
1. Zero-copy [development environment](../usecases/data-devenv.md): Experiment with ETL code, algorithm changes and schema changes in isolation, without copying data!


## Configuration

`Delta` is a data source format and files are being written as Parquet files by the s3a driver.
We need to set the proper credentials and endpoint in the S3 Hadoop configuration,
like we do with any [Spark](./spark.md#configuration) script.

```scala
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.access.key", "AKIAIOSFODNN7EXAMPLE")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.endpoint", "https://s3.lakefs.example.com")
```

You can continue to read/write Delta tables the same way you did without lakeFS,
while using lakeFS repos and branchs as the path prefixes:

```scala
data.write.format("delta").save("s3a://<repo-name>/<branch-name>/tmp/delta-table")
```

## Limitations
Delta logs are sequential. Writing to the same delta log from multiple lakeFS branches is possible,
But would result with conflicts when trying to merge those branches.
It's best to avoid writing to the same Delta log from different branches.

## Read more
[Blog post](https://lakefs.io/guarantee-consistency-in-your-delta-lake-tables-with-lakefs/) that shows how to 
guarantee data quality in a Delta table by utilizing lakeFS branches.