---
layout: default
title: Delta Lake
description: This section explains how to use Delta Lake with lakeFS
parent: Integrations
nav_order: 62
has_children: false
---

# Using lakeFS with Delta Lake
{: .no_toc }

[Delta Lake](https://delta.io/) is an open file format designed to improve performance and provide transactional guarantees to data lake tables.

lakeFS is format-agnostic, so you can save data in Delta format within a lakeFS repository to get the benefits of both technologies. Specifically:

1. ACID operations can now span across many Delta tables.
1. [CI/CD hooks](../setup/hooks.md) can validate Delta table contents, schema, or even referential integrity.
1. lakeFS branches cam zero-copy Delta tables for quick experimentation.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}


## Configuration

Most commonly Delta tables are interacted with in a Spark environment given the native integration between Delta and Spark.

To configure a Spark environment to read from and write to a Delta table within a lakeFS repository, we need to set the proper credentials and endpoint in the S3 Hadoop configuration, like we do with any [Spark](./spark.md#configuration) script.

```scala
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.access.key", "AKIAIOSFODNN7EXAMPLE")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.endpoint", "https://s3.lakefs.example.com")
```

Once set, you can now interact with Delta tables using the same commands you would use without lakeFS, except for altering the path prefix to include the lakeFS repo and branch names:

```scala
data.write.format("delta").save("s3a://<repo-name>/<branch-name>/path/to/delta-table")
```

Note: If using the Databricks Analytics Platform, see the [integration guide](./databricks.md#configuration) for configuring a Databricks cluster to use lakeFS.

## Limitations
The [Delta log](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) is an auto-generated text file used to keep track of transactions on a Delta table sequentially. Writing to one Delta table from multiple lakeFS branches is possible, but note that it will result in conflicts if later attempting to merge one branches into the other.


## Read more
[Blog post](https://lakefs.io/guarantee-consistency-in-your-delta-lake-tables-with-lakefs/) that shows how to 
guarantee data quality in a Delta table by utilizing lakeFS branches.