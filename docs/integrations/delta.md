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

[Delta Lake](https://delta.io/){: .button-clickable} is an open file format designed to improve performance and provide transactional guarantees to data lake tables.

lakeFS is format-agnostic, so you can save data in Delta format within a lakeFS repository to get the benefits of both technologies. Specifically:

1. ACID operations can now span across many Delta tables.
1. [CI/CD hooks](../setup/hooks.md){: .button-clickable} can validate Delta table contents, schema, or even referential integrity.
1. lakeFS supports zero-copy branching for quick experimentation with full isolation.

{% include toc.html %}


## Configuration

Most commonly Delta tables are interacted with in a Spark environment given the native integration between Delta Lake and Spark.

To configure a Spark environment to read from and write to a Delta table within a lakeFS repository, we need to set the proper credentials and endpoint in the S3 Hadoop configuration, like we do with any [Spark](./spark.md#configuration){: .button-clickable} script.

```scala
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.path.style.access", "true")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.access.key", "AKIAIOSFODNN7EXAMPLE")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
 sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.bucket.<repo-name>.endpoint", "https://lakefs.example.com")
```

Once set, you can now interact with Delta tables using regular Spark path URIs. Make sure you include the lakeFS repository and branch name:

```scala
data.write.format("delta").save("s3a://<repo-name>/<branch-name>/path/to/delta-table")
```

Note: If using the Databricks Analytics Platform, see the [integration guide](./databricks.md#configuration){: .button-clickable} for configuring a Databricks cluster to use lakeFS.

## Limitations
The [Delta log](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html){: .button-clickable} is an auto-generated sequence of text files used to keep track of transactions on a Delta table sequentially. Writing to one Delta table from multiple lakeFS branches is possible, but note that it will result in conflicts if later attempting to merge one branch into the other. For that reason, production workflows should ideally write to a single lakeFS branch that could then be safely merged into `main`. 


## Read more
See [this post on the lakeFS blog](https://lakefs.io/guarantee-consistency-in-your-delta-lake-tables-with-lakefs/){: .button-clickable} that shows how to 
guarantee data quality in a Delta table by utilizing lakeFS branches.
