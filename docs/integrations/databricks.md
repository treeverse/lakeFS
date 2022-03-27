---
layout: default
title: Databricks
description: This section explains how to interact with your lakeFS data from Databricks
parent: Integrations
nav_order: 60
has_children: false
redirect_from: ../using/databricks.html
---

# Using lakeFS with Databricks
{: .no_toc }

[Databricks](https://databricks.com/){: .button-clickable} is an Apache Spark-based analytics platform.  

{% include toc.html %}

## Configuration

For Databricks to work with lakeFS, set the S3 Hadoop configuration to the lakeFS endpoint and credentials:

1. In databricks, go to your cluster configuration page.
1. Click **Edit**.
1. Expand **Advanced Options**
1. Under the **Spark** tab, add the following configurations, replacing `<repo-name>` with your lakeFS repository name.
   Also replace the credentials and endpoint with those of your lakeFS installation.

```
spark.hadoop.fs.s3a.bucket.<repo-name>.access.key AKIAIOSFODNN7EXAMPLE
spark.hadoop.fs.s3a.bucket.<repo-name>.secret.key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
spark.hadoop.fs.s3a.bucket.<repo-name>.endpoint https://lakefs.example.com
spark.hadoop.fs.s3a.path.style.access true
```

When using DeltaLake tables, the following is also needed in some versions of Databricks:

```
spark.hadoop.fs.s3a.bucket.<repo-name>.aws.credentials.provider shaded.databricks.org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider
spark.hadoop.fs.s3a.bucket.<repo-name>.session.token lakefs
```

For more information, see
the [documentation](https://docs.databricks.com/data/data-sources/aws/amazon-s3.html#configuration){: .button-clickable} from Databricks.

### When running lakeFS inside your VPC

When lakeFS runs inside your private network, your Databricks cluster needs to be able to access it. This can be done by
setting up a VPC peering between the two VPCs (the one where lakeFS runs, and the one where Databricks runs). For this to
work on DeltaLake tables, you would also have to
disable [multi-cluster writes](https://docs.databricks.com/delta/delta-faq.html#what-does-it-mean-that-delta-lake-supports-multi-cluster-writes){: .button-clickable} with: 

```
spark.databricks.delta.multiClusterWrites.enabled false
```

#### Using multi-cluster writes

When using multi-cluster writes, Databricks overrides Delta's s3-commit action. The new action tries to contact lakeFS
from servers on Databricks **own** AWS account, which of course will not be able to access your private network. So, if
you must use multi-cluster writes, your will have to allow access from Databricks' AWS account to lakeFS.
We are researching for the best ways to achieve that, and will update here soon. 

## Reading Data

In order for us to access objects in lakeFS we will need to use the lakeFS path
conventions: `s3a://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT`

Here is an example for reading a parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "main"
val dataPath = s"s3a://${repo}/${branch}/example-path/example-file.parquet"

val df = spark.read.parquet(dataPath)
```

You can now use this DataFrame like you would normally do.

## Writing Data

Now simply write your results back to a lakeFS path:

```scala
df.write
  .partitionBy("example-column")
  .parquet(s"s3a://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes, or revert them.

## Case Study: SimilarWeb
See how SimilarWeb integrated [lakeFS with DataBricks](https://similarweb.engineering/data-versioning-for-customer-reports-using-databricks-and-lakefs/){: .button-clickable}.
