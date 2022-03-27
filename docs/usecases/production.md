---
layout: default
title: In Production
parent: Using lakeFS
description: lakeFS helps recover from errors and find root case in production.
nav_order: 55
---

## In Production

Errors with data in production inevitably occur. When they do, they best thing we can do is remove the erroneous data, understand why the issue happened, and deploy changes that prevent it from occurring again.

### Example 1: RollBack! - Data ingested from a Kafka stream

If you introduce a new code version to production and discover it has a critical bug, you can simply roll back to the previous version.
But you also need to roll back the results of running it. 
Similar to Git, lakeFS allows you to revert your commits in case they introduced low-quality data.
Revert in lakeFS is an atomic action that prevents the data consumers from receiving low quality data until the issue is resolved.

As previously mentioned, with lakeFS the recommended branching schema is to ingest data to a dedicated branch. When streaming data, we can decide to merge the incoming data to main at a given time interval or checkpoint, depending on how we chose to write it from Kafka. 

You can run quality tests for each merge (as discussed in the [During Deployment](./ci.md){: .button-clickable} section). Alas, tests are not perfect and we might still introduce low quality data to our main branch at some point.
In such a case, we can revert the bad commits from main to the last known high quality commit. This will record new commits reversing the effect of the bad commits.
 

<img src="{{ site.baseurl }}/assets/img/branching_7.png" alt="branching_7" width="500px"/>

_Reverting commits using the CLI_

   ```shell
   lakectl branch revert lakefs://example-repo/main 20c30c96 ababea32
   ```

**Note** lakeFS version <= v0.33.1 uses '@' (instead of '/') as separator between repository and branch.

### Example 2: Troubleshoot - Reproduce a bug in production

You upgraded spark and deployed changes in production. A few days or weeks later, you identify a data quality issue, a performance degradation, or an increase to your infra costs. Something that requires investigation and fixing (aka, a bug).

lakeFS allows you to open a branch of your lake from the specific merge/commit that introduced the changes to production. Using the metadata saved on the merge/commit  you can reproduce all aspects of the environment, then reproduce the issue on the branch and debug it. Meanwhile,  you can revert the main to a previous point in time, or keep it as is, depending on the use case

<img src="{{ site.baseurl }}/assets/img/branching_3.png" alt="branching_3" width="500px"/>


_Reading from a historic version (a previous commit) using Spark_

   ```scala
   // represents the data as existed at commit "11eef40b":
   spark.read.parquet("s3://example-repo/11eef40b/events/by-date")
   ```

### Example 3: Cross collection consistency

We often need consistency between different data collections. A few examples may be:
 - To join different collections in order to create a unified view of an account, a user or another entity we measure.
 - To introduce the same data in different formats
 - To introduce the same data with a different leading index or sorting due to performance considerations

lakeFS will help ensure you introduce only consistent data to your consumers by exposing the new collections and their join in one atomic action to main. Once you consumed the collections on a different branch, and only when both are synchronized, we calculated the join and merged to main. 

In this example you can see two data sets (Sales data and Marketing data) consumed each to its own independent branch, and after the write of both data sets is completed, they are merged to a different branch (leads branch) where the join ETL runs and creates a joined collection by account. The joined table is then merged to main.
The same logic can apply if the data is ingested in streaming, using standard formats, or formats that allow upsert/delete such as Apache Hudi, Delta Lake or Iceberg.

<img src="{{ site.baseurl }}/assets/img/branching_8.png" alt="branching_8" width="500px"/>

## Case Study: Windward
See how Windward is using lakeFS’ isolation and atomic commits to [achieve consistency](https://medium.com/data-rocks/how-windward-leverages-lakefs-for-resilient-data-ingestion-52b838da2cb8){: .button-clickable} on top of S3.

