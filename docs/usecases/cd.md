---
layout: default
title: Continuous Data Deployment
parent: Use-Cases
description: >-
  lakeFS helps you continuously validate expectations and assumptions from the
  data itself.
nav_order: 45
---

# Continuous Deployment

Not every day we introduce new data to the lake, or add/change ETLs, but we do have recurring jobs that are running, and updates to our existing data collections. Even if the code and infra didn't change, the data might, and those changes introduce quality issues. This is one of the complexities of a data product, the data we consume changes over the course of a month, a week, or even a single day.

**Examples of changes to data that may occur:**

* A client-side bug in the data collection of website events
* A new Android version that interferes with the collecting events from your App
* COVID-19 abrupt impact on consumers' behavior, and its effect on the accuracy of ML models.
* During a change to Salesforce interface, the validation requirement from a certain field had been lost

lakeFS helps you validate your expectations and assumptions from the data itself.

## Example 1: Pre merge hook - a data quality issue

Continuous deployment of existing data we expect to consume, flowing from our ingest-pipelines into the lake. Similar to the Continuous Integration use-case - we create a ingest branch \(“events-data”\), which allows us to create tests using data analysis tools or data quality services \(e.g. [Great Expectations](https://greatexpectations.io/){: target="\_blank" }, [Monte Carlo](https://www.montecarlodata.com/){: target="\_blank" }\) to ensure reliability of the data we merge to the main branch. Since merge is atomic, no performance issue will be introduced by using lakeFS, but your main branch will only include quality data.

![branching\_6](../../.gitbook/assets/branching_6%20%281%29.png)

## Example 2: RollBack! - Data ingested from a Kafka stream

If you introduce a new code version to production and discover it has a critical bug, you can simply roll back to the previous version. But you also need to roll back the results of running it. lakeFS gives you the power to rollback your data if you introduced low quality data. The rollback is an atomic action that prevents the data consumers from receiving low quality data until the issue is resolved.

As previously mentioned, with lakeFS the recommended branching schema is to ingest data to a dedicated branch. When streaming data, we can decide to merge the incoming data to main at a given time interval or checkpoint, depending on how we chose to write it from Kafka.

You can run quality tests for each merge \(as presented in Example 1\). Alas, tests are not perfect and we might still introduce low quality data at some point. In such a case, we can rollback main to the last known high quality commit, since our commits for streaming will include the metadata of the Kafka offset.

![branching\_7](../../.gitbook/assets/branching_7%20%281%29.png)

_Rolling back a branch to a previous commit using the CLI_

```text
   lakectl branch reset lakefs://example-repo/stream-1 --commit ~79RU9aUsQ9GLnU
```

**Note** lakeFS version &lt;= v0.33.1 uses '@' \(instead of '/'\) as separator between repository and branch.

## Example 3: Cross collection consistency

We often need consistency between different data collections. A few examples may be:

* To join different collections in order to create a unified view of an account, a user or another entity we measure.
* To introduce the same data in different formats
* To introduce the same data with a different leading index or sorting due to performance considerations

lakeFS will help ensure you introduce only consistent data to your consumers by exposing the new collections and their join in one atomic action to main. Once you consumed the collections on a different branch, and only when both are synchronized, we calculated the join and merged to main.

In this example you can see two data sets \(Sales data and Marketing data\) consumed each to its own independent branch, and after the write of both data sets is completed, they are merged to a different branch \(leads branch\) where the join ETL runs and creates a joined collection by account. The joined table is then merged to main. The same logic can apply if the data is ingested in streaming, using standard formats, or formats that allow upsert/delete such as Apache Hudi, Delta Lake or Iceberg.

![branching\_8](../../.gitbook/assets/branching_8%20%281%29.png)

