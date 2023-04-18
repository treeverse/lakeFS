---
layout: default
title: Integrations
description: You can integrate lakeFS with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.
nav_order: 20
has_children: true
redirect_from: /using
---

# Integrations

   **💡 Help improve lakeFS**<br/>
   Part of lakeFS' mission is to seamlessly integrate with all data frameworks and tools.
   If there's a integration you're missing or one you believe should be improved, feel free to [discuss on GitHub](https://github.com/treeverse/lakeFS/issues?q=is%3Aissue+is%3Aopen+label%3Aarea%2Fintegrations).
   {: .note }

## Integrating using the S3 Gateway

lakeFS provides an S3-compatible endpoint (see [**S3 Gateway**](../understand/architecture.html)).

This endpoint allows a variety of data systems that support the S3 protocol to seamlessly consume and produce data on lakeFS without having a custom connector or integration.

This enables integrating lakeFS with whichever data stack you're using, including all the common data ingestion, compute orchestration and data quality - as well as all common ML and research frameworks.

## Custom integrations

lakeFS provides deep integration with common data tools.
These integrations allow users to improve performance by optimizing access to storage: a common example of this is the [Hadoop / Spark integration](./spark.html) that allows applications to [directly read/write](./spark.md#use-the-lakefs-hadoop-filesystem) to the underlying object store while lakeFS keeps track of the metadata.

Other integrations, such as the [Airflow Provider](./airflow.html) allow users to automatically commit and capture important metadata from the host environment.

See the full list of supported integrations below.