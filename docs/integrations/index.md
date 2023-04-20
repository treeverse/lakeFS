---
layout: default
title: Integrations
description: You can integrate lakeFS with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.
nav_order: 20
has_children: true
has_toc: false
redirect_from: /using
---

# Integrations

<table>
<tr></tr><td><a href="https://docs.lakefs.io/integrations/airbyte.html">Airbyte</a></td>
<td><a href="https://docs.lakefs.io/integrations/athena.html">Amazon Athena</a></td>
<td><a href="https://docs.lakefs.io/integrations/airflow.html">Apache Airflow</a></td></tr>
<tr></tr><td><a href="https://docs.lakefs.io/integrations/hive.html">Apache Hive</a></td>
<td><a href="https://docs.lakefs.io/integrations/iceberg.html">Apache Iceberg</a></td>
<td><a href="https://docs.lakefs.io/integrations/kafka.html">Apache Kafka</a></td></tr>
<tr></tr><td><a href="https://docs.lakefs.io/integrations/spark.html">Apache Spark</a></td>
<td><a href="https://docs.lakefs.io/integrations/aws_cli.html">AWS CLI</a></td>
<td><a href="https://docs.lakefs.io/integrations/dbt.html">dbt</a></td></tr>
<tr></tr><td><a href="https://docs.lakefs.io/integrations/delta.html">Delta Lake</a></td>
<td><a href="https://docs.lakefs.io/integrations/dremio.html">Dremio</a></td>
<td><a href="https://docs.lakefs.io/integrations/duckdb.html">DuckDB</a></td></tr>
<tr></tr><td><a href="https://docs.lakefs.io/integrations/glue_hive_metastore.html">Glue / Hive metastore</a></td>
<td><a href="https://docs.lakefs.io/integrations/kubeflow.html">Kubeflow</a></td>
<td><a href="https://docs.lakefs.io/integrations/presto_trino.html">Presto/Trino</a></td></tr>
<tr></tr><td><a href="https://docs.lakefs.io/integrations/python.html">Python</a></td>
<td><a href="https://docs.lakefs.io/integrations/sagemaker.html">SageMaker</a></td><td>&nbsp;</td></tr>
</table>

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
