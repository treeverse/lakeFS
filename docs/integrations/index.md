---
layout: default
title: Integrations
description: Integrate lakeFS with all modern data frameworks such as Spark, Apache Iceberg, Hive, AWS Athena, Presto, and more.
has_children: true
has_toc: false
nav_order: 10
redirect_from: /using
---

# Integrations with lakeFS

You can use lakeFS with a wide range of tools and frameworks. 

lakeFS provides several clients directly, as well as an [S3-compatible gateway](../understand/architecture.md#s3-gateway). This gateway means that if you want to use something with lakeFS, so long as that technology can interface with S3, it can interface with lakeFS. 

See below for detailed instructions for using different technologies with lakeFS. 

<table>
<tr><td width="25%"><a href="./airbyte.html">Airbyte</a></td> <td width="25%"><a href="./athena.html">Amazon Athena</a></td> <td width="25%"><a href="./sagemaker.html">Amazon SageMaker</a></td> <td width="25%"><a href="./airflow.html">Apache Airflow</a></td></tr>
<tr><td width="25%"><a href="./hive.html">Apache Hive</a></td> <td width="25%"><a href="./iceberg.html">Apache Iceberg</a></td> <td width="25%"><a href="./kafka.html">Apache Kafka</a></td> <td width="25%"><a href="./spark.html">Apache Spark</a></td></tr>
<tr><td width="25%"><a href="./aws_cli.html">AWS CLI</a></td> <td width="25%"><a href="./cloudera.html">Cloudera</a></td> <td width="25%"><a href="./dbt.html">dbt</a></td> <td width="25%"><a href="./delta.html">Delta Lake</a></td></tr>
<tr><td width="25%"><a href="./dremio.html">Dremio</a></td> <td width="25%"><a href="./duckdb.html">DuckDB</a></td> <td width="25%"><a href="./glue_hive_metastore.html">Glue / Hive metastore</a></td> <td width="25%"><a href="./kubeflow.html">Kubeflow</a></td></tr>
<tr><td width="25%"><a href="./presto_trino.html">Presto / Trino</a></td> <td width="25%"><a href="./python.html">Python</a></td> <td width="25%"><a href="./r.html">R</a></td><td width="25%">&nbsp;</td></tr>
</table>

{: .tip}
If there is a technology not listed here that you would like to use with lakeFS, please drop by [our Slack](/slack) and we'll help you get started with it.
