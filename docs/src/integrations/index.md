---
title: Integrations
description: Integrate lakeFS with all modern data frameworks such as Spark, Apache Iceberg, Hive, AWS Athena, Presto, and more.
---

# Integrations with lakeFS

You can use lakeFS with a wide range of tools and frameworks.

lakeFS provides several clients directly, as well as an [S3-compatible gateway](../understand/architecture.md#s3-gateway). This gateway means that if you want to use something with lakeFS, so long as that technology can interface with S3, it can interface with lakeFS.

See below for detailed instructions for using different technologies with lakeFS.

!!! info "Missing Something?"
    If there is a technology not listed here that you would like to use with lakeFS, please drop by [our Slack](https://lakefs.io/slack) and we'll help you get started with it.

<table>
    <tr>
        <td width="25%" align=center><a href="airbyte/"><img width=120 src="../assets/img/logos/airbyte.png" alt="airbyte logo" /><br />Airbyte</a></td>
        <td width="25%" align=center><a href="athena/"><img width=120 src="../assets/img/logos/athena.png" alt="athena logo"/><br/>Amazon Athena</a></td>
        <td width="25%" align=center><a href="sagemaker/"><img width=120 src="../assets/img/logos/sagemaker.png" alt="sagemaker logo"/><br/>Amazon SageMaker</a></td>
        <td width="25%" align=center><a href="airflow/"><img width=120 src="../assets/img/logos/airflow.png" alt="airflow logo"/><br/>Apache Airflow</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="hive/"><img width=120 src="../assets/img/logos/apache_hive.png" alt="apache_hive logo"/><br/>Apache Hive</a></td>
        <td width="25%" align=center><a href="iceberg/"><img width=120 src="../assets/img/logos/apache_iceberg.png" alt="apache_iceberg logo"/><br/>Apache Iceberg</a></td>
        <td width="25%" align=center><a href="kafka/"><img width=120 src="../assets/img/logos/apache_kafka.png" alt="apache_kafka logo"/><br/>Apache Kafka</a></td>
        <td width="25%" align=center><a href="spark/"><img width=120 src="../assets/img/logos/apache_spark.png" alt="apache_spark logo"/><br/>Apache Spark</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="aws_cli/"><img width=120 src="../assets/img/logos/cli.png" alt="cli logo"/><br/>AWS CLI</a></td>
        <td width="25%" align=center><a href="cloudera/"><img width=120 src="../assets/img/logos/cloudera.png" alt="cloudera logo"/><br/>Cloudera</a></td>
        <td width="25%" align=center><a href="databricks/"><img width=120 src="../assets/img/logos/databricks.png" alt="Databricks Logo"/><br/>Databricks</a></td>
        <td width="25%" align=center><a href="delta/"><img width=120 src="../assets/img/logos/delta-lake.png" alt="delta lake logo"/><br/>Delta Lake</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="dremio/"><img width=120 src="../assets/img/logos/dremio.png" alt="dremio logo"/><br/>Dremio</a></td>
        <td width="25%" align=center><a href="duckdb/"><img width=120 src="../assets/img/logos/duckdb.png" alt="duckdb logo"/><br/>DuckDB</a></td>
        <td width="25%" align=center><a href="git/"><img width=120 src="../assets/img/logos/git.png" alt="git logo"/><br/>Git</a></td>
        <td width="25%" align=center><a href="glue_hive_metastore/"><img width=120 src="../assets/img/logos/glue.png" alt="glue logo"/><br/>Glue / Hive metastore</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="huggingface_datasets/"><img width=120 src="../assets/img/logos/huggingface.png" alt="Hugging Face Logo"/><br/>HuggingFace Datasets</a></td>
        <td width="25%" align=center><a href="kubeflow/"><img width=120 src="../assets/img/logos/kubeflow.png" alt="kubeflow logo"/><br/>Kubeflow</a></td>
        <td width="25%" align=center><a href="presto_trino/"><img width=120 src="../assets/img/logos/trino_presto.png" alt="presto and trino logos"/><br/>Presto / Trino</a></td>
        <td width="25%" align=center><a href="python/"><img width=120 src="../assets/img/logos/python.png" alt="python logo"/><br/>Python</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="r/"><img width=120 src="../assets/img/logos/r.png" alt="r logo"/><br/>R</a></td>
        <td width="25%" align=center><a href="red_hat_openshift_ai/"><img width=120 src="../assets/img/logos/red_hat_openshift_ai.png" alt="Red Hat OpenShift AI Logo"/><br/>Red Hat OpenShift AI</a></td>
        <td width="25%" align=center><a href="vertex_ai/"><img width=120 src="../assets/img/logos/vertex_ai.png" alt="Vertex AI Logo"/><br/>Vertex AI</a></td>
        <td width="25%" align=center><a href="mlflow/"><img width=120 src="../assets/img/logos/MLflow-logo.png" alt="MLflow Logo"/><br/>MLflow</a></td>
    </tr>
</table>
