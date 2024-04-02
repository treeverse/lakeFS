---
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
    <tr>
        <td width="25%" align=center><a href="./airbyte.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/airbyte.png" alt="airbyte logo" /><br />Airbyte</a></td>
        <td width="25%" align=center><a href="./athena.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/athena.png" alt="athena logo"/><br/>Amazon Athena</a></td>
        <td width="25%" align=center><a href="./sagemaker.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/sagemaker.png" alt="sagemaker logo"/><br/>Amazon SageMaker</a></td>
        <td width="25%" align=center><a href="./airflow.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/airflow.png" alt="airflow logo"/><br/>Apache Airflow</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="./hive.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/apache_hive.png" alt="apache_hive logo"/><br/>Apache Hive</a></td>
        <td width="25%" align=center><a href="./iceberg.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/apache_iceberg.png" alt="apache_iceberg logo"/><br/>Apache Iceberg</a></td>
        <td width="25%" align=center><a href="./kafka.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/apache_kafka.png" alt="apache_kafka logo"/><br/>Apache Kafka</a></td>
        <td width="25%" align=center><a href="./spark.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/apache_spark.png" alt="apache_spark logo"/><br/>Apache Spark</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="./aws_cli.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/cli.png" alt="cli logo"/><br/>AWS CLI</a></td>
        <td width="25%" align=center><a href="./cloudera.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/cloudera.png" alt="cloudera logo"/><br/>Cloudera</a></td>
        <td width="25%" align=center><a href="./delta.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/delta-lake.png" alt="delta lake logo"/><br/>Delta Lake</a></td>
        <td width="25%" align=center><a href="./git.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/git.png" alt="git logo"/><br/>Git</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="./dremio.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/dremio.png" alt="dremio logo"/><br/>Dremio</a></td>
        <td width="25%" align=center><a href="./duckdb.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/duckdb.png" alt="duckdb logo"/><br/>DuckDB</a></td>
        <td width="25%" align=center><a href="./glue_hive_metastore.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/glue.png" alt="glue logo"/><br/>Glue / Hive metastore</a></td>
        <td width="25%" align=center><a href="./kubeflow.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/kubeflow.png" alt="kubeflow logo"/><br/>Kubeflow</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="./presto_trino.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/trino_presto.png" alt="presto and trino logos"/><br/>Presto / Trino</a></td>
        <td width="25%" align=center><a href="./python.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/python.png" alt="python logo"/><br/>Python</a></td>
        <td width="25%" align=center><a href="./r.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/r.png" alt="r logo"/><br/>R</a></td>
        <td width="25%" align=center><a href="./vertex_ai.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/vertex_ai.png" alt="Vertex AI Logo"/><br/>Vertex AI</a></td>
    </tr>
    <tr>
        <td width="25%" align=center><a href="./huggingface_datasets.html"><img width=120 src="{{ site.baseurl }}/assets/img/logos/huggingface.png" alt="Hugging Face Logo"/><br/>HuggingFace Datasets</a></td>
        <td width="25%" align=center></td>
        <td width="25%" align=center></td>
        <td width="25%" align=center></td>
    </tr>
</table>

{: .tip}
If there is a technology not listed here that you would like to use with lakeFS, please drop by [our Slack](/slack) and we'll help you get started with it.
