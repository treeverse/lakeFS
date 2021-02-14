---
layout: default
title: Airflow
description: Easily build reproducible data pipelines with Airflow and lakeFS using commits, without modifying the code or logic of your job.
parent: Using lakeFS with...
nav_order: 10
has_children: false
---

# Using lakeFS with Airflow


There are two aspects we will need to handle in order to run Airflow with lakeFS:

## Access and Insert data through lakeFS
Since lakeFS supports AWS S3 API, it works seamlessly with all operators that work on top of S3 (such as SparkSubmitOperator, S3FileTransormOperator, etc.)

All we need to do is set lakeFS as the endpoint-url and use our lakeFS credentials instead of our S3 credentials and that’s about it.

We could then run tasks on lakeFS using the lakeFS path convention 

```s3://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT```

The lakeFS docs contain explanations and examples on how to use lakeFS with [AWS CLI](aws_cli.md), [Spark](spark.md), [Presto](presto.md) and many more. 

## Run lakeFS commands such as creating branches, committing, merging, etc.
We currently have two options to run lakeFS commands with Airflow
Using the SimpleHttpOperator to send [API requests](../reference/api.md) to lakeFS. Or we could use the bashOperator with [lakeCTL](../quickstart/lakefs_cli.md) commands.

For example, a commit task using the bashOperator:

```python

commit_extract = BashOperator(
       task_id='commit_extract',
       bash_command='lakectl commit lakefs://example_repo@example_dag_branch -m "extract data"',
       dag=dag,
       )
```




