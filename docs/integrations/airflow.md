---
layout: default
title: Apache Airflow
description: Easily build reproducible data pipelines with Apache Airflow and lakeFS using commits, without modifying the code or logic of your job.
parent: Integrations
has_children: false
redirect_from: /using/airflow.html
---

# Using lakeFS with Apache Airflow

[Apache Airflow](https://airflow.apache.org/) is a platform that allows users to programmatically author, schedule, and monitor workflows.

To run Airflow with lakeFS, you need to follow a few steps.

## Create a lakeFS connection on Airflow

To access the lakeFS server and authenticate with it, create a new [Airflow
Connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
of type HTTP and add it to your DAG.  You can do that using the Airflow UI
or the CLI. Here’s an example Airflow command that does just that:

```bash
airflow connections add conn_lakefs --conn-type=HTTP --conn-host=http://<LAKEFS_ENDPOINT> \
    --conn-extra='{"access_key_id":"<LAKEFS_ACCESS_KEY_ID>","secret_access_key":"<LAKEFS_SECRET_ACCESS_KEY>"}'
```

## Install the lakeFS Airflow package

You can use `pip` to install [the package](https://pypi.org/project/airflow-provider-lakefs/)

```bash
pip install airflow-provider-lakefs
```

## Use the package

### Operators

The package exposes several operations to interact with a lakeFS server:

1. `CreateBranchOperator` creates a new lakeFS branch from the source branch (`main` by default).

   ```python
   task_create_branch = CreateBranchOperator(
      task_id='create_branch',
      repo='example-repo',
      branch='example-branch',
      source_branch='main'
   )
   ```
1. `CommitOperator` commits uncommitted changes to a branch.

   ```python
   task_commit = CommitOperator(
       task_id='commit',
       repo='example-repo',
       branch='example-branch',
       msg='committing to lakeFS using airflow!',
       metadata={'committed_from": "airflow-operator'}
   )
   ```
1. `MergeOperator` merges 2 lakeFS branches.

   ```python
   task_merge = MergeOperator(
     task_id='merge_branches',
     source_ref='example-branch',
     destination_branch='main',
     msg='merging job outputs',
     metadata={'committer': 'airflow-operator'}
   )
   ```

### Sensors

Sensors are also available that allow synchronizing a running DAG with external operations:

1. `CommitSensor` waits until a commit has been applied to the branch
   
   ```python
   task_sense_commit = CommitSensor(
       repo='example-repo',
       branch='example-branch',
       task_id='sense_commit'
   )
   ```
1. `FileSensor` waits until a given file is present on a branch.

   ```python
   task_sense_file = FileSensor(
       task_id='sense_file',
       repo='example-repo',
       branch='example-branch',
       path="file/to/sense"
   )
   ```

### Example

This [example DAG](https://github.com/treeverse/airflow-provider-lakeFS/blob/main/lakefs_provider/example_dags/lakefs-dag.py)
in the airflow-provider-lakeFS repository shows how to use all of these.

### Performing other operations

Sometimes an operator might not be supported by airflow-provider-lakeFS yet. You can access lakeFS directly by using:

- SimpleHttpOperator to send [API requests](../reference/api.md) to lakeFS. 
- BashOperator with [lakectl](/reference/cli.html) commands.
For example, deleting a branch using BashOperator:
```bash
commit_extract = BashOperator(
   task_id='delete_branch',
   bash_command='lakectl branch delete lakefs://example-repo/example-branch',
   dag=dag,
)
```

**Note** lakeFS version <= v0.33.1 uses '@' (instead of '/') as separator between repository and branch.
