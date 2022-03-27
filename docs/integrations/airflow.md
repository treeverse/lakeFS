---
layout: default
title: Airflow
description: Easily build reproducible data pipelines with Airflow and lakeFS using commits, without modifying the code or logic of your job.
parent: Integrations
nav_order: 55
has_children: false
redirect_from: ../using/airflow.html
---

# Using lakeFS with Airflow
[Apache Airflow](https://airflow.apache.org/){: .button-clickable} is a platform to programmatically author, schedule and monitor workflows.

There are some aspects we will need to handle in order to run Airflow with lakeFS:

## Creating the lakeFS connection
For authenticating to the lakeFS server, you need to create a new [Airflow Connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html){: .button-clickable}
of type HTTP and pass it to your DAG. You can do that using the Airflow UI or the cli.
Here’s an example Airflow command that does just that:

```bash
airflow connections add conn_lakefs --conn-type=HTTP --conn-host=http://<LAKEFS_ENDPOINT> \
 --conn-extra='{"access_key_id":"<LAKEFS_ACCESS_KEY_ID>","secret_access_key":"<LAKEFS_SECRET_ACCESS_KEY>"}'
```

## Installing lakeFS Airflow package
Installing the package using `pip`:

```bash
pip install airflow-provider-lakefs
```

## Using the package
The package exposes several operations for interacting with a lakeFS server:
1. `CreateBranchOperator` creates a new lakeFS branch from the source branch (defaults to main).

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
   
Sensors are also available if you want to synchronize a running DAG with external operations:
1. `CommitSensor` waits until a commit has been applied to the branch
   
   ```python
   task_sense_commit = CommitSensor(
       repo='example-repo',
       branch='example-branch',
       task_id='sense_commit'
   )
   ```
1. `FileSensor` waits until a given file is present in a branch.

   ```python
   task_sense_file = FileSensor(
       task_id='sense_file',
       repo='example-repo',
       branch='example-branch',
       path="file/to/sense"
   )
   ```

For a DAG example that uses all the above, check out the [example DAG](https://github.com/treeverse/airflow-provider-lakeFS/blob/main/lakefs_provider/example_dags/lakefs-dag.py){: .button-clickable}
in the airflow-provider-lakeFS repository.


### Performing other operations
To perform other operations that are not yet supported by the package, you can use:

- SimpleHttpOperator to send [API requests](../reference/api.md){: .button-clickable} to lakeFS. 
- BashOperator with [lakeCTL](../quickstart/first_commit.md){: .button-clickable} commands.
For example, deleting a branch using BashOperator:
```bash
commit_extract = BashOperator(
   task_id='delete_branch',
   bash_command='lakectl branch delete lakefs://example-repo/example-branch',
   dag=dag,
)
```

**Note** lakeFS version <= v0.33.1 uses '@' (instead of '/') as separator between repository and branch.
