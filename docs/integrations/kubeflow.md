---
layout: default
title: Kubeflow
description: Easily build reproducible data pipelines with Kubeflow and lakeFS using commits, without modifying the code or logic of your job.
parent: Integrations
nav_order: 90
has_children: false

---
# Using lakeFS with Kubeflow pipelines
[Kubeflow](https://www.kubeflow.org/docs/about/kubeflow/) is a project dedicated to making deployments of ML workflows on Kubernetes simple, portable, and scalable.
A Kubeflow pipeline is a portable and scalable definition of an ML workflow composed of steps. Each step in the pipeline is an instance of a component represented as an instance of [ContainerOp](https://kubeflow-pipelines.readthedocs.io/en/stable/source/kfp.dsl.html#kfp.dsl.ContainerOp).

{% include toc.html %}


## Add pipeline steps for lakeFS operations

To integrate lakeFS into your Kubeflow pipeline, you need to create Kubeflow components that perform lakeFS operations.
Currently, there are two methods to create lakeFS ContainerOps:
1. Implement a function-based ContainerOp that uses the lakeFS Python API to invoke lakeFS operations.
1. Implement a ContainerOp that uses the `lakectl` CLI docker image to invoke lakeFS operations.

### Function-based ContainerOps

To implement a [function-based component](https://www.kubeflow.org/docs/components/pipelines/sdk/python-function-components/) that invokes lakeFS operations,
you should use the [Python OpenAPI client](python.md) lakeFS provides. See the example below that demonstrates how to make the client's package available to your ContainerOp.

#### Example operations

Create a new branch: A function-based ContainerOp that creates a branch called `example-branch` based on the `main` branch of `example-repo`.

```python
from kfp import components

def create_branch(repo_name, branch_name, source_branch):
   import lakefs_client
   from lakefs_client import models
   from lakefs_client.client import LakeFSClient

   # lakeFS credentials and endpoint
   configuration = lakefs_client.Configuration()
   configuration.username = 'AKIAIOSFODNN7EXAMPLE'
   configuration.password = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
   configuration.host = 'https://lakefs.example.com'
   client = LakeFSClient(configuration)

   client.branches.create_branch(repository=repo_name, branch_creation=models.BranchCreation(name=branch_name, source=source_branch))

# Convert the function to a lakeFS pipeline step.
create_branch_op = components.func_to_container_op(
   func=create_branch,
   packages_to_install=['lakefs_client==<lakeFS version>']) # Type in the lakeFS version you are using
```

You can invoke any lakeFS operation supported by lakeFS OpenAPI. For example, you could implement a commit and merge function-based ContainerOps.
Check out the full API [reference](https://docs.lakefs.io/reference/api.html).

### Non-function-based ContainerOps

To implement a non-function based ContainerOp, you should use the [`treeverse/lakectl`](https://hub.docker.com/r/treeverse/lakectl) docker image.
With this image, you can run [lakectl](/reference/cli.html) commands to execute the desired lakeFS operation.

For `lakectl` to work with Kubeflow, you will need to pass your lakeFS configurations as environment variables named:

* `LAKECTL_CREDENTIALS_ACCESS_KEY_ID: AKIAIOSFODNN7EXAMPLE`
* `LAKECTL_SECRET_ACCESS_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
* `LAKECTL_SERVER_ENDPOINT_URL: https://lakefs.example.com`

#### Example operations

1. Commit changes to a branch: A ContainerOp that commits uncommitted changes to `example-branch` on `example-repo`.

   ```python
   from kubernetes.client.models import V1EnvVar

   def commit_op():
      return dsl.ContainerOp(
      name='commit',
      image='treeverse/lakectl',
      arguments=['commit', 'lakefs://example-repo/example-branch', '-m', 'commit message']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='AKIAIOSFODNN7EXAMPLE')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='https://lakefs.example.com'))
   ```

1. Merge two lakeFS branches: A ContainerOp that merges `example-branch` into the `main` branch of `example-repo`.

   ```python
   def merge_op():
     return dsl.ContainerOp(
     name='merge',
     image='treeverse/lakectl',
     arguments=['merge', 'lakefs://example-repo/example-branch', 'lakefs://example-repo/main']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='AKIAIOSFODNN7EXAMPLE')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='https://lakefs.example.com'))
   ```

You can invoke any lakeFS operation supported by `lakectl` by implementing it as a ContainerOp. Check out the complete [CLI reference](../reference/cli.md) for the list of supported operations.


**Note**
The lakeFS Kubeflow integration that uses `lakectl` is supported on lakeFS version >= v0.43.0.
{: .note }

## Add the lakeFS steps to your pipeline

Add the steps created in the previous step to your pipeline before compiling it.

### Example pipeline

A pipeline that implements a simple ETL that has steps for branch creation and commits.

```python
def lakectl_pipeline():
   create_branch_task = create_branch_op('example-repo', 'example-branch', 'main') # A function-based component
   extract_task = example_extract_op()
   commit_task = commit_op()
   transform_task = example_transform_op()
   commit_task = commit_op()
   load_task = example_load_op()
```


**Note**
It's recommended to store credentials as Kubernetes secrets and pass them as [environment variables](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables ) to Kubeflow operations using [V1EnvVarSource](https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1EnvVarSource.md).
{: .note }
