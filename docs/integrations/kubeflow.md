---
layout: default
title: Kubeflow
description: Easily build reproducible data pipelines with Kubeflow and lakeFS using commits, without modifying the code or logic of your job. 
parent: Integrations
nav_order: 56
has_children: false

---
# Using lakeFS with Kubeflow pipelines
{: .no_toc }
[Kubeflow](https://www.kubeflow.org/docs/about/kubeflow/) is a project dedicated to making deployments of ML workflows on Kubernetes simple, portable and scalable.
A Kubeflow pipeline is a portable and scalable definition of an ML workflow composed of steps. Each step in the pipeline is an instance of a component represented as an instance of [ContainerOp](https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.dsl.html#kfp.dsl.ContainerOp).
  
## Table of contents
{: .no_toc .text-delta } 

1. TOC 
{:toc}  


# Add lakeFS-specific pipeline steps

To integrate lakeFS onto your Kubeflow pipeline, we will need to create Kubeflow components that perform lakeFS-specific operations.
Currently, there are two methods to create lakeFS-specific ContainerOps:
1. Implement function-based ContainerOps that use lakeFS's Python API to invoke lakeFS-specific operations.
2. Implement ContainerOps that use the `lakectl` CLI to invoke lakeFS-specific operations.

## Function-based ContainerOps with lakeFS Python API client

lakeFS comes with a [Python API client](python.md). To use it to build lakeFS-specific pipeline steps, you can write Python functions that use 
the client to invoke lakeFS-specific operations, and then use Kubeflow's mechanism to build a [function-based component](https://www.kubeflow.org/docs/components/pipelines/sdk/python-function-components/) out of it.

### Example operations
{: .no_toc }

Create new branch: A function-based ContainerOp that creates a branch called `example-branch` based on the `main` branch of `example-repo`.
   ```python
   import kfp
   from kfp import dsl
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

   # Convert the function to a lakeFS-specific pipeline step.
   create_branch_op = components.func_to_container_op(
      func=create_branch,
      packages_to_install=['lakefs_client==<lakeFS version>']) # Type in the lakeFS version your server is using
   ```

## ContainerOps with lakectl CLI

### Prerequisites
{: .no_toc }

#### Make lakectl docker image accessible by Kubeflow
{: .no_toc }

lakeFS comes with its own [native CLI client](../quickstart/lakefs_cli.md), which is also available as a [docker image](https://hub.docker.com/r/treeverse/lakectl). 
To build lakeFS-specific pipeline steps that invoke lakectl commands, Kubeflow should be able to pull the `treeverse/lakectl` docker image either from docker hub, or from your private registry. 

### Implementation
{: .no_toc }

This method implements ContainerOps, that use the `treeverse/lakectl` as their image, and run `lakectl` commands to execute the desired lakeFS-specific operation.   

For `lakectl` to work with Kubeflow, you will need to pass your lakeFS configurations as environment variables named:
* `LAKECTL_CREDENTIALS_ACCESS_KEY_ID: AKIAIOSFODNN7EXAMPLE`
* `LAKECTL_SECRET_ACCESS_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
* `LAKECTL_SERVER_ENDPOINT_URL: https://lakefs.example.com`

### Example operations
{: .no_toc }

1. Commit changes to a branch: A ContainerOp that commits uncommitted changes to `example-branch` on `example-repo`.

   ```python
   from kubernetes.client.models import V1EnvVar

   def commit_op():
      return dsl.ContainerOp(
      name='commit',
      image='treeverse/lakectl',
      arguments=['commit', 'lakefs://example-repo/example-branch', '-m', 'commit message']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='AKIAIOSFODNN7EXAMPLE')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='https://lakefs.example.com'))
   ```
2. Merge two lakeFS branches: A ContainerOp that merges `example-branch` into the `main` branch of `example-repo`.
    
   ```python
   def merge_op():
     return dsl.ContainerOp(
     name='merge',
     image='treeverse/lakectl',
     arguments=['merge', 'lakefs://example-repo/example-branch', 'lakefs://example-repo/main']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='AKIAIOSFODNN7EXAMPLE')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='https://lakefs.example.com'))
   ```

You can invoke any lakeFS operation supported by `lakectl` by implementing it as a ContainerOp. Check out the complete [CLI reference](https://docs.lakefs.io/reference/commands.html) for the list of supported operations.


**Note**
The lakeFS Kubeflow integration that uses `lakectl` is supported on lakeFS version >= v0.43.0.
{: .note }

# Add the lakeFS steps to your pipeline

Add the steps created on the previous step to your pipeline before compiling it. 

### Example pipeline
{: .no_toc }

A pipeline that implements a simple ETL, that has steps for branch creation and commits.    

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
It is recommended to store credentials as kubernetes secrets and pass them as [environment variables](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables ) to Kubeflow operations using [V1EnvVarSource](https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1EnvVarSource.md).
{: .note }