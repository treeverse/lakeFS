---
layout: default
title: Kubeflow
description: Easily build reproducible data pipelines with Kubeflow and lakeFS using commits, without modifying the code or logic of your job. 
parent: Integrations
nav_order: 56
has_children: false

---


[Kubeflow](https://www.kubeflow.org/docs/about/kubeflow/) is a project dedicated to making deployments of ML workflows on Kubernetes simple, portable and scalable.

## Table of contents
{: .no_toc .text-delta }

1. TOC 
{:toc}

## Using lakeFS with Kubeflow pipelines
{: .no_toc }
A Kubeflow pipeline is a portable and scalable definition of an ML workflow composed of steps. Each step in the pipeline is an instance of a component represented as an instance of [ContainerOp](https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.dsl.html#kfp.dsl.ContainerOp).

There are some aspects we will need to handle to run Kubeflow with lakeFS:
## Install lakeFS CLI 
lakeFS comes with its own native CLI client. We will use it to build lakeFS-specific Kubeflow pipeline steps. 
To install and configure the lakeFS CLI client follow [this](https://docs.lakefs.io/quickstart/lakefs_cli.html) guide.  

## Add lakeFS-specific pipeline steps
Once you have `lakectl` installed, you can add pipeline components that invoke lakeFS operations. As mentioned above, a pipeline component is represented as a ContainerOp.
For `lakectl` to work with Kubeflow, you will need to provide your `lakectl` configurations as environment variables named:
* `LAKECTL_CREDENTIALS_ACCESS_KEY_ID: <Access key ID>`
* `LAKEFS_SECRET_ACCESS_KEY: <Secret access key>`
* `LAKECTL_SERVER_ENDPOINT_URL: <Server endpoint URL>`

### Example operations
{: .no_toc }
1. Create new branch 
    
   ```python
    import kfp
    from kfp import dsl
    from kubernetes.client.models import V1EnvVar
    
    def create_branch_op():
       return dsl.ContainerOp(
          name='create_branch',
          image='treeverse/lakectl',
          arguments=['branch', 'create', 'lakefs://example-repo/example-branch', '-s', 'lakefs://example-repo/main']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='<Access key ID>')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='<Secret access key>')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='<Server endpoint URL>'))
    ```
2. Commit changes to a branch
    
    ```python
    def commit_op():
        return dsl.ContainerOp(
           name='commit',
           image='treeverse/lakectl',
           arguments=['commit', 'lakefs://repo/example-repo', '-m', 'commit message']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='<Access key ID>')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='<Secret access key>')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='<Server endpoint URL>'))
    ```
3. Merge two lakeFS branches
    
   ```python
    def merge_op():
        return dsl.ContainerOp(
           name='merge',
           image='treeverse/lakectl',
           arguments=['merge', 'lakefs://repo/example-branch', 'lakefs://example-repo/main']).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_ACCESS_KEY_ID',value='<Access key ID>')).add_env_variable(V1EnvVar(name='LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY',value='<Secret access key>')).add_env_variable(V1EnvVar(name='LAKECTL_SERVER_ENDPOINT_URL',value='<Server endpoint URL>'))
    ```

You can invoke any lakeFS operation supported by `lakectl` by implementing it a ContainerOp. Check out the complete [CLI reference](https://docs.lakefs.io/reference/commands.html) for the list of supported operations.  

## Add the lakeFS steps to your pipeline
Add the steps created on the previous step to your pipeline before compiling it. 

### Example pipeline
{: .no_toc }
A pipeline that implements a simple ETL, that has steps for branch creation and commits.    
   ```python
    def lakectl_pipeline():
       create_branch_task = create_branch_op()
       extract_task = extract_op() # a made-up generic operation 
       commit_task = commit_op() 
       transform_task = transform_op() # a made-up generic operation
       commit_task = commit_op()
       load_task = load_op() # a made-up generic operation
   ```

## What's next?
{: .no_toc }
We are working to make the lakeFS [python client](https://docs.lakefs.io/integrations/python.html) work with Kubeflow.



**Notes**  \  
lakeFS version <= v0.33.1 uses '@' (instead of '/') as separator between repository and branch.  \
The lakeFS Kubeflow integration is supported on lakeFS version >= v0.43.0.
{: .note}