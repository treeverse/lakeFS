---
layout: default
title: Python
description: The lakeFS API is OpenAPI 3.0 compliant, allowing the generation of clients from Python and multiple other languages
parent: Using lakeFS with...
nav_order: 30
has_children: false
---

# Calling the lakeFS API from Python
{: .no_toc }

The [lakeFS API](../reference/api.md){: target="_blank" } is OpenAPI 3.0 compliant, allowing the generation of clients from multiple languages.

For Python, this example uses [Swagger Codegen](https://swagger.io/tools/swagger-codegen/){: target="_blank" }
which can generates Python client code for the OpenAPI definition served by a lakeFS server.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Locating the OpenAPI definition for our installation

Assuming we have a lakeFS server deployed at `https://lakefs.example.com`, the OpenAPI definition URL will be `https://lakefs.example.com/swagger.json`.
For local development, we can use `http://localhost:8000/swagger.json`.

## Generating a Python client

A complete installation guide for Swagger Codegen is available in the [swagger-codegen GitHub repository](https://github.com/swagger-api/swagger-codegen/tree/3.0.0){: target="_blank" }.
we'll simply use Docker.

```shell
docker run -v $PWD:/out --rm -it swaggerapi/swagger-codegen-cli-v3 generate -l python -i http://host.docker.internal:8000/swagger.json -o /out/python
```

Alternatively, download lakeFS `swagger.json`, place it under the current working directory, and use the following:

```shell
docker run -v $PWD:/out --rm -it swaggerapi/swagger-codegen-cli-v3 generate -l python -i /out/swagger.json -o /out
```

## Install Client Requirements

To install the dependencies using pip:

```shell
pip install -r requirements.txt
```

How to instantiate a client:

```python
import time
import swagger_client
from swagger_client.rest import ApiException
from swagger_client import models

configuration = swagger_client.Configuration()
configuration.username = 'AKIAIOSFODNN7EXAMPLE'
configuration.password = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
configuration.host = 'http://localhost:8000/api/v1'

api_client = swagger_client.ApiClient(configuration)
``` 

## Using the generated client

Now that we have a client object, we can use it to interact with the API.

### Creating a repository

```python
repositories_api = swagger_client.RepositoriesApi(api_client)
repository_creation = models.RepositoryCreation(name="example-repo", storage_namespace="s3://storage-bucket/repos/example-repo", default_branch="main")
repositories_api.create_repository(repository_creation)
# output:
# {'creation_date': 1617532175,
#  'default_branch': 'main',
#  'id': 'example-repo',
#  'storage_namespace': 's3://storage-bucket/repos/example-repo'}
```

### Creating a branch, uploading files, committing changes

List current branches:

```python
branches_api = swagger_client.BranchesApi(api_client)
branches_api.list_branches('example-repo')
# output:
# [{'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'main'}]
```

Create a new branch:

```python
branches_api.create_branch(models.BranchCreation(name='experiment-aggregations1', source='main'), 'example-repo')
# output:
# 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656'
```

Let's list again, to see our newly created branch:

```python
branches_api.list_branches('example-repo').results
# output:
# [{'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'experiment-aggregations1'}, {'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'main'}]
```

Great. Now, let's upload a file into our new branch:

```python
import os

objects_api = swagger_client.ObjectsApi(api_client)
objects_api.upload_object(repository='example-repo', branch='experiment-aggregations1', path='path/to/file.csv', content='file.csv')
# output:
# {'checksum': '0d3b39380e2500a0f60fb3c09796fdba',
#  'mtime': 1617534834,
#  'path': 'path/to/file.csv',
#  'path_type': 'object',
#  'physical_address': 'local://example-repo/1865650a296c42e28183ad08e9b068a3',
#  'size_bytes': 18}
```

Diffing a single branch will show all uncommitted changes on that branch:

```python
branches_api.diff_branch(repository='example-repo', branch='experiment-aggregations1').results
# output:
# [{'path': 'path/to/file.csv', 'path_type': 'object', 'type': 'added'}]
```

As expected, our change appears here. Let's commit it, and attach some arbitrary metadata:

```python
commits_api = swagger_client.CommitsApi(api_client)
commits_api.commit(
    repository='example-repo',
    branch='experiment-aggregations1',
    body=models.CommitCreation(message='Added a CSV file!', metadata={'using': 'python_api'}))
# output:
# {'committer': 'barak',
#  'creation_date': 1617535120,
#  'id': 'e80899a5709509c2daf797c69a6118be14733099f5928c14d6b65c9ac2ac841b',
#  'message': 'Added a CSV file!',
#  'meta_range_id': '',
#  'metadata': {'using': 'python_api'},
#  'parents': ['cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656']}
```

Diffing again, this time there should be no uncommitted files:

```python
branches_api.diff_branch(repository='example-repo', branch='experiment-aggregations1').results
# output:
# []
```

### Merging changes from a branch into master 

Let's diff between our branch and the main branch:

```python
refs_api = swagger_client.RefsApi(api_client)
refs_api.diff_refs(repository='example-repo', left_ref='experiment-aggregations1', right_ref='main').results
# output:
# [{'path': 'path/to/file.csv', 'path_type': 'object', 'type': 'added'}]

```

Looks like we have a change. Let's merge it:

```python
refs_api.merge_into_branch(repository='example-repo', source_ref='experiment-aggregations1', destination_branch='main')
# output:
# {'reference': 'd0414a3311a8c1cef1ef355d6aca40db72abe545e216648fe853e25db788fa2e',
#  'summary': {'added': 1, 'changed': 0, 'conflict': 0, 'removed': 0}}
```

Let's diff again - there should be no changes as all changes are on our main branch already:

```python
refs_api.diff_refs(repository='example-repo', left_ref='experiment-aggregations1', right_ref='main').results
# output:
# []
```

## Full API reference

For a full reference of the lakeFS API, see [lakeFS API](../reference/api.md)
