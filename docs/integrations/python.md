---
layout: default
title: Python
description: The lakeFS API is OpenAPI 3.0 compliant, allowing the generation of clients from Python and multiple other languages
parent: Integrations
nav_order: 30
has_children: false
redirect_from: ../using/python.html
---

# Calling the lakeFS API from Python
{: .no_toc }

The [lakeFS API](../reference/api.md){: target="_blank" .button-clickable} is OpenAPI 3.0 compliant, allowing the generation of clients from multiple languages or directly accessed by any HTTP client.

For Python, this example uses [lakeFS's python package](https://pypi.org/project/lakefs-client/){: target="_blank" .button-clickable}.
The lakefs-client pacakge was created by [OpenAPI Generator](https://openapi-generator.tech){: target="_blank" .button-clickable} using our OpenAPI definition served by a lakeFS server.

{% include toc.html %}

## Install lakeFS Python Client API

Install the Python client using pip:


```shell
pip install 'lakefs_client==<lakeFS version>'
```

The package is available from version >= 0.34.0.


## Working with the Client API

How to instantiate a client:

```python
import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient

# lakeFS credentials and endpoint
configuration = lakefs_client.Configuration()
configuration.username = 'AKIAIOSFODNN7EXAMPLE'
configuration.password = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
configuration.host = 'http://localhost:8000'

client = LakeFSClient(configuration)
``` 

## Using the generated client

Now that we have a client object, we can use it to interact with the API.

### Creating a repository

```python
repo = models.RepositoryCreation(name='example-repo', storage_namespace='s3://storage-bucket/repos/example-repo', default_branch='main')
client.repositories.create_repository(repo)
# output:
# {'creation_date': 1617532175,
#  'default_branch': 'main',
#  'id': 'example-repo',
#  'storage_namespace': 's3://storage-bucket/repos/example-repo'}
```

### Creating a branch, uploading files, committing changes

List repository branches:

```python
client.branches.list_branches('example-repo')
# output:
# [{'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'main'}]
```

Create a new branch:

```python
client.branches.create_branch(repository='example-repo', branch_creation=models.BranchCreation(name='experiment-aggregations1', source='main'))
# output:
# 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656'
```

Let's list again, to see our newly created branch:

```python
client.branches.list_branches('example-repo').results
# output:
# [{'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'experiment-aggregations1'}, {'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'main'}]
```

Great. Now, let's upload a file into our new branch:

```python
with open('file.csv', 'rb') as f:
    client.objects.upload_object(repository='example-repo', branch='experiment-aggregations1', path='path/to/file.csv', content=f)
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
client.branches.diff_branch(repository='example-repo', branch='experiment-aggregations1').results
# output:
# [{'path': 'path/to/file.csv', 'path_type': 'object', 'type': 'added'}]
```

As expected, our change appears here. Let's commit it, and attach some arbitrary metadata:

```python
client.commits.commit(
    repository='example-repo',
    branch='experiment-aggregations1',
    commit_creation=models.CommitCreation(message='Added a CSV file!', metadata={'using': 'python_api'}))
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
client.branches.diff_branch(repository='example-repo', branch='experiment-aggregations1').results
# output:
# []
```

### Merging changes from a branch into main 

Let's diff between our branch and the main branch:

```python
client.refs.diff_refs(repository='example-repo', left_ref='main', right_ref='experiment-aggregations1').results
# output:
# [{'path': 'path/to/file.csv', 'path_type': 'object', 'type': 'added'}]

```

Looks like we have a change. Let's merge it:

```python
client.refs.merge_into_branch(repository='example-repo', source_ref='experiment-aggregations1', destination_branch='main')
# output:
# {'reference': 'd0414a3311a8c1cef1ef355d6aca40db72abe545e216648fe853e25db788fa2e',
#  'summary': {'added': 1, 'changed': 0, 'conflict': 0, 'removed': 0}}
```

Let's diff again - there should be no changes as all changes are on our main branch already:

```python
client.refs.diff_refs(repository='example-repo', left_ref='main', right_ref='experiment-aggregations1').results
# output:
# []
```

## Python client documentation

For the documentation of lakeFS’s python package, see [https://pydocs.lakefs.io](https://pydocs.lakefs.io){: .button-clickable}


## Full API reference

For a full reference of the lakeFS API, see [lakeFS API](../reference/api.md){: .button-clickable}
