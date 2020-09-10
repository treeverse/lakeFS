---
layout: default
title: Python
parent: Using lakeFS with...
nav_order: 5
has_children: false
---

# Calling the lakeFS API from Python
{: .no_toc }

The [lakeFS API](../reference/api.md){: target="_blank" } is OpenAPI 2.0 compliant, allowing the dynamic generation of clients from multiple languages.

For Python, this example uses [Bravado](https://github.com/Yelp/bravado){: target="_blank" }
which generates a dynamic client at runtime, from an OpenAPI definition served by a lakeFS server.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Installing Python Dependencies

A complete installation guide is available in the [bravado GitHub repository](https://github.com/Yelp/bravado){: target="_blank" }.
For our example, we'll simply install it with pip:

```shell
pip install bravado==10.6.2
```

At the time of writing this guide, the current stable bravado release is `10.6.2`.


## Locating the OpenAPI definition for our installation

Assuming we have a lakeFS server deployed at `https://lakefs.example.com`, the OpenAPI definition URL will be `https://lakefs.example.com/swagger.json`.
For local development, we can use `http://localhost:8000/swagger.json`.

## Generating a Python client with Bravado

Once we have Bravado installed, and a URL of an OpenAPI definition we can generate a client:


```python
from bravado.requests_client import RequestsClient
from bravado.client import SwaggerClient

http_client = RequestsClient()
http_client.set_basic_auth('localhost:8000', 'AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
client = SwaggerClient.from_url(
    'http://localhost:8000/swagger.json',
    http_client=http_client,
    config={"validate_swagger_spec": False})

``` 

## Using the generated client

Now that we have a client object, we can use it to interact with the API.

### Listing and creating repositories

```python
client.repositories.createRepository(repository={
    'id': 'example-repo',
    'storage_namespace': 's3://storage-bucket/repos/example-repo',
    'default_branch':'main'
}).result()
# output:
# repository(creation_date=1599560048, default_branch='main', id='example-repo', storage_namespace='s3://storage-bucket/repos/example-repo')
```

### Creating a branch, uploading files, committing changes

List current branches:

```python
client.branches.listBranches(repository='test-repo').result()
# output:
# {'pagination': pagination(has_more=False, max_per_page=1000, next_offset=None, results=1), 'results': ['main']}
```

Create a new branch:

```python
client.branches.createBranch(repository='test-repo', branch={'name': 'experiment-aggregations1', 'source': 'main'}).result()
# output:
# '~EiRd5nyjm8kWLDHesLTsywmd1MNW5hB3ApQi4'
```

Let's list again, to see our newly created branch:

```python
client.branches.listBranches(repository='test-repo').result()
# output:
# {'pagination': pagination(has_more=False, max_per_page=1000, next_offset=None, results=2),
#    'results': ['experiment-aggregations1', 'main']}
```

Great. Now, let's upload a file into our new branch:

```python
with open('file.csv', 'rb') as file_handle:
    client.objects.uploadObject(
        repository='test-repo',
        branch='experiment-aggregations1',
        path='path/to/file.csv',
        content=file_handle
    ).result()
# output:
# object_stats(checksum='319ccf050a10a87ba20e00a64c6d738e', mtime=1599563388, path='path/to/file.csv', path_type='object', size_bytes=727)
```

Diffing a single branch will show all uncommitted changes on that branch:

```python
client.branches.diffBranch(repository='test-repo', branch='experiment-aggregations1').result()
# output:
# {'results': [diff(path='path/to/file.csv', path_type='object', type='added')]}
```

As expected, our change appears here. Let's commit it, and attach some arbitrary metadata:

```python
client.commits.commit(
    repository='test-repo',
    branch='experiment-aggregations1',
    commit={
        'message': 'Added a CSV file!',
        'metadata': {
            'using': 'python_api'
        }
    }).result()
# output:
# commit(committer='jane.doe', creation_date=1599563809, id='~EiRd5nyjm8kWLDHesLTsywmd1MNW5hB3ApQnW',
#     message='Added a CSV file!', metadata={'using': 
# 'python_api'}, parents=['~EiRd5nyjm8kWLDHesLTsywmd1MNW5hB3ApQnU'])
```

Diffing again, this time there should be no uncommitted branches:

```python
client.branches.diffBranch(repository='test-repo', branch='experiment-aggregations1').result()
# output:
# {'results': []}
```

### Merging changes from a branch into master 

Let's diff between our branch and the main branch:

```python
client.refs.diffRefs(repository='test-repo', leftRef='experiment-aggregations1', rightRef='main').result()
# output:
# {'results': [diff(path='path/to/file.csv', path_type='object', type='added')]}
```

Looks like we have a change. Let's merge it:

```python
client.refs.mergeIntoBranch(repository='test-repo', sourceRef='experiment-aggregations1', destinationRef='main').result()
# output:
# {'results': [merge_result(path='path/to/object', path_type='object', type='added')]}
```

Let's diff again - there should be no changes as all changes are on our main branch already:

```python
client.refs.diffRefs(repository='test-repo', leftRef='experiment-aggregations1', rightRef='main').result()
# output:
# {'results': []}
```

## Full API reference

For a full reference of the lakeFS API, see [lakeFS API](../reference/api.md)
