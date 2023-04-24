---
layout: default
title: Python
description: Use Python to interact with your objects on lakeFS
parent: Integrations
has_children: false
redirect_from: 
  - /using/python.html
  - /using/boto.html
  - /integrations/boto.html
---

{% include toc.html %}

## Boto vs. lakeFS SDK

To interact with lakeFS from Python:
* [Use Boto](#using-boto) to perform **object operations** through the lakeFS S3 gateway.
* [Use the lakeFS SDK](#using-the-lakefs-sdk) to perform **versioning** and other lakeFS-specific operations.

## Using the lakeFS SDK

### Installing

Install the Python client using pip:


```shell
pip install 'lakefs_client==<lakeFS version>'
```

### Initializing

Here's how to instantiate a client:

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

### Usage Examples

Now that you have a client object, you can use it to interact with the API.

#### Creating a repository
{: .no_toc }

```python
repo = models.RepositoryCreation(name='example-repo', storage_namespace='s3://storage-bucket/repos/example-repo', default_branch='main')
client.repositories.create_repository(repo)
# output:
# {'creation_date': 1617532175,
#  'default_branch': 'main',
#  'id': 'example-repo',
#  'storage_namespace': 's3://storage-bucket/repos/example-repo'}
```

#### Creating a branch, uploading files, committing changes
{: .no_toc }

List the repository branches:

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

List again to see your newly created branch:

```python
client.branches.list_branches('example-repo').results
# output:
# [{'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'experiment-aggregations1'}, {'commit_id': 'cdd673a4c5f42d33acdf3505ecce08e4d839775485990d231507f586ebe97656', 'id': 'main'}]
```

Great. Now, let's upload a file into your new branch:

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

Diffing a single branch will show all the uncommitted changes on that branch:

```python
client.branches.diff_branch(repository='example-repo', branch='experiment-aggregations1').results
# output:
# [{'path': 'path/to/file.csv', 'path_type': 'object', 'type': 'added'}]
```

As expected, our change appears here. Let's commit it and attach some arbitrary metadata:

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

#### Merging changes from a branch into main 
{: .no_toc }

Let's diff between your branch and the main branch:

```python
client.refs.diff_refs(repository='example-repo', left_ref='main', right_ref='experiment-aggregations1').results
# output:
# [{'path': 'path/to/file.csv', 'path_type': 'object', 'type': 'added'}]

```

Looks like you have a change. Let's merge it:

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

### Python Client documentation
{: .no_toc }

For the documentation of lakeFS’s Python package, see [https://pydocs.lakefs.io](https://pydocs.lakefs.io)


### Full API reference
{: .no_toc }

For a full reference of the lakeFS API, see [lakeFS API](../reference/api.md)

## Using Boto

💡 To use Boto with lakeFS alongside S3, check out [Boto S3 Router](https://github.com/treeverse/boto-s3-router){:target="_blank"}. It will route
requests to either S3 or lakeFS according to the provided bucket name.
{: .note }

lakeFS exposes an S3-compatible API, so you can use Boto to interact with your objects on lakeFS.

### Initializing

Create a Boto3 S3 client with your lakeFS endpoint and key-pair:
```python
import boto3
s3 = boto3.client('s3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
```

The client is now configured to operate on your lakeFS installation.

### Usage Examples

#### Put an object into lakeFS
{: .no_toc }

Use a branch name and a path to put an object in lakeFS:

```python
with open('/local/path/to/file_0', 'rb') as f:
    s3.put_object(Body=f, Bucket='example-repo', Key='main/example-file.parquet')
```

You can now commit this change using the lakeFS UI or CLI.

#### List objects
{: .no_toc }

List the branch objects starting with a prefix:
 
```python
list_resp = s3.list_objects_v2(Bucket='example-repo', Prefix='main/example-prefix')
for obj in list_resp['Contents']:
    print(obj['Key'])
```

Or, use a lakeFS commit ID to list objects for a specific commit:
 
```python
list_resp = s3.list_objects_v2(Bucket='example-repo', Prefix='c7a632d74f/example-prefix')
for obj in list_resp['Contents']:
    print(obj['Key'])
```

#### Get object metadata
{: .no_toc }

Get object metadata using branch and path:
```python
s3.head_object(Bucket='example-repo', Key='main/example-file.parquet')
# output:
# {'ResponseMetadata': {'RequestId': '72A9EBD1210E90FA',
#  'HostId': '',
#  'HTTPStatusCode': 200,
#  'HTTPHeaders': {'accept-ranges': 'bytes',
#   'content-length': '1024',
#   'etag': '"2398bc5880e535c61f7624ad6f138d62"',
#   'last-modified': 'Sun, 24 May 2020 10:42:24 GMT',
#   'x-amz-request-id': '72A9EBD1210E90FA',
#   'date': 'Sun, 24 May 2020 10:45:42 GMT'},
#  'RetryAttempts': 0},
# 'AcceptRanges': 'bytes',
# 'LastModified': datetime.datetime(2020, 5, 24, 10, 42, 24, tzinfo=tzutc()),
# 'ContentLength': 1024,
# 'ETag': '"2398bc5880e535c61f7624ad6f138d62"',
# 'Metadata': {}}
``` 
