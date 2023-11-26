---
title: Python
description: Use Python to interact with your objects on lakeFS
parent: Integrations
redirect_from: 
  - /using/python.html
  - /using/boto.html
  - /integrations/boto.html
---

# Use Python to interact with your objects on lakeFS

{% include toc_2-3.html %}


**Improved Python SDK**  <span class="badge mr-1">New</span><span class="badge">beta</span>  
Check-out the newly released [Python SDK library](https://pydocs-lakefs.lakefs.io/) - providing simpler interface and improved user experience!  
*OR* continue reading for the current Python SDK.
{: .note }

There are two primary ways to work with lakeFS from Python: 

* [Use Boto](#using-boto) to perform **object operations** through the lakeFS S3 gateway.
* [Use the lakeFS SDK](#using-the-lakefs-sdk) to perform **versioning** and other lakeFS-specific operations.

## Using the lakeFS SDK

### Installing

Install the Python client using pip:


```shell
pip install 'lakefs_sdk~=1.0'
```

### Initializing

Here's how to instantiate a client:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

configuration = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)
client = LakeFSClient(configuration)
``` 

For testing SSL endpoints you may wish to use a self-signed certificate.  If you do this and receive an `SSL: CERTIFICATE_VERIFY_FAILED` error message you might add the following configuration to your client:

```python
configuration.verify_ssl = False
```

{: .warning }
This setting allows well-known "man-in-the-middle",
impersonation, and credential stealing attacks.  Never use this in any
production setting.

Optionally, to enable communication via proxies, simply set the proxy configuration:

```python
configuration.ssl_ca_cert = <path to a file of concatenated CA certificates in PEM format> # Set this to customize the certificate file to verify the peer
configuration.proxy = <proxy server URL>
``` 

### Usage Examples

Now that you have a client object, you can use it to interact with the API.


To shorten the code example, let's create a helper function that will iterate over all results of a paginated API:

```python
def pagination_helper(page_fetcher, **kwargs):
    """Helper function to iterate over paginated results"""
    while True:
        resp = page_fetcher(**kwargs)
        yield from resp.results
        if not resp.pagination.has_more:
            break
        kwargs['after'] = resp.pagination.next_offset
```

#### Creating a repository

```python
import lakefs_sdk
from lakefs_sdk.models import RepositoryCreation
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.repositories_api.create_repository(
    RepositoryCreation(
        name="example-repo",
        storage_namespace="s3://storage-bucket/repos/example-repo",
    )
)
print(resp)
```

#### Output
```
id='example-repo' creation_date=1697815536 default_branch='main' storage_namespace='s3://storage-bucket/repos/example-repo'
```

#### List repositories


```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client and pagination_helper ...

print("Listing repositories:")
for repo in pagination_helper(client.repositories_api.list_repositories):
    print(repo)

```

#### Output
```
Listing repositories:
id='example-repo' creation_date=1697815536 default_branch='main' storage_namespace='s3://storage-bucket/repos/example-repo'
```

#### Creating a branch

```python
import lakefs_sdk
from lakefs_sdk.models import BranchCreation
from lakefs_sdk.client import LakeFSClient

# ... client ...

ref1 = client.branches_api.create_branch('example-repo', BranchCreation(name='experiment1', source='main'))
print("experiment1 ref:", ref1)

ref2 = client.branches_api.create_branch('example-repo', BranchCreation(name='experiment2', source='main'))
print("experiment2 ref:", ref2)
```

#### Output
```
experiment1 ref: 7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859
experiment2 ref: 7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859
```

### List branches

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# .. client and pagination_helper

for branch in pagination_helper(client.branches_api.list_branches, repository='example-repo'):
    print(branch)

```

#### Output
```
id='experiment1' commit_id='7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859'
id='experiment2' commit_id='7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859'
id='main' commit_id='7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859'
```

### Upload

Great. Now, let's upload some data:
Generate "sample_data.csv" or use your own data

```python
import csv

sample_data = [
    [1, "Alice", "alice@example.com"],
    [2, "Bob", "bob@example.com"],
    [3, "Carol", "carol@example.com"],
]

with open("sample_data.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["ID", "Name", "Email"])
    for row in sample_data:
        writer.writerow(row)
```

Upload the data file by passing the filename as content:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.objects_api.upload_object(repository="example-repo", branch="experiment1", path="csv/sample_data.csv", content="sample_data.csv")
print(resp)
```

#### Output
```
path='csv/sample_data.csv' path_type='object' physical_address='s3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfk4fnl531fa6k90pg' physical_address_expiry=None checksum='b6b6a1a17ff85291376ae6a5d7fa69d0' size_bytes=92 mtime=1697839635 metadata=None content_type='text/csv'
```

We can also upload content a bytes:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.objects_api.upload_object(repository="example-repo", branch="experiment1", path="raw/file1.data", content=b"Hello Object World")
print(resp)
```

#### Output
```
path='rawv/file1.data' path_type='object' physical_address='s3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfltvnl531fa6k90q0' physical_address_expiry=None checksum='0ef432f8eb0305f730b0c57bbd7a6b08' size_bytes=18 mtime=1697839863 metadata=None content_type='application/octet-stream
```

### Uncommitted changes

Diffing a single branch will show all the uncommitted changes on that branch:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client and pagination_helper ...

for diff in pagination_helper(client.branches_api.diff_branch, repository='example-repo', branch='experiment1'):
    print(diff)

```

#### Output

```
type='added' path='csv/sample_data.csv' path_type='object' size_bytes=92
type='added' path='raw/file1.data' path_type='object' size_bytes=18
```

As expected, our change appears here. Let's commit it and attach some arbitrary metadata:

```python
import lakefs_sdk
from lakefs_sdk.models import CommitCreation
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.commits_api.commit(
    repository='example-repo',
    branch='experiment1',
    commit_creation=CommitCreation(message='Add some data!', metadata={'using': 'python_api'})
)
print(resp)
```

#### Output
```
id='d51b2428106921fcb893813b1eb668b46284067bb5264d89ed409ccb95676e3d' parents=['7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859'] committer='barak' message='Add some data!' creation_date=1697884139 meta_range_id='' metadata={'using': 'python_api'}
```

Calling diff again on the same branch, this time there should be no uncommitted files:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.branches_api.diff_branch(repository='example-repo', branch='experiment1')
print(resp)
```

#### Output
```
pagination=Pagination(has_more=False, next_offset='', results=0, max_per_page=1000) results=[]
```

#### Merging changes from a branch into main 

Let's diff between your branch and the main branch:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client and pagination_helper ...

for diff in pagination_helper(client.refs_api.diff_refs, repository='example-repo', left_ref='main', right_ref='experiment1'):
        print(diff)

```

#### Output
```
type='added' path='csv/sample_data.csv' path_type='object' size_bytes=92
type='added' path='raw/file1.data' path_type='object' size_bytes=18
```

Looks like you have a change. Let's merge it:

```python
client.refs_api.merge_into_branch(repository='example-repo', source_ref='experiment-aggregations1', destination_branch='main')
# output:
# {'reference': 'd0414a3311a8c1cef1ef355d6aca40db72abe545e216648fe853e25db788fa2e',
#  'summary': {'added': 1, 'changed': 0, 'conflict': 0, 'removed': 0}}
```

Let's diff again - there should be no changes as all changes are on our main branch already:

```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.refs_api.merge_into_branch(
    repository='example-repo',
    source_ref='experiment1',
    destination_branch='main')
print(resp)
```

#### Output
```
reference='a3ea99167a25748cf1d33ba284bda9c1400a8acfae8477562032d2b2435fd37b'
```

### Read data from main branch

```python
import csv
from io import StringIO
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# ... client ...

resp = client.objects_api.get_object(
    repository='example-repo',
    ref='main',
    path='csv/sample_data.csv')

data = StringIO(resp.decode('utf-8'))
for row in csv.reader(data):
    print(row)
```

#### Output
```
['ID', 'Name', 'Email']
['1', 'Alice', 'alice@example.com']
['2', 'Bob', 'bob@example.com']
['3', 'Carol', 'carol@example.com']
```

### Python Client documentation

For the documentation of lakeFS’s Python package, see [https://pydocs-sdk.lakefs.io](https://pydocs-sdk.lakefs.io)


### Full API reference

For a full reference of the lakeFS API, see [lakeFS API]({% link reference/api.md %})

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

Use a branch name and a path to put an object in lakeFS:

```python
with open('/local/path/to/file_0', 'rb') as f:
    s3.put_object(Body=f, Bucket='example-repo', Key='main/example-file.parquet')
```

You can now commit this change using the lakeFS UI or CLI.

#### List objects

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

