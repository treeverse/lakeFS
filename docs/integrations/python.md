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


**High Level Python SDK**  <span class="badge mr-1">New</span>  
We've just released a new High Level Python SDK library, and we're super excited to tell you about it! Continue reading to get the
full story!  
Though our previous SDK client is still supported and maintained, we highly recommend using the new High Level SDK.  
**For previous Python SDKs follow these links:**  
[lakefs-sdk](https://pydocs-sdk.lakefs.io)  
[legacy-sdk](https://pydocs.lakefs.io) (Depracated)
{: .note }

There are two primary ways to work with lakeFS from Python: 

* [Use Boto](#using-boto) to perform **object operations** through the **lakeFS S3 gateway**.
* [Use the High Level lakeFS SDK](#using-the-lakefs-sdk) to perform **object operations**, **versioning** and other **lakeFS-specific operations**.

## Using the lakeFS SDK

### Installing

Install the Python client using pip:


```shell
pip install lakefs
```

### Initializing

The High Level SDK by default will try to collect authentication parameters from the environment and attempt to create a default client.
When working in an environment where **lakectl** is configured it is not necessary to instantiate a lakeFS client or provide it for creating the lakeFS objects.
In case no authentication parameters exist, it is also possible to explicitly create a lakeFS client

Here's how to instantiate a client:

```python
from lakefs.client import Client

clt = Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)
```

You can use TLS with a CA that is not trusted on the host by configuring the
client with a CA cert bundle file.  It should contain concatenated CA
certificates in PEM format:
```python
clt = Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    # Customize the CA certificates used to verify the peer.
    ssl_ca_cert="path/to/concatenated_CA_certificates.PEM",
)
```

For testing SSL endpoints you may wish to use a self-signed certificate.  If you do this and receive an `SSL: CERTIFICATE_VERIFY_FAILED` error message you might add the following configuration to your client:

```python
clt = Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    verify_ssl=False,
)
```

{: .warning }
This setting allows well-known "man-in-the-middle",
impersonation, and credential stealing attacks.  Never use this in any
production setting.

Optionally, to enable communication via proxies, add a proxy configuration:

```python
clt = Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    ssl_ca_cert="(if needed)",
    proxy="<proxy server URL>",
)
```

### Usage Examples

Lets see how we can interact with lakeFS using the High Level SDK.

#### Creating a repository

```python
import lakefs

repo = lakefs.repository("example-repo").create(storage_namespace="s3://storage-bucket/repos/example-repo")
print(repo)
```

If using an explicit client, create the Repository object and pass the client to it (note the changed syntax).

```python
import lakefs
from lakefs.client import Client

clt = Client(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
)

repo = lakefs.Repository("example-repo", client=clt).create(storage_namespace="s3://storage-bucket/repos/example-repo")
print(repo)
```

#### Output
```
{id: 'example-repo', creation_date: 1697815536, default_branch: 'main', storage_namespace: 's3://storage-bucket/repos/example-repo'}
```

#### List repositories


```python
import lakefs

print("Listing repositories:")
for repo in lakefs.repositories():
    print(repo)

```

#### Output
```
Listing repositories:
{id: 'example-repo', creation_date: 1697815536, default_branch: 'main', storage_namespace: 's3://storage-bucket/repos/example-repo'}
```

#### Creating a branch

```python
import lakefs

branch1 = lakefs.repository("example-repo").branch("experiment1").create(source_reference_id="main")
print("experiment1 ref:", branch1.get_commit().id)

branch1 = lakefs.repository("example-repo").branch("experiment2").create(source_reference_id="main")
print("experiment2 ref:", branch2.get_commit().id)
```

#### Output
```
experiment1 ref: 7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859
experiment2 ref: 7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859
```

### List branches

```python
import lakefs

for branch in lakefs.repository("example-repo").branches():
    print(branch)

```

#### Output
```
experiment1
experiment2
main
```

## IO

Great, now lets see some IO operations in action!  
The new High Level SDK provide IO semantics which allow to work with lakeFS objects as if they were files in your
filesystem. This is extremely useful when working with data transformation packages that accept file descriptors and streams. 

### Upload

A simple way to upload data is to use the `upload` method which accepts contents as `str/bytes`

```python
obj = branch1.object(path="text/sample_data.txt").upload(content_type="text/plain", data="This is my object data")
print(obj.stats())
```

#### Output
```
{'path': 'text/sample_data.txt', 'physical_address': 's3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfk4fnl531fa6k90pg', 'physical_address_expiry': None, 'checksum': '4a09d10820234a95bb548f14e4435bba', 'size_bytes': 15, 'mtime': 1701865289, 'metadata': {}, 'content_type': 'text/plain'}
```

Reading the data is just as simple:
```python
print(obj.reader(mode='r').read())
```

#### Output
```
This is my object data
```

Now let's generate a "sample_data.csv" file and write it directly to a lakeFS writer object

```python
import csv

sample_data = [
    [1, "Alice", "alice@example.com"],
    [2, "Bob", "bob@example.com"],
    [3, "Carol", "carol@example.com"],
]

obj = branch1.object(path="csv/sample_data.csv")

with obj.writer(mode='w', pre_sign=True, content_type="text/csv") as fd:
    writer = csv.writer(fd)
    writer.writerow(["ID", "Name", "Email"])
    for row in sample_data:
        writer.writerow(row)
```

On context exit the object will be uploaded to lakeFS

```python
print(obj.stats())
```

#### Output
```
{'path': 'csv/sample_data.csv', 'physical_address': 's3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfk4fnl531fa6k90pg', 'physical_address_expiry': None, 'checksum': 'f181262c138901a74d47652d5ea72295', 'size_bytes': 88, 'mtime': 1701865939, 'metadata': {}, 'content_type': 'text/csv'}
```

We can also upload raw byte contents:

```python
obj = branch1.object(path="raw/file1.data").upload(data=b"Hello Object World", pre_sign=True)
print(obj.stats())
```

#### Output
```
{'path': 'raw/file1.data', 'physical_address': 's3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfltvnl531fa6k90q0', 'physical_address_expiry': None, 'checksum': '0ef432f8eb0305f730b0c57bbd7a6b08', 'size_bytes': 18, 'mtime': 1701866323, 'metadata': {}, 'content_type': 'application/octet-stream'}
```

### Uncommitted changes

Using the branch `uncommmitted` method will show all the uncommitted changes on that branch:

```python
for diff in branch1.uncommitted():
    print(diff)
```

#### Output

```
{'type': 'added', 'path': 'text/sample_data.txt', 'path_type': 'object', 'size_bytes': 15}
{'type': 'added', 'path': 'csv/sample_data.csv', 'path_type': 'object', 'size_bytes': 88}
{'type': 'added', 'path': 'raw/file1.data', 'path_type': 'object', 'size_bytes': 18}

```

As expected, our change appears here. Let's commit it and attach some arbitrary metadata:

```python
ref = branch1.commit(message='Add some data!', metadata={'using': 'python_sdk'})
print(ref.get_commit())
```

#### Output
```
{'id': 'c4666db80d2a984b4eab8ce02b6a60830767eba53995c26350e0ad994e15fedb', 'parents': ['a7a092a5a32a2cd97f22abcc99414f6283d29f6b9dd2725ce89f90188c5901e5'], 'committer': 'admin', 'message': 'Add some data!', 'creation_date': 1701866838, 'meta_range_id': '999bedeab1b740f83d2cf8c52548d55446f9038c69724d399adc4438412cade2', 'metadata': {'using': 'python_sdk'}}

```

Calling `uncommitted` again on the same branch, this time there should be no uncommitted files:

```python
print(len(list(branch1.uncommitted())))
```

#### Output
```
0
```

#### Merging changes from a branch into main 

Let's diff between your branch and the main branch:

```python
main = repo.branch("main")
for diff in main.diff(other_ref=branch1):
    print(diff)
```

#### Output
```
{'type': 'added', 'path': 'text/sample_data.txt', 'path_type': 'object', 'size_bytes': 15}
{'type': 'added', 'path': 'csv/sample_data.csv', 'path_type': 'object', 'size_bytes': 88}
{'type': 'added', 'path': 'raw/file1.data', 'path_type': 'object', 'size_bytes': 18}
```

Looks like we have some changes. Let's merge them:

```python
res = branch1.merge_into(main)
print(res)
# output:
# cfddb68b7265ae0b17fafa1a2068f8414395e0a8b8bc0f8d741cbcce1e67e394
```

Let's diff again - there should be no changes as all changes are on our main branch already:

```python
print(len(list(main.diff(other_ref=branch1))))
```

#### Output
```
0
```

### Read data from main branch

```python
import csv

obj = main.object(path="csv/sample_data.csv")

for row in csv.reader(obj.reader(mode='r')):
    print(row)
```

#### Output
```
['ID', 'Name', 'Email']
['1', 'Alice', 'alice@example.com']
['2', 'Bob', 'bob@example.com']
['3', 'Carol', 'carol@example.com']
```

### Importing data into lakeFS

The new SDK makes it much easier to import existing data from the object store into lakeFS, using the new ImportManager

```python
import lakefs

branch = lakefs.repository("example-repo").repo.branch("experiment3")

# We can import data from multiple sources in a single import process
# The following example initializes a new ImportManager and adds 2 source types; A prefix and an object.
importer = branch.import_data(commit_message="added public S3 data") \ 
    .prefix("s3://example-bucket1/path1/", destination="datasets/path1/") \
    .object("s3://example-bucket1/path2/imported_obj", destination="datasets/path2/imported_obj")

# run() is a convenience method that blocks until the import is reported as done, raising an exception if it fails.
importer.run()

```

Alternatively we can call `start()` and `status()` ourselves for an async version of the above

```python
import time

# Async version
importer.start()
status = importer.start()

while not status.completed or status.error is None:
        time.sleep(3)  # or whatever interval you choose
        status = importer.status()

if status.error:
    # handle!
    
print(f"imported a total of {status.ingested_objects} objects!")

```

#### Output
```
imported a total of 25478 objects!
```

### Transactions

Transactions is a new feature in the High Level SDK. It allows performing a sequence of operations on a branch as an atomic unit, similarly to how database transactions work.
Under the hood, the transaction creates an ephemeral branch from the source branch, performs all the operation on that branch, and merges it back to the source branch once the transaction is completed.
Transactions are currently supported as a context manager only.

```python
import lakefs

branch = lakefs.repository("example-repo").repo.branch("experiment3")

with branch.transact(commit_message="my transaction") as tx:
    for obj in tx.objects(prefix="prefix_to_delete/"):  # Delete some objects
        obj.delete()

    # Create new object
    tx.object("new_object").upload("new object data")

print(len(list(branch.objects(prefix="prefix_to_delete/"))))
print(branch.object("new_object").exists())
```

#### Output
```
0
True
```

### Python SDK documentation and API reference

For the documentation of lakeFS’s Python package and full api reference, see [https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)

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

