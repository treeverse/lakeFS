---
title: Python
description: Use Python to interact with your objects on lakeFS
---

# Use Python to interact with your objects on lakeFS


!!! warning
    If your project is currently using the [legacy Python `lakefs-client`][legacy-pypi], please be aware that this version has been [deprecated][legacy-deprecated].
    As of release **v1.44.0**, it's no longer supported for new updates or features.

**High Level Python SDK**

We've just released a new High Level Python SDK library, and we're super excited to tell you about it! Continue reading to get the
full story!
Though our previous SDK client is still supported and maintained, we highly recommend using the new High Level SDK.

!!! info "For previous Python SDKs follow these links"
    [legacy-sdk](https://pydocs.lakefs.io) (Deprecated)

## References & Resources

- **High Level Python SDK Documentation**: [https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)
- **Generated Python SDK Documentation**: [https://pydocs-sdk.lakefs.io](https://pydocs-sdk.lakefs.io)
- **Boto S3 Router**: [https://github.com/treeverse/boto-s3-router](https://github.com/treeverse/boto-s3-router)
- **lakefs-spec API Reference**: [https://lakefs-spec.org/latest/reference/lakefs_spec/](https://lakefs-spec.org/latest/reference/lakefs_spec/)

### Python Integration Options

- [Use the High Level lakeFS SDK](#using-the-lakefs-sdk) to perform **object operations**, **versioning** and other **lakeFS-specific operations**.
- [Use the generated lakefs-sdk](https://pydocs-sdk.lakefs.io) for direct API access based on the OpenAPI specification of lakeFS.
- [Using lakefs-spec](#using-lakefs-spec-for-higher-level-file-operations) to perform high-level file operations through a file-system-like API.
- [Use Boto](#using-boto) to perform **object operations** through the **lakeFS S3 gateway**.

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

!!! info
    See [here](../security/external-principals-aws.md#login-with-python) for instructions on how to log in with Python using your AWS role. This is applicable for enterprise users.

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

!!! warning
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

<h4>Creating a repository</h4>

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

**Output**

```
{id: 'example-repo', creation_date: 1697815536, default_branch: 'main', storage_namespace: 's3://storage-bucket/repos/example-repo'}
```

<h4>List repositories</h4>

```python
import lakefs

print("Listing repositories:")
for repo in lakefs.repositories():
    print(repo)

```

**Output**

```
Listing repositories:
{id: 'example-repo', creation_date: 1697815536, default_branch: 'main', storage_namespace: 's3://storage-bucket/repos/example-repo'}
```

<h4>Creating a branch</h4>

```python
import lakefs

branch1 = lakefs.repository("example-repo").branch("experiment1").create(source_reference="main")
print("experiment1 ref:", branch1.get_commit().id)

branch2 = lakefs.repository("example-repo").branch("experiment2").create(source_reference="main")
print("experiment2 ref:", branch2.get_commit().id)
```

**Output**

```
experiment1 ref: 7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859
experiment2 ref: 7a300b41a8e1ca666c653171a364c08f640549c24d7e82b401bf077c646f8859
```

<h4>List branches</h4>

```python
import lakefs

for branch in lakefs.repository("example-repo").branches():
    print(branch)

```

**Output**

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
print(obj.stat())
```

**Output**

```
{'path': 'text/sample_data.txt', 'physical_address': 's3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfk4fnl531fa6k90pg', 'physical_address_expiry': None, 'checksum': '4a09d10820234a95bb548f14e4435bba', 'size_bytes': 15, 'mtime': 1701865289, 'metadata': {}, 'content_type': 'text/plain'}
```

Reading the data is just as simple:

```python
print(obj.reader(mode='r').read())
```

**Output**

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
print(obj.stat())
```

**Output**

```
{'path': 'csv/sample_data.csv', 'physical_address': 's3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfk4fnl531fa6k90pg', 'physical_address_expiry': None, 'checksum': 'f181262c138901a74d47652d5ea72295', 'size_bytes': 88, 'mtime': 1701865939, 'metadata': {}, 'content_type': 'text/csv'}
```

We can also upload raw byte contents:

```python
obj = branch1.object(path="raw/file1.data").upload(data=b"Hello Object World", pre_sign=True)
print(obj.stat())
```

**Output**

```
{'path': 'raw/file1.data', 'physical_address': 's3://storage-bucket/repos/example-repo/data/gke0ignnl531fa6k90p0/ckpfltvnl531fa6k90q0', 'physical_address_expiry': None, 'checksum': '0ef432f8eb0305f730b0c57bbd7a6b08', 'size_bytes': 18, 'mtime': 1701866323, 'metadata': {}, 'content_type': 'application/octet-stream'}
```

### Uncommitted changes

Using the branch `uncommmitted` method will show all the uncommitted changes on that branch:

```python
for diff in branch1.uncommitted():
    print(diff)
```

**Output**

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

**Output**

```
{'id': 'c4666db80d2a984b4eab8ce02b6a60830767eba53995c26350e0ad994e15fedb', 'parents': ['a7a092a5a32a2cd97f22abcc99414f6283d29f6b9dd2725ce89f90188c5901e5'], 'committer': 'admin', 'message': 'Add some data!', 'creation_date': 1701866838, 'meta_range_id': '999bedeab1b740f83d2cf8c52548d55446f9038c69724d399adc4438412cade2', 'metadata': {'using': 'python_sdk'}}

```

Calling `uncommitted` again on the same branch, this time there should be no uncommitted files:

```python
print(len(list(branch1.uncommitted())))
```

**Output**

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

**Output**

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

**Output**

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

**Output**

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

**Output**

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

**Output**

```
0
True
```

### Python SDK documentation and API reference

For the documentation of lakeFS's Python package and full api reference, see [https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)

## Using lakefs-spec for higher-level file operations

The [lakefs-spec](https://lakefs-spec.org/latest/) project
provides higher-level file operations on lakeFS objects with a filesystem API,
built on the [fsspec](https://github.com/fsspec/filesystem_spec) project.

!!! note
    This library is a third-party package and not maintained by the lakeFS developers; please file issues and bug reports directly
    in the [lakefs-spec](https://github.com/aai-institute/lakefs-spec) repository.

### Installation

Install `lakefs-spec` directly with `pip`:

```shell
python -m pip install --upgrade lakefs-spec
```

### Interacting with lakeFS through a file system

To write a file directly to a branch in a lakeFS repository, consider the following example:

```python
from pathlib import Path

from lakefs_spec import LakeFSFileSystem

REPO, BRANCH = "example-repo", "main"

# Prepare a local example file.
lpath = Path("demo.txt")
lpath.write_text("Hello, lakeFS!")

fs = LakeFSFileSystem()  # will auto-discover credentials from ~/.lakectl.yaml
rpath = f"{REPO}/{BRANCH}/{lpath.name}"
fs.put(lpath, rpath)
```

Reading it again from remote is as easy as the following:

```python
f = fs.open(rpath, "rt")
print(f.readline())  # prints "Hello, lakeFS!"
```

Many more operations like retrieving an object's metadata or checking an
object's existence on the lakeFS server are also supported. For a full list,
see the [API reference](https://lakefs-spec.org/latest/reference/lakefs_spec/).

### Integrations with popular data science packages

A number of Python data science projects support fsspec, with [pandas](https://pandas.pydata.org/) being a prominent example.
Reading a Parquet file from a lakeFS repository into a Pandas data frame for analysis is very easy, demonstrated on the quickstart repository sample data:

```python
import pandas as pd

# Read into pandas directly by supplying the lakeFS URI...
lakes = pd.read_parquet(f"lakefs://quickstart/main/lakes.parquet")
german_lakes = lakes.query('Country == "Germany"')
# ... and store directly, again with a raw lakeFS URI.
german_lakes.to_csv(f"lakefs://quickstart/main/german_lakes.csv")
```

A list of integrations with popular data science libraries can be found in the [lakefs-spec documentation](https://lakefs-spec.org/latest/guides/integrations/).

### Using transactions for atomic versioning operations

As with the high-level SDK (see above), lakefs-spec also supports transactions
for conducting versioning operations on newly modified files. The following is an example of creating a commit on the repository's main branch directly after a file upload:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

# assumes you have a local train-test split as two text files:
# train-data.txt, and test-data.txt.
with fs.transaction("example-repo", "main") as tx:
    fs.put_file("train-data.txt", f"example-repo/{tx.branch.id}/train-data.txt")
    tx.commit(message="Add training data")
    fs.put_file("test-data.txt", f"example-repo/{tx.branch.id}/test-data.txt")
    sha = tx.commit(message="Add test data")
    tx.tag(sha, name="My train-test split")
```

Transactions are atomic - if an exception happens at any point of the transaction, the repository remains unchanged.

### Further information

For more user guides, tutorials on integrations with data science tools like pandas, and more, check out the [lakefs-spec documentation](https://lakefs-spec.org/latest/).

## Using Boto

!!! info
    To use Boto with lakeFS alongside S3, check out [Boto S3 Router](https://github.com/treeverse/boto-s3-router){:target="_blank"}. It will route
    requests to either S3 or lakeFS according to the provided bucket name.

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

#### Configuring Boto3 S3 Client with Checksum Settings

In newer versions of Boto3, when connecting to **lakeFS** using **HTTPS**,
you might encounter an **AccessDenied** error on upload,
while the lakeFS logs display an error `encoding/hex: invalid byte: U+0053 'S'`.

To avoid this issue, explicitly configure the Boto3 client with the following checksum settings:

- `request_checksum_calculation: 'when_required'`
- `response_checksum_validation: 'when_required'`

Example of how to configure it:

```python
import boto3
from botocore.config import Config

# Configure checksum settings
config = Config(
    request_checksum_calculation='when_required',
    response_checksum_validation='when_required'
)

s3_client = boto3.client(
    's3',
    endpoint_url='https://lakefs.example.com',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config,
)
```

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

[legacy-deprecated]:  ../posts/deprecate-py-legacy.md
[legacy-pypi]:  https://pypi.org/project/lakefs-client/
