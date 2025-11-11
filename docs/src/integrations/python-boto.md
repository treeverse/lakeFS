---
title: Python - Boto & S3 Gateway
description: Use Boto3 with lakeFS S3 Gateway for S3-compatible operations
---

# Using Boto3 with lakeFS S3 Gateway

lakeFS exposes an S3-compatible API through its S3 Gateway, allowing you to use Boto3 (AWS SDK for Python) directly with lakeFS. This integration is perfect for existing S3 workflows and applications.

!!! info
    To use Boto with lakeFS alongside S3, check out [Boto S3 Router](https://github.com/treeverse/boto-s3-router). It will route requests to either S3 or lakeFS according to the provided bucket name.

## When to Use

Use Boto with lakeFS when you:

- Have **existing S3 workflows** you want to use with lakeFS
- Need **S3-compatible operations** (put, get, list, delete)
- Work with **legacy S3 applications**
- Want to **migrate from S3** without code changes

For versioning-focused workflows, use the **[High-Level SDK](./python.md)** or **[lakefs-spec](./python-lakefs-spec.md)**.

## Installation

Install Boto3 using pip:

```shell
pip install boto3
```

Or upgrade to the latest version:

```shell
pip install --upgrade boto3
```

## Basic Setup

### Initializing Boto3 Client

```python
import boto3

# Create S3 client pointing to lakeFS
s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
    region_name='us-east-1'
)

print("Client initialized")
```

## Checksum Configuration

In newer versions of Boto3 when using HTTPS, you might encounter an `AccessDenied` error with lakeFS logs showing `encoding/hex: invalid byte: U+0053 'S'`. This is due to checksum configuration.

### Configuring Checksum Settings

```python
import boto3
from botocore.config import Config

# Configure checksum settings
config = Config(
    request_checksum_calculation='when_required',
    response_checksum_validation='when_required'
)

s3 = boto3.client(
    's3',
    endpoint_url='https://lakefs.example.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key',
    config=config
)

print("Client with checksum configuration initialized")
```

## Basic Operations

### Uploading Objects

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

# Upload from bytes
data = b"Hello, lakeFS!"
s3.put_object(
    Bucket='my-repo',
    Key='main/data/hello.txt',
    Body=data
)

# Upload from file
with open('local_file.csv', 'rb') as f:
    s3.put_object(
        Bucket='my-repo',
        Key='main/data/imported.csv',
        Body=f
    )

# Upload with metadata
s3.put_object(
    Bucket='my-repo',
    Key='main/data/data.csv',
    Body=b'id,name\n1,Alice\n2,Bob',
    Metadata={
        'owner': 'data-team',
        'version': '1.0'
    }
)

print("Upload complete")
```

### Downloading Objects

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

# Download entire object
response = s3.get_object(
    Bucket='my-repo',
    Key='main/data/data.csv'
)
data = response['Body'].read()
print(f"Downloaded {len(data)} bytes")

# Download to file
s3.download_file(
    Bucket='my-repo',
    Key='main/data/large_file.parquet',
    Filename='local_file.parquet'
)

# Stream download (for large files)
response = s3.get_object(Bucket='my-repo', Key='main/data/large.csv')
for chunk in iter(lambda: response['Body'].read(1024), b''):
    process_chunk(chunk)
```

### Listing Objects

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

# List objects in branch
response = s3.list_objects_v2(
    Bucket='my-repo',
    Prefix='main/data/'
)

for obj in response.get('Contents', []):
    print(f"{obj['Key']} ({obj['Size']} bytes)")

# List objects at commit
response = s3.list_objects_v2(
    Bucket='my-repo',
    Prefix='abc123def456/data/'
)

for obj in response.get('Contents', []):
    print(f"{obj['Key']}")
```

### Getting Object Metadata

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

# Head object
response = s3.head_object(
    Bucket='my-repo',
    Key='main/data/file.csv'
)

print(f"Content Type: {response.get('ContentType')}")
print(f"Content Length: {response.get('ContentLength')}")
print(f"Last Modified: {response.get('LastModified')}")
print(f"Metadata: {response.get('Metadata')}")
```

### Deleting Objects

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

# Delete single object
s3.delete_object(
    Bucket='my-repo',
    Key='main/data/temp.txt'
)

# Delete multiple objects
s3.delete_objects(
    Bucket='my-repo',
    Delete={
        'Objects': [
            {'Key': 'main/data/file1.txt'},
            {'Key': 'main/data/file2.txt'},
            {'Key': 'main/data/file3.txt'}
        ]
    }
)

print("Delete complete")
```

## Real-World Workflows

### ETL with S3-Like Operations

```python
import boto3
import csv
import io

def etl_pipeline():
    s3 = boto3.client(
        's3',
        endpoint_url='https://example.lakefs.io',
        aws_access_key_id='your-access-key',
        aws_secret_access_key='your-secret-key'
    )

    # Extract: Read from source
    response = s3.get_object(Bucket='my-repo', Key='main/raw/input.csv')
    input_data = response['Body'].read().decode()

    # Transform: Process data
    reader = csv.DictReader(io.StringIO(input_data))
    rows = list(reader)

    # Clean: Remove duplicates
    unique_rows = {row['id']: row for row in rows}.values()

    # Load: Write processed data
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=['id', 'name', 'value'])
    writer.writeheader()
    writer.writerows(unique_rows)

    s3.put_object(
        Bucket='my-repo',
        Key='main/processed/output.csv',
        Body=output.getvalue()
    )

    print(f"ETL complete: {len(unique_rows)} unique records")

etl_pipeline()
```

### Backup and Sync

```python
import boto3
import os
from pathlib import Path

def backup_to_lakeFS(local_dir, repo, branch, prefix):
    """Backup local directory to lakeFS"""
    s3 = boto3.client(
        's3',
        endpoint_url='https://example.lakefs.io',
        aws_access_key_id='your-access-key',
        aws_secret_access_key='your-secret-key'
    )

    count = 0
    for local_file in Path(local_dir).rglob('*'):
        if local_file.is_file():
            # Calculate remote path
            rel_path = local_file.relative_to(local_dir)
            remote_path = f"{branch}/{prefix}/{rel_path}".replace("\\", "/")

            # Upload
            with open(local_file, 'rb') as f:
                s3.put_object(
                    Bucket=repo,
                    Key=remote_path,
                    Body=f
                )
            count += 1

            if count % 100 == 0:
                print(f"Backed up {count} files...")

    print(f"Backup complete: {count} files uploaded")

# Usage:
# backup_to_lakeFS("/path/to/local/data", "my-repo", "main", "backups/2024-01")
```

### Copy from S3 to lakeFS

```python
import boto3

def migrate_s3_to_lakefs(s3_bucket, prefix, repo, branch):
    """Migrate data from S3 to lakeFS"""
    # Connect to S3
    s3_source = boto3.client(
        's3',
        region_name='us-east-1'
    )

    # Connect to lakeFS
    s3_dest = boto3.client(
        's3',
        endpoint_url='https://example.lakefs.io',
        aws_access_key_id='your-access-key',
        aws_secret_access_key='your-secret-key'
    )

    # List objects
    paginator = s3_source.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

    count = 0
    for page in pages:
        for obj in page.get('Contents', []):
            # Download from S3
            response = s3_source.get_object(
                Bucket=s3_bucket,
                Key=obj['Key']
            )
            data = response['Body'].read()

            # Upload to lakeFS
            s3_dest.put_object(
                Bucket=repo,
                Key=f"{branch}/{obj['Key']}",
                Body=data
            )

            count += 1
            if count % 100 == 0:
                print(f"Migrated {count} objects...")

    print(f"Migration complete: {count} objects")

# Usage:
# migrate_s3_to_lakefs("my-s3-bucket", "data/", "my-repo", "main")
```

### Version-Specific Access

```python
import boto3

def read_from_commit(repo, commit_id, key):
    """Read object from specific commit"""
    s3 = boto3.client(
        's3',
        endpoint_url='https://example.lakefs.io',
        aws_access_key_id='your-access-key',
        aws_secret_access_key='your-secret-key'
    )

    # Use commit ID as prefix
    response = s3.get_object(
        Bucket=repo,
        Key=f"{commit_id}/{key}"
    )

    return response['Body'].read()

# Usage:
# data = read_from_commit("my-repo", "abc123def456", "data/file.csv")
```

## Error Handling

```python
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client(
    's3',
    endpoint_url='https://example.lakefs.io',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

try:
    s3.put_object(
        Bucket='my-repo',
        Key='main/data/file.txt',
        Body=b'data'
    )
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == 'AccessDenied':
        print("Access denied - check credentials or permissions")
    elif error_code == 'NoSuchBucket':
        print("Bucket not found - check repository name")
    else:
        print(f"Error: {error_code}")
```

## Further Resources

- **[lakeFS S3 Gateway](../reference/s3.md)** - S3 Gateway API documentation
- **[Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)** - Official Boto3 reference
