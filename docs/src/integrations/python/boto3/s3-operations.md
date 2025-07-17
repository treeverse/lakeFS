---
title: S3 Operations with lakeFS
description: Comprehensive guide to S3-compatible operations using Boto3 with lakeFS
sdk_types: ["boto3"]
difficulty: "beginner"
use_cases: ["s3-migration", "object-storage"]
---

# S3 Operations with lakeFS

This guide covers all S3-compatible operations available when using Boto3 with lakeFS, including object management, multipart uploads, and presigned URLs.

## Understanding lakeFS Object Paths

In lakeFS, object keys include the branch or commit reference:

```
Format: {branch-or-commit}/{path/to/object}

Examples:
main/data/users.csv              # File on main branch
feature-branch/data/users.csv    # File on feature branch  
c1a2b3c4d5e6f7g8/data/users.csv  # File at specific commit
```

## Basic Object Operations

### Upload Objects (PUT)

Upload objects to lakeFS repositories using standard S3 operations:

```python
import boto3

s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Upload string content
s3_client.put_object(
    Bucket='my-repo',
    Key='main/data/hello.txt',
    Body='Hello, lakeFS!',
    ContentType='text/plain'
)

# Upload binary content
with open('local-file.pdf', 'rb') as f:
    s3_client.put_object(
        Bucket='my-repo',
        Key='main/documents/file.pdf',
        Body=f,
        ContentType='application/pdf'
    )

# Upload with metadata
s3_client.put_object(
    Bucket='my-repo',
    Key='main/data/processed.json',
    Body='{"status": "processed"}',
    ContentType='application/json',
    Metadata={
        'processed-by': 'data-pipeline',
        'version': '1.0',
        'timestamp': '2024-01-15T10:30:00Z'
    }
)

print("Objects uploaded successfully!")
```

### Download Objects (GET)

Retrieve objects from lakeFS:

```python
# Download object content
response = s3_client.get_object(
    Bucket='my-repo',
    Key='main/data/hello.txt'
)

content = response['Body'].read().decode('utf-8')
print(f"Content: {content}")

# Download with metadata
response = s3_client.get_object(
    Bucket='my-repo',
    Key='main/data/processed.json'
)

# Access content
content = response['Body'].read().decode('utf-8')
print(f"Content: {content}")

# Access metadata
metadata = response.get('Metadata', {})
print(f"Metadata: {metadata}")

# Access standard attributes
print(f"Content Type: {response['ContentType']}")
print(f"Content Length: {response['ContentLength']}")
print(f"Last Modified: {response['LastModified']}")
print(f"ETag: {response['ETag']}")
```

**Expected Output:**
```
Content: Hello, lakeFS!
Content: {"status": "processed"}
Metadata: {'processed-by': 'data-pipeline', 'version': '1.0', 'timestamp': '2024-01-15T10:30:00Z'}
Content Type: application/json
Content Length: 23
Last Modified: 2024-01-15 10:30:00+00:00
ETag: "d41d8cd98f00b204e9800998ecf8427e"
```

### Download to File

```python
# Download directly to file
s3_client.download_file(
    Bucket='my-repo',
    Key='main/documents/file.pdf',
    Filename='downloaded-file.pdf'
)

# Download with error handling
try:
    s3_client.download_file(
        Bucket='my-repo',
        Key='main/data/large-dataset.csv',
        Filename='local-dataset.csv'
    )
    print("File downloaded successfully!")
except Exception as e:
    print(f"Download failed: {e}")
```

### Object Information (HEAD)

Get object metadata without downloading content:

```python
# Get object metadata
response = s3_client.head_object(
    Bucket='my-repo',
    Key='main/data/processed.json'
)

print(f"Content Type: {response['ContentType']}")
print(f"Content Length: {response['ContentLength']}")
print(f"Last Modified: {response['LastModified']}")
print(f"Custom Metadata: {response.get('Metadata', {})}")

# Check if object exists
def object_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        print(f"Error checking object: {e}")
        return False

if object_exists('my-repo', 'main/data/hello.txt'):
    print("Object exists!")
```

### Delete Objects

Remove objects from lakeFS:

```python
# Delete single object
s3_client.delete_object(
    Bucket='my-repo',
    Key='main/temp/temporary-file.txt'
)

# Delete multiple objects
objects_to_delete = [
    {'Key': 'main/temp/file1.txt'},
    {'Key': 'main/temp/file2.txt'},
    {'Key': 'main/temp/file3.txt'}
]

response = s3_client.delete_objects(
    Bucket='my-repo',
    Delete={
        'Objects': objects_to_delete,
        'Quiet': False  # Set to True to suppress response details
    }
)

# Check deletion results
for deleted in response.get('Deleted', []):
    print(f"Deleted: {deleted['Key']}")

for error in response.get('Errors', []):
    print(f"Error deleting {error['Key']}: {error['Message']}")
```

## List Operations

### List Objects

List objects in repositories and branches:

```python
# List all objects in main branch
response = s3_client.list_objects_v2(
    Bucket='my-repo',
    Prefix='main/'
)

print(f"Found {response.get('KeyCount', 0)} objects:")
for obj in response.get('Contents', []):
    print(f"  {obj['Key']} ({obj['Size']} bytes)")

# List with pagination
paginator = s3_client.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(
    Bucket='my-repo',
    Prefix='main/data/',
    PaginationConfig={'PageSize': 100}
)

for page in page_iterator:
    for obj in page.get('Contents', []):
        print(f"{obj['Key']} - {obj['LastModified']}")
```

### List with Filtering

```python
# List objects with specific extension
def list_objects_by_extension(bucket, prefix, extension):
    """List objects with specific file extension"""
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    
    filtered_objects = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith(extension):
            filtered_objects.append(obj)
    
    return filtered_objects

# Find all CSV files in data directory
csv_files = list_objects_by_extension('my-repo', 'main/data/', '.csv')
print(f"Found {len(csv_files)} CSV files:")
for obj in csv_files:
    print(f"  {obj['Key']} ({obj['Size']} bytes)")
```

### List Repositories (Buckets)

```python
# List all repositories
response = s3_client.list_buckets()

print(f"Found {len(response['Buckets'])} repositories:")
for bucket in response['Buckets']:
    print(f"  {bucket['Name']} (created: {bucket['CreationDate']})")
```

## Multipart Upload Operations

For large files, use multipart uploads:

```python
import os
from concurrent.futures import ThreadPoolExecutor

def multipart_upload(bucket, key, file_path, part_size=5*1024*1024):
    """Upload large file using multipart upload"""
    
    # Initialize multipart upload
    response = s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=key,
        ContentType='application/octet-stream'
    )
    upload_id = response['UploadId']
    
    try:
        # Calculate parts
        file_size = os.path.getsize(file_path)
        parts = []
        part_number = 1
        
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(part_size)
                if not data:
                    break
                
                # Upload part
                part_response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                
                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })
                
                print(f"Uploaded part {part_number}")
                part_number += 1
        
        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"Multipart upload completed: {key}")
        return True
        
    except Exception as e:
        # Abort upload on error
        s3_client.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )
        print(f"Multipart upload aborted: {e}")
        return False

# Usage
success = multipart_upload(
    bucket='my-repo',
    key='main/large-files/dataset.zip',
    file_path='local-large-file.zip'
)
```

### Parallel Multipart Upload

```python
def parallel_multipart_upload(bucket, key, file_path, part_size=5*1024*1024, max_workers=4):
    """Upload large file using parallel multipart upload"""
    
    # Initialize multipart upload
    response = s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=key
    )
    upload_id = response['UploadId']
    
    def upload_part(part_info):
        part_number, start, size = part_info
        with open(file_path, 'rb') as f:
            f.seek(start)
            data = f.read(size)
            
            response = s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=data
            )
            
            return {
                'ETag': response['ETag'],
                'PartNumber': part_number
            }
    
    try:
        # Calculate parts
        file_size = os.path.getsize(file_path)
        parts_info = []
        part_number = 1
        start = 0
        
        while start < file_size:
            size = min(part_size, file_size - start)
            parts_info.append((part_number, start, size))
            start += size
            part_number += 1
        
        # Upload parts in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            parts = list(executor.map(upload_part, parts_info))
        
        # Sort parts by part number
        parts.sort(key=lambda x: x['PartNumber'])
        
        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"Parallel multipart upload completed: {key}")
        return True
        
    except Exception as e:
        # Abort upload on error
        s3_client.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id
        )
        print(f"Parallel multipart upload failed: {e}")
        return False

# Usage
success = parallel_multipart_upload(
    bucket='my-repo',
    key='main/large-files/big-dataset.zip',
    file_path='very-large-file.zip',
    max_workers=8
)
```

## Presigned URLs

Generate temporary URLs for secure access:

```python
from botocore.exceptions import ClientError

# Generate presigned URL for download
def generate_download_url(bucket, key, expiration=3600):
    """Generate presigned URL for downloading object"""
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except ClientError as e:
        print(f"Error generating presigned URL: {e}")
        return None

# Generate presigned URL for upload
def generate_upload_url(bucket, key, expiration=3600):
    """Generate presigned URL for uploading object"""
    try:
        url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except ClientError as e:
        print(f"Error generating presigned URL: {e}")
        return None

# Usage
download_url = generate_download_url('my-repo', 'main/data/report.pdf', expiration=7200)
if download_url:
    print(f"Download URL (valid for 2 hours): {download_url}")

upload_url = generate_upload_url('my-repo', 'main/uploads/new-file.txt', expiration=1800)
if upload_url:
    print(f"Upload URL (valid for 30 minutes): {upload_url}")
```

### Using Presigned URLs

```python
import requests

# Upload using presigned URL
def upload_with_presigned_url(presigned_url, file_path):
    """Upload file using presigned URL"""
    try:
        with open(file_path, 'rb') as f:
            response = requests.put(presigned_url, data=f)
            response.raise_for_status()
        print("Upload successful!")
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

# Download using presigned URL
def download_with_presigned_url(presigned_url, save_path):
    """Download file using presigned URL"""
    try:
        response = requests.get(presigned_url)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            f.write(response.content)
        print("Download successful!")
        return True
    except Exception as e:
        print(f"Download failed: {e}")
        return False

# Usage
upload_url = generate_upload_url('my-repo', 'main/uploads/document.pdf')
if upload_url:
    upload_with_presigned_url(upload_url, 'local-document.pdf')

download_url = generate_download_url('my-repo', 'main/data/report.pdf')
if download_url:
    download_with_presigned_url(download_url, 'downloaded-report.pdf')
```

## Object Metadata Operations

### Working with Custom Metadata

```python
# Upload with extensive metadata
s3_client.put_object(
    Bucket='my-repo',
    Key='main/datasets/customer-data.csv',
    Body=open('customer-data.csv', 'rb'),
    ContentType='text/csv',
    Metadata={
        'source': 'customer-database',
        'extracted-date': '2024-01-15',
        'record-count': '10000',
        'schema-version': '2.1',
        'data-classification': 'sensitive'
    },
    # Standard S3 metadata
    ContentDisposition='attachment; filename="customer-data.csv"',
    ContentLanguage='en',
    CacheControl='max-age=3600'
)

# Retrieve and display metadata
response = s3_client.head_object(
    Bucket='my-repo',
    Key='main/datasets/customer-data.csv'
)

print("Standard Metadata:")
print(f"  Content Type: {response.get('ContentType')}")
print(f"  Content Length: {response.get('ContentLength')}")
print(f"  Last Modified: {response.get('LastModified')}")
print(f"  ETag: {response.get('ETag')}")

print("\nCustom Metadata:")
for key, value in response.get('Metadata', {}).items():
    print(f"  {key}: {value}")
```

### Copy Objects with Metadata

```python
# Copy object with metadata preservation
s3_client.copy_object(
    Bucket='my-repo',
    Key='feature-branch/datasets/customer-data.csv',
    CopySource={
        'Bucket': 'my-repo',
        'Key': 'main/datasets/customer-data.csv'
    },
    MetadataDirective='COPY'  # Preserve original metadata
)

# Copy object with new metadata
s3_client.copy_object(
    Bucket='my-repo',
    Key='feature-branch/datasets/customer-data-v2.csv',
    CopySource={
        'Bucket': 'my-repo',
        'Key': 'main/datasets/customer-data.csv'
    },
    MetadataDirective='REPLACE',
    Metadata={
        'source': 'customer-database',
        'version': '2.0',
        'updated-date': '2024-01-16'
    },
    ContentType='text/csv'
)
```

## Error Handling

### Common Error Patterns

```python
from botocore.exceptions import ClientError, NoCredentialsError

def robust_s3_operation(operation_func, *args, **kwargs):
    """Execute S3 operation with comprehensive error handling"""
    try:
        return operation_func(*args, **kwargs)
        
    except NoCredentialsError:
        print("Error: AWS credentials not found")
        print("Check your access key configuration")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'NoSuchBucket':
            print(f"Error: Repository not found - {error_message}")
        elif error_code == 'NoSuchKey':
            print(f"Error: Object not found - {error_message}")
        elif error_code == 'AccessDenied':
            print(f"Error: Access denied - {error_message}")
        elif error_code == 'InvalidRequest':
            print(f"Error: Invalid request - {error_message}")
        else:
            print(f"Error: {error_code} - {error_message}")
            
    except Exception as e:
        print(f"Unexpected error: {e}")

# Usage examples
robust_s3_operation(
    s3_client.get_object,
    Bucket='my-repo',
    Key='main/data/nonexistent.txt'
)

robust_s3_operation(
    s3_client.put_object,
    Bucket='nonexistent-repo',
    Key='main/data/test.txt',
    Body='test content'
)
```

### Retry Logic

```python
import time
from botocore.exceptions import ClientError

def s3_operation_with_retry(operation_func, max_retries=3, backoff_factor=1, *args, **kwargs):
    """Execute S3 operation with exponential backoff retry"""
    
    for attempt in range(max_retries + 1):
        try:
            return operation_func(*args, **kwargs)
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            # Don't retry certain errors
            if error_code in ['NoSuchBucket', 'NoSuchKey', 'AccessDenied']:
                raise
            
            if attempt == max_retries:
                print(f"Max retries ({max_retries}) exceeded")
                raise
            
            wait_time = backoff_factor * (2 ** attempt)
            print(f"Attempt {attempt + 1} failed, retrying in {wait_time}s...")
            time.sleep(wait_time)
            
        except Exception as e:
            if attempt == max_retries:
                raise
            
            wait_time = backoff_factor * (2 ** attempt)
            print(f"Attempt {attempt + 1} failed, retrying in {wait_time}s...")
            time.sleep(wait_time)

# Usage
try:
    response = s3_operation_with_retry(
        s3_client.put_object,
        max_retries=3,
        backoff_factor=1,
        Bucket='my-repo',
        Key='main/data/important-file.txt',
        Body='Important content'
    )
    print("Operation successful!")
except Exception as e:
    print(f"Operation failed after retries: {e}")
```

## Performance Optimization

### Batch Operations

```python
def batch_upload_objects(bucket, objects_data, max_workers=5):
    """Upload multiple objects in parallel"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def upload_single_object(item):
        key, body = item
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body
            )
            return f"✓ {key}"
        except Exception as e:
            return f"✗ {key}: {e}"
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(upload_single_object, item): item[0] 
            for item in objects_data
        }
        
        for future in as_completed(future_to_key):
            result = future.result()
            results.append(result)
            print(result)
    
    return results

# Usage
objects_to_upload = [
    ('main/data/file1.txt', 'Content 1'),
    ('main/data/file2.txt', 'Content 2'),
    ('main/data/file3.txt', 'Content 3'),
    ('main/data/file4.txt', 'Content 4'),
    ('main/data/file5.txt', 'Content 5')
]

results = batch_upload_objects('my-repo', objects_to_upload)
print(f"Batch upload completed: {len(results)} operations")
```

### Connection Pooling

```python
import boto3
from botocore.config import Config

# Configure connection pooling for better performance
config = Config(
    max_pool_connections=50,  # Increase connection pool size
    retries={
        'max_attempts': 3,
        'mode': 'adaptive'
    }
)

s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    config=config
)
```

## Next Steps

- Learn about [S3 Router](s3-router.md) for hybrid S3/lakeFS workflows
- Explore [troubleshooting guide](../reference/troubleshooting.md) for common issues
- Review [best practices](../reference/best-practices.md) for production usage
- Check out [Boto3 configuration](configuration.md) for advanced setup options

## See Also

- [High-Level SDK](../high-level-sdk/) - For advanced lakeFS features
- [Generated SDK](../generated-sdk/) - For direct API access
- [lakefs-spec](../lakefs-spec/) - For filesystem-style operations