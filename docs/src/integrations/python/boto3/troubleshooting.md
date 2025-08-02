---
title: Boto3 Troubleshooting and Migration Guide
description: Comprehensive troubleshooting guide and migration patterns for Boto3 with lakeFS
sdk_types: ["boto3"]
difficulty: "intermediate"
use_cases: ["troubleshooting", "migration", "s3-compatibility"]
---

# Boto3 Troubleshooting and Migration Guide

This guide covers common issues when using Boto3 with lakeFS and provides step-by-step migration patterns from pure S3 workflows.

## Common Error Scenarios

### Connection and Authentication Errors

#### Error: "Could not connect to the endpoint URL"

**Symptom:**
```
botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: "http://localhost:8000/"
```

**Causes and Solutions:**

1. **lakeFS Server Not Running**
   ```bash
   # Check if lakeFS is running
   curl http://localhost:8000/api/v1/healthcheck
   
   # Start lakeFS if needed
   lakefs run
   ```

2. **Wrong Endpoint URL**
   ```python
   # ✗ Wrong
   s3_client = boto3.client('s3',
       endpoint_url='http://localhost:8080'  # Wrong port
   )
   
   # ✓ Correct
   s3_client = boto3.client('s3',
       endpoint_url='http://localhost:8000'  # Default lakeFS port
   )
   ```

3. **Network/Firewall Issues**
   ```python
   # Test connectivity
   import requests
   try:
       response = requests.get('http://localhost:8000/api/v1/healthcheck')
       print(f"lakeFS is reachable: {response.status_code}")
   except Exception as e:
       print(f"Cannot reach lakeFS: {e}")
   ```

#### Error: "The AWS Access Key Id you provided does not exist"

**Symptom:**
```
botocore.exceptions.ClientError: An error occurred (InvalidAccessKeyId) when calling the ListBuckets operation: The AWS Access Key Id you provided does not exist in our records.
```

**Solutions:**

1. **Check Credentials**
   ```python
   # Verify credentials are set correctly
   import os
   
   print("Endpoint:", os.getenv('LAKEFS_ENDPOINT'))
   print("Access Key:", os.getenv('LAKEFS_ACCESS_KEY_ID'))
   print("Secret Key:", "***" if os.getenv('LAKEFS_SECRET_ACCESS_KEY') else "NOT SET")
   ```

2. **Create New Access Keys**
   ```bash
   # Using lakectl
   lakectl auth users credentials create --user admin
   ```

3. **Validate Credentials**
   ```python
   def validate_credentials(endpoint, access_key, secret_key):
       """Validate lakeFS credentials"""
       try:
           client = boto3.client('s3',
               endpoint_url=endpoint,
               aws_access_key_id=access_key,
               aws_secret_access_key=secret_key
           )
           
           response = client.list_buckets()
           print(f"✓ Credentials valid. Found {len(response['Buckets'])} repositories")
           return True
           
       except Exception as e:
           print(f"✗ Credentials invalid: {e}")
           return False
   
   # Test
   validate_credentials(
       'http://localhost:8000',
       'AKIAIOSFODNN7EXAMPLE',
       'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
   )
   ```

### SSL and Certificate Errors

#### Error: "SSL: CERTIFICATE_VERIFY_FAILED"

**Symptom:**
```
ssl.SSLError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed certificate
```

**Solutions:**

1. **Disable SSL Verification (Development Only)**
   ```python
   import urllib3
   urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
   
   s3_client = boto3.client('s3',
       endpoint_url='https://lakefs.example.com',
       aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
       aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
       verify=False  # Disable SSL verification
   )
   ```

2. **Provide Custom CA Certificate**
   ```python
   s3_client = boto3.client('s3',
       endpoint_url='https://lakefs.example.com',
       aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
       aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
       verify='/path/to/ca-bundle.pem'  # Custom CA certificate
   )
   ```

3. **Use HTTP for Development**
   ```python
   # For local development, use HTTP instead of HTTPS
   s3_client = boto3.client('s3',
       endpoint_url='http://localhost:8000',  # HTTP instead of HTTPS
       aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
       aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
   )
   ```

### Checksum and Compatibility Errors

#### Error: "An error occurred (InvalidRequest) when calling the PutObject operation"

**Symptom:**
```
botocore.exceptions.ClientError: An error occurred (InvalidRequest) when calling the PutObject operation: Content-MD5 header is required
```

**Cause:** Newer versions of Boto3 automatically calculate checksums that may not be compatible with lakeFS.

**Solutions:**

1. **Configure Checksum Settings**
   ```python
   from botocore.config import Config
   
   # Disable automatic checksums
   config = Config(
       request_checksum_calculation='when_required',
       response_checksum_validation='when_required'
   )
   
   s3_client = boto3.client('s3',
       endpoint_url='http://localhost:8000',
       aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
       aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
       config=config
   )
   ```

2. **Downgrade Boto3 (Temporary Solution)**
   ```bash
   # Install compatible version
   pip install boto3==1.26.137 botocore==1.29.137
   ```

3. **Use Alternative Upload Methods**
   ```python
   # Use upload_file instead of put_object for large files
   s3_client.upload_file(
       Filename='local-file.txt',
       Bucket='my-repo',
       Key='main/data/file.txt'
   )
   ```

### Repository and Object Errors

#### Error: "The specified bucket does not exist"

**Symptom:**
```
botocore.exceptions.ClientError: An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist
```

**Solutions:**

1. **Check Repository Exists**
   ```python
   def check_repository_exists(s3_client, repo_name):
       """Check if repository exists"""
       try:
           s3_client.head_bucket(Bucket=repo_name)
           print(f"✓ Repository '{repo_name}' exists")
           return True
       except s3_client.exceptions.NoSuchBucket:
           print(f"✗ Repository '{repo_name}' does not exist")
           return False
       except Exception as e:
           print(f"✗ Error checking repository: {e}")
           return False
   
   # Usage
   check_repository_exists(s3_client, 'my-repo')
   ```

2. **List Available Repositories**
   ```python
   def list_repositories(s3_client):
       """List all available repositories"""
       try:
           response = s3_client.list_buckets()
           repos = [bucket['Name'] for bucket in response['Buckets']]
           print(f"Available repositories: {repos}")
           return repos
       except Exception as e:
           print(f"Error listing repositories: {e}")
           return []
   
   # Usage
   available_repos = list_repositories(s3_client)
   ```

3. **Create Repository (if needed)**
   ```bash
   # Create repository using lakectl
   lakectl repo create lakefs://my-repo s3://my-storage-bucket/path/
   ```

#### Error: "The specified key does not exist"

**Symptom:**
```
botocore.exceptions.ClientError: An error occurred (NoSuchKey) when calling the GetObject operation: The specified key does not exist
```

**Solutions:**

1. **Check Object Path Format**
   ```python
   # ✗ Wrong - missing branch
   key = 'data/file.txt'
   
   # ✓ Correct - includes branch
   key = 'main/data/file.txt'
   
   # ✓ Correct - specific commit
   key = 'c1a2b3c4d5e6f7g8/data/file.txt'
   ```

2. **List Objects to Verify Path**
   ```python
   def find_object(s3_client, bucket, partial_key):
       """Find objects matching partial key"""
       try:
           response = s3_client.list_objects_v2(
               Bucket=bucket,
               Prefix=partial_key
           )
           
           objects = response.get('Contents', [])
           if objects:
               print(f"Found {len(objects)} matching objects:")
               for obj in objects[:10]:  # Show first 10
                   print(f"  {obj['Key']}")
           else:
               print(f"No objects found with prefix: {partial_key}")
           
           return [obj['Key'] for obj in objects]
           
       except Exception as e:
           print(f"Error searching objects: {e}")
           return []
   
   # Usage
   find_object(s3_client, 'my-repo', 'main/data/')
   ```

3. **Check Branch Exists**
   ```bash
   # List branches using lakectl
   lakectl branch list lakefs://my-repo
   ```

## Migration Patterns

### S3 to lakeFS Migration

#### Pattern 1: Direct Replacement

**Before (Pure S3):**
```python
import boto3

# Original S3 configuration
s3_client = boto3.client('s3', region_name='us-east-1')

def process_data():
    # Read from S3
    response = s3_client.get_object(
        Bucket='data-bucket',
        Key='input/data.csv'
    )
    
    # Process data
    data = response['Body'].read()
    processed = process_csv_data(data)
    
    # Write to S3
    s3_client.put_object(
        Bucket='results-bucket',
        Key='output/processed.csv',
        Body=processed
    )
```

**After (lakeFS):**
```python
import boto3

# lakeFS configuration - only endpoint changes
s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

def process_data():
    # Read from lakeFS - add branch to path
    response = s3_client.get_object(
        Bucket='data-repo',           # Repository name
        Key='main/input/data.csv'     # Branch + path
    )
    
    # Process data (unchanged)
    data = response['Body'].read()
    processed = process_csv_data(data)
    
    # Write to lakeFS - add branch to path
    s3_client.put_object(
        Bucket='results-repo',        # Repository name
        Key='main/output/processed.csv',  # Branch + path
        Body=processed
    )
```

#### Pattern 2: Gradual Migration with Environment Variables

```python
import boto3
import os

class AdaptiveS3Client:
    """S3 client that adapts based on configuration"""
    
    def __init__(self):
        self.use_lakefs = os.getenv('USE_LAKEFS', 'false').lower() == 'true'
        self.s3_client = self._create_client()
    
    def _create_client(self):
        if self.use_lakefs:
            return boto3.client('s3',
                endpoint_url=os.getenv('LAKEFS_ENDPOINT', 'http://localhost:8000'),
                aws_access_key_id=os.getenv('LAKEFS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('LAKEFS_SECRET_ACCESS_KEY')
            )
        else:
            return boto3.client('s3',
                region_name=os.getenv('AWS_REGION', 'us-east-1')
            )
    
    def get_bucket_name(self, logical_name):
        """Get actual bucket/repository name"""
        if self.use_lakefs:
            return f"lakefs-{logical_name}"
        else:
            return f"s3-{logical_name}"
    
    def get_object_key(self, logical_path):
        """Get actual object key"""
        if self.use_lakefs:
            return f"main/{logical_path}"  # Add branch prefix
        else:
            return logical_path
    
    def get_object(self, logical_bucket, logical_key):
        """Get object with automatic path translation"""
        return self.s3_client.get_object(
            Bucket=self.get_bucket_name(logical_bucket),
            Key=self.get_object_key(logical_key)
        )
    
    def put_object(self, logical_bucket, logical_key, body, **kwargs):
        """Put object with automatic path translation"""
        return self.s3_client.put_object(
            Bucket=self.get_bucket_name(logical_bucket),
            Key=self.get_object_key(logical_key),
            Body=body,
            **kwargs
        )

# Usage - same code works for both S3 and lakeFS
client = AdaptiveS3Client()

# This works with both S3 and lakeFS
response = client.get_object('data', 'input/file.csv')
data = response['Body'].read()

processed = process_data(data)

client.put_object('results', 'output/processed.csv', processed)
```

#### Pattern 3: Side-by-Side Comparison

```python
class MigrationValidator:
    """Validate migration by comparing S3 and lakeFS results"""
    
    def __init__(self):
        # S3 client
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        
        # lakeFS client
        self.lakefs_client = boto3.client('s3',
            endpoint_url='http://localhost:8000',
            aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
            aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        )
    
    def compare_operations(self, s3_bucket, lakefs_repo, key_mapping):
        """Compare operations between S3 and lakeFS"""
        
        results = {
            'matching': [],
            'different': [],
            'errors': []
        }
        
        for s3_key, lakefs_key in key_mapping.items():
            try:
                # Get from S3
                s3_response = self.s3_client.get_object(
                    Bucket=s3_bucket,
                    Key=s3_key
                )
                s3_content = s3_response['Body'].read()
                
                # Get from lakeFS
                lakefs_response = self.lakefs_client.get_object(
                    Bucket=lakefs_repo,
                    Key=lakefs_key
                )
                lakefs_content = lakefs_response['Body'].read()
                
                # Compare content
                if s3_content == lakefs_content:
                    results['matching'].append(s3_key)
                else:
                    results['different'].append({
                        's3_key': s3_key,
                        'lakefs_key': lakefs_key,
                        's3_size': len(s3_content),
                        'lakefs_size': len(lakefs_content)
                    })
                    
            except Exception as e:
                results['errors'].append({
                    's3_key': s3_key,
                    'lakefs_key': lakefs_key,
                    'error': str(e)
                })
        
        return results
    
    def migration_report(self, results):
        """Generate migration validation report"""
        
        total = len(results['matching']) + len(results['different']) + len(results['errors'])
        
        print(f"Migration Validation Report")
        print(f"=" * 40)
        print(f"Total objects checked: {total}")
        print(f"Matching: {len(results['matching'])} ({len(results['matching'])/total*100:.1f}%)")
        print(f"Different: {len(results['different'])} ({len(results['different'])/total*100:.1f}%)")
        print(f"Errors: {len(results['errors'])} ({len(results['errors'])/total*100:.1f}%)")
        
        if results['different']:
            print(f"\nDifferent objects:")
            for diff in results['different']:
                print(f"  {diff['s3_key']} → {diff['lakefs_key']}")
                print(f"    S3 size: {diff['s3_size']}, lakeFS size: {diff['lakefs_size']}")
        
        if results['errors']:
            print(f"\nErrors:")
            for error in results['errors']:
                print(f"  {error['s3_key']} → {error['lakefs_key']}: {error['error']}")

# Usage
validator = MigrationValidator()

# Define key mappings (S3 key → lakeFS key)
key_mapping = {
    'data/file1.csv': 'main/data/file1.csv',
    'data/file2.json': 'main/data/file2.json',
    'results/output.txt': 'main/results/output.txt'
}

results = validator.compare_operations('s3-bucket', 'lakefs-repo', key_mapping)
validator.migration_report(results)
```

### Common Migration Challenges

#### Challenge 1: Path Structure Differences

**Problem:** S3 uses flat key structure, lakeFS uses branch/path structure.

**Solution:**
```python
def convert_s3_path_to_lakefs(s3_key, branch='main'):
    """Convert S3 key to lakeFS key format"""
    return f"{branch}/{s3_key}"

def convert_lakefs_path_to_s3(lakefs_key):
    """Convert lakeFS key to S3 key format"""
    # Remove branch prefix
    parts = lakefs_key.split('/', 1)
    if len(parts) > 1:
        return parts[1]  # Return path without branch
    return lakefs_key

# Usage
s3_key = 'data/users/profile.json'
lakefs_key = convert_s3_path_to_lakefs(s3_key)  # 'main/data/users/profile.json'

original_key = convert_lakefs_path_to_s3(lakefs_key)  # 'data/users/profile.json'
```

#### Challenge 2: Batch Operations

**Problem:** Need to migrate large numbers of objects efficiently.

**Solution:**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class BatchMigrator:
    """Efficiently migrate objects in batches"""
    
    def __init__(self, s3_client, lakefs_client, max_workers=10):
        self.s3_client = s3_client
        self.lakefs_client = lakefs_client
        self.max_workers = max_workers
    
    def migrate_objects(self, s3_bucket, lakefs_repo, object_list, branch='main'):
        """Migrate objects in parallel"""
        
        def migrate_single_object(s3_key):
            try:
                # Download from S3
                response = self.s3_client.get_object(
                    Bucket=s3_bucket,
                    Key=s3_key
                )
                
                # Upload to lakeFS
                self.lakefs_client.put_object(
                    Bucket=lakefs_repo,
                    Key=f"{branch}/{s3_key}",
                    Body=response['Body'].read(),
                    ContentType=response.get('ContentType', 'binary/octet-stream'),
                    Metadata=response.get('Metadata', {})
                )
                
                return {'status': 'success', 'key': s3_key}
                
            except Exception as e:
                return {'status': 'error', 'key': s3_key, 'error': str(e)}
        
        # Execute migrations in parallel
        results = {'success': 0, 'errors': 0, 'details': []}
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_key = {
                executor.submit(migrate_single_object, key): key 
                for key in object_list
            }
            
            # Process results
            for future in as_completed(future_to_key):
                result = future.result()
                results['details'].append(result)
                
                if result['status'] == 'success':
                    results['success'] += 1
                    if results['success'] % 100 == 0:
                        print(f"Migrated {results['success']} objects...")
                else:
                    results['errors'] += 1
                    print(f"Error migrating {result['key']}: {result['error']}")
        
        return results
    
    def get_all_s3_objects(self, bucket, prefix=''):
        """Get list of all objects in S3 bucket"""
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                objects.append(obj['Key'])
        
        return objects

# Usage
migrator = BatchMigrator(s3_client, lakefs_client, max_workers=20)

# Get all objects to migrate
objects_to_migrate = migrator.get_all_s3_objects('source-s3-bucket')
print(f"Found {len(objects_to_migrate)} objects to migrate")

# Migrate in batches
batch_size = 1000
for i in range(0, len(objects_to_migrate), batch_size):
    batch = objects_to_migrate[i:i+batch_size]
    print(f"Migrating batch {i//batch_size + 1} ({len(batch)} objects)...")
    
    results = migrator.migrate_objects('source-s3-bucket', 'target-lakefs-repo', batch)
    print(f"Batch completed: {results['success']} success, {results['errors']} errors")
    
    # Brief pause between batches
    time.sleep(1)
```

#### Challenge 3: Metadata Preservation

**Problem:** Preserving S3 object metadata during migration.

**Solution:**
```python
def migrate_with_metadata(s3_client, lakefs_client, s3_bucket, lakefs_repo, s3_key, lakefs_key):
    """Migrate object while preserving all metadata"""
    
    try:
        # Get object with metadata
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        
        # Extract all metadata
        content = response['Body'].read()
        content_type = response.get('ContentType', 'binary/octet-stream')
        metadata = response.get('Metadata', {})
        
        # Additional S3 metadata to preserve
        additional_metadata = {}
        if 'CacheControl' in response:
            additional_metadata['cache-control'] = response['CacheControl']
        if 'ContentDisposition' in response:
            additional_metadata['content-disposition'] = response['ContentDisposition']
        if 'ContentEncoding' in response:
            additional_metadata['content-encoding'] = response['ContentEncoding']
        if 'ContentLanguage' in response:
            additional_metadata['content-language'] = response['ContentLanguage']
        
        # Combine metadata
        all_metadata = {**metadata, **additional_metadata}
        
        # Upload to lakeFS with preserved metadata
        put_args = {
            'Bucket': lakefs_repo,
            'Key': lakefs_key,
            'Body': content,
            'ContentType': content_type
        }
        
        if all_metadata:
            put_args['Metadata'] = all_metadata
        
        if 'CacheControl' in response:
            put_args['CacheControl'] = response['CacheControl']
        if 'ContentDisposition' in response:
            put_args['ContentDisposition'] = response['ContentDisposition']
        if 'ContentEncoding' in response:
            put_args['ContentEncoding'] = response['ContentEncoding']
        if 'ContentLanguage' in response:
            put_args['ContentLanguage'] = response['ContentLanguage']
        
        lakefs_client.put_object(**put_args)
        
        print(f"✓ Migrated {s3_key} with metadata")
        return True
        
    except Exception as e:
        print(f"✗ Failed to migrate {s3_key}: {e}")
        return False

# Usage
success = migrate_with_metadata(
    s3_client, lakefs_client,
    's3-bucket', 'lakefs-repo',
    'data/file.csv', 'main/data/file.csv'
)
```

## Performance Troubleshooting

### Slow Operations

**Symptoms:**
- Long response times for S3 operations
- Timeouts during large file uploads
- Poor performance compared to direct S3

**Solutions:**

1. **Optimize Connection Settings**
   ```python
   from botocore.config import Config
   
   # Optimize for performance
   config = Config(
       max_pool_connections=50,
       retries={'max_attempts': 3, 'mode': 'adaptive'},
       connect_timeout=10,
       read_timeout=60
   )
   
   s3_client = boto3.client('s3',
       endpoint_url='http://localhost:8000',
       config=config
   )
   ```

2. **Use Multipart Upload for Large Files**
   ```python
   def upload_large_file(s3_client, bucket, key, file_path, part_size=100*1024*1024):
       """Upload large file using multipart upload"""
       
       file_size = os.path.getsize(file_path)
       if file_size < part_size:
           # Use regular upload for small files
           s3_client.upload_file(file_path, bucket, key)
           return
       
       # Use multipart upload
       response = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
       upload_id = response['UploadId']
       
       parts = []
       part_number = 1
       
       try:
           with open(file_path, 'rb') as f:
               while True:
                   data = f.read(part_size)
                   if not data:
                       break
                   
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
                   
                   part_number += 1
           
           # Complete upload
           s3_client.complete_multipart_upload(
               Bucket=bucket,
               Key=key,
               UploadId=upload_id,
               MultipartUpload={'Parts': parts}
           )
           
       except Exception as e:
           # Abort on error
           s3_client.abort_multipart_upload(
               Bucket=bucket,
               Key=key,
               UploadId=upload_id
           )
           raise e
   ```

3. **Monitor Performance**
   ```python
   import time
   from contextlib import contextmanager
   
   @contextmanager
   def measure_time(operation_name):
       """Measure operation time"""
       start = time.time()
       try:
           yield
       finally:
           duration = time.time() - start
           print(f"{operation_name} took {duration:.2f} seconds")
   
   # Usage
   with measure_time("List buckets"):
       response = s3_client.list_buckets()
   
   with measure_time("Upload file"):
       s3_client.upload_file('large-file.zip', 'my-repo', 'main/data/large-file.zip')
   ```

### Memory Issues

**Symptoms:**
- Out of memory errors during large file operations
- High memory usage during batch operations

**Solutions:**

1. **Stream Large Files**
   ```python
   def stream_large_file(s3_client, bucket, key, local_path):
       """Stream large file without loading into memory"""
       
       with open(local_path, 'rb') as f:
           s3_client.upload_fileobj(f, bucket, key)
   
   def download_large_file(s3_client, bucket, key, local_path):
       """Download large file with streaming"""
       
       with open(local_path, 'wb') as f:
           s3_client.download_fileobj(bucket, key, f)
   ```

2. **Process in Chunks**
   ```python
   def process_large_object_in_chunks(s3_client, bucket, key, chunk_size=1024*1024):
       """Process large object in chunks"""
       
       # Get object size
       response = s3_client.head_object(Bucket=bucket, Key=key)
       object_size = response['ContentLength']
       
       # Process in chunks
       for start in range(0, object_size, chunk_size):
           end = min(start + chunk_size - 1, object_size - 1)
           
           # Download chunk
           response = s3_client.get_object(
               Bucket=bucket,
               Key=key,
               Range=f'bytes={start}-{end}'
           )
           
           chunk_data = response['Body'].read()
           
           # Process chunk
           process_chunk(chunk_data)
           
           print(f"Processed bytes {start}-{end} of {object_size}")
   ```

## Debugging Tools

### Enable Debug Logging

```python
import logging
import boto3

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('boto3')
logger.setLevel(logging.DEBUG)
logger = logging.getLogger('botocore')
logger.setLevel(logging.DEBUG)

# Create client with debug logging
s3_client = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Operations will now show detailed debug information
s3_client.list_buckets()
```

### Request/Response Inspection

```python
import boto3
from botocore.awsrequest import AWSRequest

class DebugS3Client:
    """S3 client wrapper with request/response debugging"""
    
    def __init__(self, **kwargs):
        self.client = boto3.client('s3', **kwargs)
        
        # Add event handlers for debugging
        self.client.meta.events.register('before-call', self._log_request)
        self.client.meta.events.register('after-call', self._log_response)
    
    def _log_request(self, event_name, **kwargs):
        """Log request details"""
        print(f"\n--- REQUEST ---")
        print(f"Operation: {kwargs.get('operation_name')}")
        print(f"Params: {kwargs.get('params', {})}")
    
    def _log_response(self, event_name, **kwargs):
        """Log response details"""
        print(f"\n--- RESPONSE ---")
        if 'parsed' in kwargs:
            response = kwargs['parsed']
            print(f"Status: {response.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
            print(f"Headers: {response.get('ResponseMetadata', {}).get('HTTPHeaders', {})}")
    
    def __getattr__(self, name):
        """Delegate to underlying client"""
        return getattr(self.client, name)

# Usage
debug_client = DebugS3Client(
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# This will show detailed request/response information
debug_client.list_buckets()
```

### Health Check Script

```python
#!/usr/bin/env python3
"""
lakeFS Boto3 Health Check Script
Run this script to diagnose common issues
"""

import boto3
import os
import sys
import requests
from botocore.exceptions import ClientError, NoCredentialsError

def check_environment():
    """Check environment configuration"""
    print("=== Environment Check ===")
    
    required_vars = ['LAKEFS_ENDPOINT', 'LAKEFS_ACCESS_KEY_ID', 'LAKEFS_SECRET_ACCESS_KEY']
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✓ {var}: {'*' * len(value)}")
        else:
            print(f"✗ {var}: NOT SET")
            missing_vars.append(var)
    
    return len(missing_vars) == 0

def check_connectivity():
    """Check lakeFS connectivity"""
    print("\n=== Connectivity Check ===")
    
    endpoint = os.getenv('LAKEFS_ENDPOINT', 'http://localhost:8000')
    
    try:
        # Test HTTP connectivity
        response = requests.get(f"{endpoint}/api/v1/healthcheck", timeout=10)
        if response.status_code == 200:
            print(f"✓ lakeFS server reachable at {endpoint}")
            return True
        else:
            print(f"✗ lakeFS server returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Cannot reach lakeFS server: {e}")
        return False

def check_credentials():
    """Check credential validity"""
    print("\n=== Credentials Check ===")
    
    try:
        s3_client = boto3.client('s3',
            endpoint_url=os.getenv('LAKEFS_ENDPOINT'),
            aws_access_key_id=os.getenv('LAKEFS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('LAKEFS_SECRET_ACCESS_KEY')
        )
        
        response = s3_client.list_buckets()
        repos = response.get('Buckets', [])
        print(f"✓ Credentials valid. Found {len(repos)} repositories")
        
        if repos:
            print("  Repositories:")
            for repo in repos[:5]:  # Show first 5
                print(f"    - {repo['Name']}")
        
        return True
        
    except NoCredentialsError:
        print("✗ No credentials found")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidAccessKeyId':
            print("✗ Invalid access key ID")
        elif error_code == 'SignatureDoesNotMatch':
            print("✗ Invalid secret access key")
        else:
            print(f"✗ Credential error: {error_code}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def check_operations():
    """Check basic operations"""
    print("\n=== Operations Check ===")
    
    try:
        s3_client = boto3.client('s3',
            endpoint_url=os.getenv('LAKEFS_ENDPOINT'),
            aws_access_key_id=os.getenv('LAKEFS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('LAKEFS_SECRET_ACCESS_KEY')
        )
        
        # Get first repository
        response = s3_client.list_buckets()
        repos = response.get('Buckets', [])
        
        if not repos:
            print("⚠ No repositories found for testing")
            return True
        
        test_repo = repos[0]['Name']
        print(f"Testing operations on repository: {test_repo}")
        
        # Test list objects
        try:
            response = s3_client.list_objects_v2(Bucket=test_repo, MaxKeys=1)
            print("✓ List objects successful")
        except Exception as e:
            print(f"✗ List objects failed: {e}")
            return False
        
        # Test put/get/delete object
        test_key = 'main/health-check-test.txt'
        test_content = b'Health check test content'
        
        try:
            # Put object
            s3_client.put_object(
                Bucket=test_repo,
                Key=test_key,
                Body=test_content
            )
            print("✓ Put object successful")
            
            # Get object
            response = s3_client.get_object(Bucket=test_repo, Key=test_key)
            retrieved_content = response['Body'].read()
            
            if retrieved_content == test_content:
                print("✓ Get object successful")
            else:
                print("✗ Get object content mismatch")
                return False
            
            # Delete object
            s3_client.delete_object(Bucket=test_repo, Key=test_key)
            print("✓ Delete object successful")
            
        except Exception as e:
            print(f"✗ Object operations failed: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"✗ Operations check failed: {e}")
        return False

def main():
    """Run all health checks"""
    print("lakeFS Boto3 Health Check")
    print("=" * 40)
    
    checks = [
        ("Environment", check_environment),
        ("Connectivity", check_connectivity),
        ("Credentials", check_credentials),
        ("Operations", check_operations)
    ]
    
    results = {}
    for name, check_func in checks:
        results[name] = check_func()
    
    print("\n=== Summary ===")
    all_passed = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All checks passed! Your Boto3 + lakeFS setup is working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Some checks failed. Please review the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

Save this as `lakefs_health_check.py` and run:

```bash
# Set environment variables
export LAKEFS_ENDPOINT=http://localhost:8000
export LAKEFS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export LAKEFS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Run health check
python lakefs_health_check.py
```

## Next Steps

- Review [S3 operations](s3-operations.md) for detailed operation examples
- Explore [S3 Router](s3-router.md) for hybrid workflows
- Check [configuration guide](configuration.md) for advanced setup
- Visit [best practices](../reference/best-practices.md) for production guidance

## See Also

- [High-Level SDK](../high-level-sdk/) - For advanced lakeFS features
- [Generated SDK](../generated-sdk/) - For direct API access
- [General troubleshooting](../reference/troubleshooting.md) - Cross-SDK issues