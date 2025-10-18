---
title: Troubleshooting Guide
description: Comprehensive troubleshooting guide for Python lakeFS integrations
sdk_types: ["high-level", "generated", "lakefs-spec", "boto3"]
difficulty: "intermediate"
use_cases: ["debugging", "error-resolution", "performance"]
---

# Troubleshooting Guide

This comprehensive guide covers common issues, error patterns, debugging techniques, and performance optimization tips for Python lakeFS integrations across all SDK options.

## Common Error Types and Solutions

### Authentication and Connection Errors

#### Error: `Unauthorized (401)`

**Symptom:**
```
lakefs.exceptions.UnauthorizedException: Unauthorized
```

**Causes and Solutions:**

1. **Invalid Credentials**
   ```python
   # Check your credentials
   import lakefs
   
   # Verify credentials are set correctly
   client = lakefs.Client(
       host="http://localhost:8000",
       access_key_id="your-access-key",
       secret_access_key="your-secret-key"
   )
   
   # Test connection
   try:
       repos = list(client.repositories.list(amount=1))
       print("Authentication successful")
   except lakefs.exceptions.UnauthorizedException:
       print("Invalid credentials")
   ```

2. **Environment Variables Not Set**
   ```bash
   # Check environment variables
   echo $LAKEFS_ACCESS_KEY_ID
   echo $LAKEFS_SECRET_ACCESS_KEY
   echo $LAKEFS_HOST
   
   # Set if missing
   export LAKEFS_ACCESS_KEY_ID="your-access-key"
   export LAKEFS_SECRET_ACCESS_KEY="your-secret-key"
   export LAKEFS_HOST="http://localhost:8000"
   ```

3. **Token Expiration (for JWT tokens)**
   ```python
   # Refresh token if using JWT authentication
   import lakefs
   
   def refresh_client_token():
       # Re-authenticate to get fresh token
       client = lakefs.Client()
       # Your token refresh logic here
       return client
   ```

#### Error: `Connection refused` or `Connection timeout`

**Symptom:**
```
requests.exceptions.ConnectionError: HTTPConnectionPool(host='localhost', port=8000): 
Max retries exceeded with url: /api/v1/repositories
```

**Solutions:**

1. **Verify lakeFS Server is Running**
   ```bash
   # Check if lakeFS is running
   curl http://localhost:8000/api/v1/healthcheck
   
   # Check Docker container status
   docker ps | grep lakefs
   
   # Check service status
   systemctl status lakefs
   ```

2. **Check Network Configuration**
   ```python
   import requests
   
   # Test basic connectivity
   try:
       response = requests.get("http://localhost:8000/api/v1/healthcheck", timeout=5)
       print(f"Server responding: {response.status_code}")
   except requests.exceptions.ConnectionError:
       print("Cannot connect to lakeFS server")
   except requests.exceptions.Timeout:
       print("Connection timeout - server may be slow")
   ```

3. **Proxy Configuration Issues**
   ```python
   import lakefs
   
   # Configure proxy settings
   client = lakefs.Client(
       host="http://localhost:8000",
       access_key_id="your-key",
       secret_access_key="your-secret",
       # Proxy configuration
       proxies={
           'http': 'http://proxy.company.com:8080',
           'https': 'https://proxy.company.com:8080'
       }
   )
   ```

#### Error: SSL Certificate Verification Failed

**Symptom:**
```
requests.exceptions.SSLError: HTTPSConnectionPool(host='lakefs.example.com', port=443): 
Max retries exceeded with url: /api/v1/repositories (Caused by SSLError(SSLCertVerificationError))
```

**Solutions:**

1. **Configure SSL Verification**
   ```python
   import lakefs
   
   # Option 1: Disable SSL verification (NOT recommended for production)
   client = lakefs.Client(
       host="https://lakefs.example.com",
       access_key_id="your-key",
       secret_access_key="your-secret",
       verify_ssl=False
   )
   
   # Option 2: Provide custom CA certificate
   client = lakefs.Client(
       host="https://lakefs.example.com",
       access_key_id="your-key",
       secret_access_key="your-secret",
       ssl_ca_cert="/path/to/ca-bundle.crt"
   )
   
   # Option 3: Use system CA bundle
   import certifi
   client = lakefs.Client(
       host="https://lakefs.example.com",
       access_key_id="your-key",
       secret_access_key="your-secret",
       ssl_ca_cert=certifi.where()
   )
   ```

### Repository and Branch Errors

#### Error: `Repository not found (404)`

**Symptom:**
```
lakefs.exceptions.NotFoundException: Repository 'my-repo' not found
```

**Solutions:**

1. **Verify Repository Exists**
   ```python
   import lakefs
   
   client = lakefs.Client()
   
   # List all repositories
   repos = list(client.repositories.list())
   print("Available repositories:")
   for repo in repos:
       print(f"  - {repo.id}")
   
   # Check specific repository
   try:
       repo = client.repository("my-repo")
       print(f"Repository found: {repo.id}")
   except lakefs.exceptions.NotFoundException:
       print("Repository does not exist")
   ```

2. **Create Repository if Missing**
   ```python
   def ensure_repository_exists(client, repo_name, storage_namespace):
       try:
           return client.repository(repo_name)
       except lakefs.exceptions.NotFoundException:
           print(f"Creating repository: {repo_name}")
           return client.repositories.create(
               name=repo_name,
               storage_namespace=storage_namespace
           )
   ```

#### Error: `Branch not found (404)`

**Symptom:**
```
lakefs.exceptions.NotFoundException: Branch 'feature-branch' not found in repository 'my-repo'
```

**Solutions:**

1. **List Available Branches**
   ```python
   repo = client.repository("my-repo")
   
   # List all branches
   branches = list(repo.branches.list())
   print("Available branches:")
   for branch in branches:
       print(f"  - {branch.id}")
   ```

2. **Create Branch if Missing**
   ```python
   def ensure_branch_exists(repo, branch_name, source_branch="main"):
       try:
           return repo.branch(branch_name)
       except lakefs.exceptions.NotFoundException:
           print(f"Creating branch: {branch_name}")
           return repo.branches.create(
               name=branch_name,
               source_ref=source_branch
           )
   ```

### Object Operation Errors

#### Error: `Object not found (404)`

**Symptom:**
```
lakefs.exceptions.NotFoundException: Object 'path/to/file.txt' not found
```

**Solutions:**

1. **Verify Object Path and Branch**
   ```python
   repo = client.repository("my-repo")
   branch = repo.branch("main")
   
   # List objects to verify path
   objects = list(branch.objects.list(prefix="path/to/"))
   print("Objects in path:")
   for obj in objects:
       print(f"  - {obj.path}")
   
   # Check if object exists
   try:
       obj = branch.object("path/to/file.txt")
       stat = obj.stat()
       print(f"Object found: {stat.path}, size: {stat.size_bytes}")
   except lakefs.exceptions.NotFoundException:
       print("Object does not exist")
   ```

2. **Handle Missing Objects Gracefully**
   ```python
   def safe_download(branch, object_path, default_content=None):
       try:
           obj = branch.object(object_path)
           return obj.reader().read()
       except lakefs.exceptions.NotFoundException:
           if default_content is not None:
               return default_content
           raise
   ```

#### Error: `Conflict (409)` during Upload

**Symptom:**
```
lakefs.exceptions.ConflictException: Object 'path/to/file.txt' was modified
```

**Solutions:**

1. **Handle Concurrent Modifications**
   ```python
   import time
   import random
   
   def upload_with_retry(branch, path, data, max_retries=3):
       for attempt in range(max_retries):
           try:
               return branch.object(path).upload(data)
           except lakefs.exceptions.ConflictException:
               if attempt == max_retries - 1:
                   raise
               # Wait with exponential backoff
               wait_time = (2 ** attempt) + random.uniform(0, 1)
               time.sleep(wait_time)
   ```

2. **Use Transactions for Atomic Operations**
   ```python
   # High-Level SDK transaction
   with repo.branch("main").transaction() as tx:
       tx.upload("file1.txt", data1)
       tx.upload("file2.txt", data2)
       # All uploads succeed or all fail
   ```

### Performance Issues

#### Slow Upload/Download Performance

**Symptoms:**
- Uploads taking much longer than expected
- High memory usage during file operations
- Timeouts on large files

**Solutions:**

1. **Use Streaming for Large Files**
   ```python
   # Instead of loading entire file into memory
   # BAD:
   with open("large_file.dat", "rb") as f:
       data = f.read()  # Loads entire file into memory
       branch.object("large_file.dat").upload(data)
   
   # GOOD:
   with open("large_file.dat", "rb") as f:
       branch.object("large_file.dat").upload(f, mode='rb')
   ```

2. **Configure Connection Pooling**
   ```python
   import lakefs
   
   # Configure larger connection pool
   client = lakefs.Client(
       host="http://localhost:8000",
       access_key_id="your-key",
       secret_access_key="your-secret",
       pool_connections=30,
       pool_maxsize=30
   )
   ```

3. **Use Batch Operations**
   ```python
   from concurrent.futures import ThreadPoolExecutor, as_completed
   
   def upload_files_concurrently(branch, files, max_workers=10):
       def upload_single_file(file_info):
           path, data = file_info
           return branch.object(path).upload(data)
       
       with ThreadPoolExecutor(max_workers=max_workers) as executor:
           futures = [executor.submit(upload_single_file, file_info) 
                     for file_info in files]
           
           results = []
           for future in as_completed(futures):
               try:
                   result = future.result()
                   results.append(result)
               except Exception as e:
                   print(f"Upload failed: {e}")
           
           return results
   ```

#### Memory Usage Issues

**Symptoms:**
- High memory consumption
- Out of memory errors
- Memory leaks in long-running processes

**Solutions:**

1. **Use Streaming Readers**
   ```python
   # Instead of reading entire object into memory
   # BAD:
   obj = branch.object("large_file.dat")
   data = obj.reader().read()  # Loads entire file
   
   # GOOD:
   obj = branch.object("large_file.dat")
   with obj.reader() as reader:
       for chunk in reader.iter_chunks(chunk_size=8192):
           process_chunk(chunk)
   ```

2. **Implement Proper Resource Management**
   ```python
   import gc
   from contextlib import contextmanager
   
   @contextmanager
   def memory_managed_processing():
       try:
           yield
       finally:
           gc.collect()  # Force garbage collection
   
   # Usage
   with memory_managed_processing():
       # Process large amounts of data
       for file_path in large_file_list:
           process_file(file_path)
   ```

### SDK-Specific Issues

#### High-Level SDK Issues

**Error: Transaction Rollback**
```python
# Handle transaction failures gracefully
try:
    with repo.branch("main").transaction() as tx:
        tx.upload("file1.txt", data1)
        tx.upload("file2.txt", data2)
        # Some operation fails here
        raise Exception("Simulated failure")
except Exception as e:
    print(f"Transaction rolled back: {e}")
    # Handle rollback scenario
```

#### Generated SDK Issues

**Error: API Response Parsing**
```python
from lakefs_sdk import ApiException
import json

try:
    # Generated SDK API call
    response = api_client.list_repositories()
except ApiException as e:
    print(f"API Error: {e.status}")
    print(f"Reason: {e.reason}")
    
    # Parse error details
    try:
        error_details = json.loads(e.body)
        print(f"Error message: {error_details.get('message')}")
    except json.JSONDecodeError:
        print(f"Raw error: {e.body}")
```

#### lakefs-spec Issues

**Error: fsspec Configuration**
```python
import fsspec
import lakefs_spec

# Debug fsspec configuration
def debug_lakefs_spec():
    try:
        fs = fsspec.filesystem(
            'lakefs',
            host='http://localhost:8000',
            access_key_id='your-key',
            secret_access_key='your-secret'
        )
        
        # Test basic operation
        files = fs.ls('repo/branch/')
        print(f"Found {len(files)} files")
        
    except Exception as e:
        print(f"lakefs-spec error: {e}")
        
        # Check if lakefs-spec is properly installed
        try:
            import lakefs_spec
            print(f"lakefs-spec version: {lakefs_spec.__version__}")
        except ImportError:
            print("lakefs-spec not installed")
```

#### Boto3 Issues

**Error: S3 Compatibility Issues**
```python
import boto3
from botocore.exceptions import ClientError

# Debug Boto3 with lakeFS
def debug_boto3_lakefs():
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:8000',
        aws_access_key_id='your-key',
        aws_secret_access_key='your-secret'
    )
    
    try:
        # Test basic operation
        response = s3.list_objects_v2(Bucket='repo', Prefix='branch/')
        print(f"Found {response.get('KeyCount', 0)} objects")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"S3 Error {error_code}: {error_message}")
        
        # Common S3 compatibility issues
        if error_code == 'NoSuchBucket':
            print("Repository may not exist or incorrect format")
        elif error_code == 'AccessDenied':
            print("Check credentials and permissions")
```

## Debugging Techniques

### Enable Debug Logging

```python
import logging
import lakefs

# Enable debug logging for lakeFS
logging.basicConfig(level=logging.DEBUG)
lakefs_logger = logging.getLogger('lakefs')
lakefs_logger.setLevel(logging.DEBUG)

# Enable debug logging for requests
requests_logger = logging.getLogger('urllib3')
requests_logger.setLevel(logging.DEBUG)

# Enable debug logging for specific SDK
import lakefs_sdk
lakefs_sdk_logger = logging.getLogger('lakefs_sdk')
lakefs_sdk_logger.setLevel(logging.DEBUG)
```

### Network Traffic Inspection

```python
import requests
import lakefs

# Enable request/response logging
import http.client as http_client
http_client.HTTPConnection.debuglevel = 1

# Create client with debug session
session = requests.Session()
session.hooks['response'].append(lambda r, *args, **kwargs: print(f"Response: {r.status_code} {r.url}"))

client = lakefs.Client(session=session)
```

### Performance Profiling

```python
import cProfile
import pstats
import io

def profile_operation():
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Your lakeFS operations here
    client = lakefs.Client()
    repo = client.repository("my-repo")
    branch = repo.branch("main")
    
    # Perform operations to profile
    for i in range(100):
        branch.object(f"test_{i}.txt").upload(f"data_{i}")
    
    profiler.disable()
    
    # Analyze results
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.sort_stats('cumulative')
    ps.print_stats(20)
    
    print(s.getvalue())

profile_operation()
```

### Memory Usage Analysis

```python
import tracemalloc
import lakefs

def analyze_memory_usage():
    # Start tracing
    tracemalloc.start()
    
    # Your operations
    client = lakefs.Client()
    repo = client.repository("my-repo")
    branch = repo.branch("main")
    
    # Take snapshot before operations
    snapshot1 = tracemalloc.take_snapshot()
    
    # Perform memory-intensive operations
    large_data = "x" * (10 * 1024 * 1024)  # 10MB string
    for i in range(10):
        branch.object(f"large_{i}.txt").upload(large_data)
    
    # Take snapshot after operations
    snapshot2 = tracemalloc.take_snapshot()
    
    # Compare snapshots
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')
    
    print("Top 10 memory allocations:")
    for stat in top_stats[:10]:
        print(stat)

analyze_memory_usage()
```

## Performance Optimization Tips

### Connection Optimization

```python
import lakefs
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Create optimized session
session = requests.Session()

# Configure retry strategy
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)

# Configure adapter with retry strategy
adapter = HTTPAdapter(
    pool_connections=20,
    pool_maxsize=20,
    max_retries=retry_strategy
)

session.mount("http://", adapter)
session.mount("https://", adapter)

# Use optimized session with client
client = lakefs.Client(session=session)
```

### Batch Processing Optimization

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class OptimizedBatchProcessor:
    def __init__(self, client, max_workers=10, batch_size=100):
        self.client = client
        self.max_workers = max_workers
        self.batch_size = batch_size
    
    def process_files_in_batches(self, repo_name, branch_name, files):
        repo = self.client.repository(repo_name)
        branch = repo.branch(branch_name)
        
        # Process files in batches
        for i in range(0, len(files), self.batch_size):
            batch = files[i:i + self.batch_size]
            self._process_batch(branch, batch)
            
            # Small delay between batches to avoid overwhelming server
            time.sleep(0.1)
    
    def _process_batch(self, branch, batch):
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for file_info in batch:
                future = executor.submit(self._process_single_file, branch, file_info)
                futures.append(future)
            
            # Wait for all files in batch to complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    print(f"Processed: {result}")
                except Exception as e:
                    print(f"Error processing file: {e}")
    
    def _process_single_file(self, branch, file_info):
        path, data = file_info
        return branch.object(path).upload(data)
```

### Caching Strategies

```python
import functools
import time
from typing import Dict, Any

class CachedLakeFSClient:
    def __init__(self, client, cache_ttl=300):  # 5 minutes
        self.client = client
        self.cache_ttl = cache_ttl
        self._cache = {}
    
    def _is_cache_valid(self, key):
        if key not in self._cache:
            return False
        return time.time() - self._cache[key]['timestamp'] < self.cache_ttl
    
    def get_repository_cached(self, repo_name):
        cache_key = f"repo:{repo_name}"
        
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['data']
        
        repo = self.client.repository(repo_name)
        self._cache[cache_key] = {
            'data': repo,
            'timestamp': time.time()
        }
        
        return repo
    
    @functools.lru_cache(maxsize=128)
    def get_branch_metadata(self, repo_name, branch_name):
        """Cache branch metadata using LRU cache"""
        repo = self.get_repository_cached(repo_name)
        branch = repo.branch(branch_name)
        
        return {
            'id': branch.id,
            'commit_id': branch.head.id if branch.head else None
        }
```

## Diagnostic Tools and Scripts

### Health Check Script

```python
#!/usr/bin/env python3
"""
lakeFS Python SDK Health Check Script
"""

import sys
import time
import lakefs
from typing import Dict, Any

def check_connectivity(client: lakefs.Client) -> Dict[str, Any]:
    """Check basic connectivity to lakeFS server"""
    try:
        start_time = time.time()
        repos = list(client.repositories.list(amount=1))
        response_time = time.time() - start_time
        
        return {
            'status': 'pass',
            'response_time': response_time,
            'repository_count': len(repos)
        }
    except Exception as e:
        return {
            'status': 'fail',
            'error': str(e)
        }

def check_authentication(client: lakefs.Client) -> Dict[str, Any]:
    """Check authentication status"""
    try:
        # Try to perform an authenticated operation
        repos = list(client.repositories.list(amount=1))
        return {'status': 'pass'}
    except lakefs.exceptions.UnauthorizedException:
        return {'status': 'fail', 'error': 'Invalid credentials'}
    except Exception as e:
        return {'status': 'fail', 'error': str(e)}

def check_repository_operations(client: lakefs.Client, repo_name: str) -> Dict[str, Any]:
    """Check repository operations"""
    try:
        repo = client.repository(repo_name)
        branches = list(repo.branches.list(amount=1))
        
        return {
            'status': 'pass',
            'branch_count': len(branches)
        }
    except lakefs.exceptions.NotFoundException:
        return {'status': 'fail', 'error': f'Repository {repo_name} not found'}
    except Exception as e:
        return {'status': 'fail', 'error': str(e)}

def main():
    """Run comprehensive health check"""
    print("lakeFS Python SDK Health Check")
    print("=" * 40)
    
    try:
        client = lakefs.Client()
    except Exception as e:
        print(f"Failed to create client: {e}")
        sys.exit(1)
    
    # Run checks
    checks = {
        'Connectivity': check_connectivity(client),
        'Authentication': check_authentication(client),
    }
    
    # Add repository check if specified
    if len(sys.argv) > 1:
        repo_name = sys.argv[1]
        checks[f'Repository ({repo_name})'] = check_repository_operations(client, repo_name)
    
    # Print results
    all_passed = True
    for check_name, result in checks.items():
        status = "✓ PASS" if result['status'] == 'pass' else "✗ FAIL"
        print(f"{check_name}: {status}")
        
        if result['status'] == 'fail':
            all_passed = False
            print(f"  Error: {result.get('error', 'Unknown error')}")
        elif 'response_time' in result:
            print(f"  Response time: {result['response_time']:.3f}s")
    
    print("\n" + "=" * 40)
    if all_passed:
        print("All checks passed!")
        sys.exit(0)
    else:
        print("Some checks failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### Configuration Validator

```python
#!/usr/bin/env python3
"""
lakeFS Configuration Validator
"""

import os
import sys
import requests
import lakefs
from urllib.parse import urlparse

def validate_environment_variables():
    """Validate required environment variables"""
    required_vars = [
        'LAKEFS_HOST',
        'LAKEFS_ACCESS_KEY_ID',
        'LAKEFS_SECRET_ACCESS_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("✓ All required environment variables are set")
    return True

def validate_host_format(host: str):
    """Validate host URL format"""
    try:
        parsed = urlparse(host)
        if not parsed.scheme or not parsed.netloc:
            print(f"✗ Invalid host format: {host}")
            return False
        
        print(f"✓ Host format is valid: {host}")
        return True
    except Exception as e:
        print(f"✗ Error parsing host: {e}")
        return False

def validate_connectivity(host: str):
    """Validate network connectivity"""
    try:
        response = requests.get(f"{host}/api/v1/healthcheck", timeout=10)
        if response.status_code == 200:
            print("✓ Server is reachable and healthy")
            return True
        else:
            print(f"✗ Server returned status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to server")
        return False
    except requests.exceptions.Timeout:
        print("✗ Connection timeout")
        return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False

def main():
    """Run configuration validation"""
    print("lakeFS Configuration Validator")
    print("=" * 40)
    
    all_valid = True
    
    # Check environment variables
    if not validate_environment_variables():
        all_valid = False
    
    # Check host format
    host = os.environ.get('LAKEFS_HOST', '')
    if host and not validate_host_format(host):
        all_valid = False
    
    # Check connectivity
    if host and not validate_connectivity(host):
        all_valid = False
    
    # Test client creation
    try:
        client = lakefs.Client()
        print("✓ Client created successfully")
    except Exception as e:
        print(f"✗ Failed to create client: {e}")
        all_valid = False
    
    print("\n" + "=" * 40)
    if all_valid:
        print("Configuration is valid!")
        sys.exit(0)
    else:
        print("Configuration has issues!")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

## Getting Help

### Community Resources

- **GitHub Issues**: [lakeFS Python SDK Issues](https://github.com/treeverse/lakeFS/issues)
- **Slack Community**: [Join lakeFS Slack](https://lakefs.io/slack)
- **Documentation**: [Official lakeFS Documentation](https://docs.lakefs.io)

### Reporting Issues

When reporting issues, please include:

1. **Environment Information**:
   ```python
   import sys
   import lakefs
   import platform
   
   print(f"Python version: {sys.version}")
   print(f"lakeFS SDK version: {lakefs.__version__}")
   print(f"Platform: {platform.platform()}")
   ```

2. **Minimal Reproduction Case**:
   ```python
   # Provide minimal code that reproduces the issue
   import lakefs
   
   client = lakefs.Client(host="...", access_key_id="...", secret_access_key="...")
   # Steps that cause the issue
   ```

3. **Error Messages**: Include full error messages and stack traces

4. **Configuration**: Describe your lakeFS server setup and network configuration

### Advanced Debugging

For complex issues, consider:

1. **Enable verbose logging** for all components
2. **Use network traffic inspection** tools like Wireshark
3. **Profile memory and CPU usage** during operations
4. **Test with minimal configurations** to isolate issues
5. **Compare behavior** across different SDK options

## See Also

- [API Comparison](api-comparison.md) - Choose the right SDK for your use case
- [Best Practices](best-practices.md) - Production deployment guidelines
- [Getting Started Guide](../getting-started.md) - Initial setup and configuration
- [High-Level SDK Documentation](../high-level-sdk/index.md) - Comprehensive SDK guide
- [Generated SDK Documentation](../generated-sdk/index.md) - Direct API access
- [lakefs-spec Documentation](../lakefs-spec/index.md) - Filesystem interface
- [Boto3 Integration](../boto3/index.md) - S3-compatible operations