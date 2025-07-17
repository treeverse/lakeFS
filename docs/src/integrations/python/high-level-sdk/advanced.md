---
title: Advanced Features
description: Advanced patterns and optimization techniques for the High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "advanced"
use_cases: ["performance", "optimization", "advanced-patterns", "production"]
topics: ["performance", "optimization", "patterns", "advanced"]
audience: ["advanced-users", "developers", "data-engineers"]
last_updated: "2024-01-15"
---

# Advanced Features

Explore advanced patterns, optimization techniques, and best practices for the High-Level Python SDK. This guide covers error handling, performance optimization, logging, debugging, and production deployment strategies.

## Error Handling and Exception Management

### Complete Exception Hierarchy

The High-Level SDK provides a comprehensive exception hierarchy for different error scenarios:

```python
import lakefs
from lakefs.exceptions import (
    # Base exceptions
    LakeFSException,
    ServerException,
    
    # Authentication and authorization
    NoAuthenticationFound,
    NotAuthorizedException,
    ForbiddenException,
    PermissionException,
    
    # Resource errors
    NotFoundException,
    ObjectNotFoundException,
    ConflictException,
    ObjectExistsException,
    
    # Request errors
    BadRequestException,
    UnsupportedOperationException,
    InvalidRangeException,
    
    # SDK-specific errors
    ImportManagerException,
    TransactionException,
    
    # Configuration errors
    UnsupportedCredentialsProviderType,
    InvalidEnvVarFormat
)
```

### Comprehensive Error Handling Patterns

```python
def robust_repository_operations(repo_id, storage_namespace):
    """Demonstrate comprehensive error handling for repository operations"""
    
    try:
        # Attempt to create repository
        repo = lakefs.repository(repo_id).create(
            storage_namespace=storage_namespace,
            exist_ok=False
        )
        print(f"Repository created: {repo_id}")
        return repo
        
    except ConflictException:
        print(f"Repository {repo_id} already exists, connecting to existing")
        return lakefs.repository(repo_id)
        
    except NotAuthorizedException:
        print("Authentication failed - check credentials")
        raise
        
    except ForbiddenException:
        print("Operation forbidden - insufficient permissions")
        raise
        
    except BadRequestException as e:
        print(f"Invalid request parameters: {e}")
        raise
        
    except ServerException as e:
        print(f"Server error (HTTP {e.status_code}): {e.reason}")
        if e.body:
            print(f"Error details: {e.body}")
        raise
        
    except LakeFSException as e:
        print(f"lakeFS SDK error: {e}")
        raise
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

# Usage
try:
    repo = robust_repository_operations("my-repo", "s3://my-bucket/repos/my-repo")
except Exception as e:
    print(f"Failed to set up repository: {e}")
```

### Object-Level Error Handling

```python
def safe_object_operations(branch, operations):
    """Safely perform multiple object operations with detailed error handling"""
    
    results = []
    
    for operation in operations:
        op_type = operation["type"]
        path = operation["path"]
        
        try:
            if op_type == "upload":
                obj = branch.object(path).upload(
                    data=operation["data"],
                    mode=operation.get("mode", "w")
                )
                results.append({"path": path, "status": "uploaded", "object": obj})
                
            elif op_type == "download":
                obj = branch.object(path)
                if not obj.exists():
                    raise ObjectNotFoundException(404, "Object not found", None)
                
                content = obj.reader().read()
                results.append({"path": path, "status": "downloaded", "content": content})
                
            elif op_type == "delete":
                obj = branch.object(path)
                obj.delete()
                results.append({"path": path, "status": "deleted"})
                
        except ObjectNotFoundException:
            print(f"Object not found: {path}")
            results.append({"path": path, "status": "not_found"})
            
        except ObjectExistsException:
            print(f"Object already exists (exclusive mode): {path}")
            results.append({"path": path, "status": "exists"})
            
        except PermissionException:
            print(f"Permission denied for object: {path}")
            results.append({"path": path, "status": "permission_denied"})
            
        except InvalidRangeException:
            print(f"Invalid range request for object: {path}")
            results.append({"path": path, "status": "invalid_range"})
            
        except Exception as e:
            print(f"Unexpected error with object {path}: {e}")
            results.append({"path": path, "status": "error", "error": str(e)})
    
    return results

# Usage
operations = [
    {"type": "upload", "path": "data/file1.txt", "data": "content1"},
    {"type": "upload", "path": "data/file2.txt", "data": "content2", "mode": "x"},
    {"type": "download", "path": "data/file1.txt"},
    {"type": "delete", "path": "data/old-file.txt"}
]

results = safe_object_operations(branch, operations)
for result in results:
    print(f"{result['path']}: {result['status']}")
```

### Custom Exception Handling

```python
class DataPipelineException(LakeFSException):
    """Custom exception for data pipeline operations"""
    
    def __init__(self, stage, message, original_exception=None):
        self.stage = stage
        self.original_exception = original_exception
        super().__init__(f"Pipeline failed at stage '{stage}': {message}")

def pipeline_stage_wrapper(stage_name):
    """Decorator for pipeline stages with custom error handling"""
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except LakeFSException as e:
                raise DataPipelineException(stage_name, str(e), e)
            except Exception as e:
                raise DataPipelineException(stage_name, f"Unexpected error: {e}", e)
        return wrapper
    return decorator

@pipeline_stage_wrapper("data_extraction")
def extract_data(branch, source_path):
    """Extract data with error handling"""
    obj = branch.object(source_path)
    return json.loads(obj.reader().read())

@pipeline_stage_wrapper("data_transformation")
def transform_data(data):
    """Transform data with error handling"""
    return [{"id": item["id"], "value": item["value"] * 2} for item in data]

@pipeline_stage_wrapper("data_loading")
def load_data(branch, target_path, data):
    """Load data with error handling"""
    branch.object(target_path).upload(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )

# Usage
try:
    data = extract_data(branch, "raw/input.json")
    transformed = transform_data(data)
    load_data(branch, "processed/output.json", transformed)
    print("Pipeline completed successfully")
    
except DataPipelineException as e:
    print(f"Pipeline failed: {e}")
    print(f"Failed stage: {e.stage}")
    if e.original_exception:
        print(f"Original error: {e.original_exception}")
```

## Performance Optimization

### Advanced Client Configuration

```python
from lakefs.client import Client
import os

def create_optimized_client():
    """Create a performance-optimized client"""
    
    return Client(
        host=os.getenv('LAKEFS_ENDPOINT'),
        username=os.getenv('LAKEFS_ACCESS_KEY_ID'),
        password=os.getenv('LAKEFS_SECRET_ACCESS_KEY'),
        
        # Connection pooling for high throughput
        pool_connections=50,
        pool_maxsize=100,
        pool_block=False,
        
        # Retry configuration
        max_retries=5,
        backoff_factor=0.3,
        retry_on_status=[500, 502, 503, 504],
        
        # Timeout settings
        timeout=60,
        connect_timeout=10,
        read_timeout=50,
        
        # SSL and security
        verify_ssl=True,
        ssl_ca_cert=os.getenv('LAKEFS_CA_CERT_PATH'),
        
        # Proxy configuration
        proxy=os.getenv('HTTPS_PROXY'),
        proxy_headers={'User-Agent': 'MyApp/1.0'}
    )

# Create singleton client for reuse
_optimized_client = None

def get_optimized_client():
    """Get or create optimized client singleton"""
    global _optimized_client
    if _optimized_client is None:
        _optimized_client = create_optimized_client()
    return _optimized_client
```

### Batch Operations and Bulk Processing

```python
import concurrent.futures
import threading
from collections import defaultdict

class BulkOperationManager:
    """Manager for efficient bulk operations"""
    
    def __init__(self, branch, max_workers=10, batch_size=100):
        self.branch = branch
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.results = defaultdict(list)
        self.lock = threading.Lock()
    
    def bulk_upload(self, file_data_pairs):
        """Upload multiple files efficiently using threading"""
        
        def upload_batch(batch):
            """Upload a batch of files in a transaction"""
            batch_results = []
            
            try:
                with self.branch.transact(
                    commit_message=f"Bulk upload batch ({len(batch)} files)"
                ) as tx:
                    for path, data in batch:
                        obj = tx.object(path).upload(data=data)
                        batch_results.append({"path": path, "status": "success", "object": obj})
                
            except Exception as e:
                for path, _ in batch:
                    batch_results.append({"path": path, "status": "error", "error": str(e)})
            
            with self.lock:
                self.results["uploads"].extend(batch_results)
            
            return batch_results
        
        # Split into batches
        batches = [
            file_data_pairs[i:i + self.batch_size]
            for i in range(0, len(file_data_pairs), self.batch_size)
        ]
        
        # Process batches concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(upload_batch, batch) for batch in batches]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Batch upload failed: {e}")
        
        return self.results["uploads"]
    
    def bulk_download(self, paths):
        """Download multiple files efficiently"""
        
        def download_file(path):
            """Download a single file"""
            try:
                obj = self.branch.object(path)
                if obj.exists():
                    content = obj.reader().read()
                    return {"path": path, "status": "success", "content": content}
                else:
                    return {"path": path, "status": "not_found"}
            except Exception as e:
                return {"path": path, "status": "error", "error": str(e)}
        
        # Download files concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(download_file, path): path for path in paths}
            
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
        
        return results

# Usage
bulk_manager = BulkOperationManager(branch, max_workers=20, batch_size=50)

# Bulk upload
files_to_upload = [
    (f"data/file_{i}.txt", f"Content for file {i}")
    for i in range(1000)
]

upload_results = bulk_manager.bulk_upload(files_to_upload)
success_count = len([r for r in upload_results if r["status"] == "success"])
print(f"Successfully uploaded {success_count} files")

# Bulk download
paths_to_download = [f"data/file_{i}.txt" for i in range(100)]
download_results = bulk_manager.bulk_download(paths_to_download)
```

### Memory-Efficient Streaming

```python
import hashlib
import time

class StreamingProcessor:
    """Efficient streaming processor for large files"""
    
    def __init__(self, chunk_size=64*1024):  # 64KB chunks
        self.chunk_size = chunk_size
    
    def stream_upload_with_progress(self, branch, local_path, remote_path, 
                                  progress_callback=None):
        """Upload large file with progress tracking"""
        
        file_size = os.path.getsize(local_path)
        uploaded_bytes = 0
        start_time = time.time()
        
        obj = branch.object(remote_path)
        
        with open(local_path, 'rb') as local_file:
            with obj.writer(mode='wb') as remote_writer:
                while True:
                    chunk = local_file.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    remote_writer.write(chunk)
                    uploaded_bytes += len(chunk)
                    
                    if progress_callback:
                        progress = (uploaded_bytes / file_size) * 100
                        elapsed = time.time() - start_time
                        speed = uploaded_bytes / elapsed if elapsed > 0 else 0
                        
                        progress_callback({
                            "progress": progress,
                            "uploaded_bytes": uploaded_bytes,
                            "total_bytes": file_size,
                            "speed_bps": speed,
                            "elapsed_time": elapsed
                        })
        
        return obj
    
    def stream_download_with_verification(self, branch, remote_path, local_path,
                                        verify_checksum=True):
        """Download large file with checksum verification"""
        
        obj = branch.object(remote_path)
        
        # Get object stats for verification
        stats = obj.stat()
        expected_size = stats.size_bytes
        expected_checksum = stats.checksum if verify_checksum else None
        
        downloaded_bytes = 0
        hasher = hashlib.sha256() if verify_checksum else None
        
        with obj.reader(mode='rb') as remote_reader:
            with open(local_path, 'wb') as local_file:
                while True:
                    chunk = remote_reader.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    local_file.write(chunk)
                    downloaded_bytes += len(chunk)
                    
                    if hasher:
                        hasher.update(chunk)
        
        # Verify download
        if downloaded_bytes != expected_size:
            raise ValueError(f"Size mismatch: expected {expected_size}, got {downloaded_bytes}")
        
        if verify_checksum and expected_checksum:
            actual_checksum = f"sha256:{hasher.hexdigest()}"
            if actual_checksum != expected_checksum:
                raise ValueError(f"Checksum mismatch: expected {expected_checksum}, got {actual_checksum}")
        
        return {
            "local_path": local_path,
            "remote_path": remote_path,
            "size_bytes": downloaded_bytes,
            "checksum_verified": verify_checksum
        }

# Usage with progress tracking
def progress_callback(info):
    print(f"Upload progress: {info['progress']:.1f}% "
          f"({info['uploaded_bytes']}/{info['total_bytes']} bytes) "
          f"Speed: {info['speed_bps']/1024/1024:.1f} MB/s")

processor = StreamingProcessor()

# Upload large file
large_obj = processor.stream_upload_with_progress(
    branch, 
    "large_dataset.csv", 
    "data/large_dataset.csv",
    progress_callback=progress_callback
)

# Download with verification
result = processor.stream_download_with_verification(
    branch,
    "data/large_dataset.csv",
    "downloaded_dataset.csv",
    verify_checksum=True
)
print(f"Download verified: {result}")
```

### Connection Pooling and Resource Management

```python
import atexit
from contextlib import contextmanager

class ConnectionManager:
    """Manage lakeFS connections and resources efficiently"""
    
    def __init__(self):
        self.clients = {}
        self.active_connections = 0
        self.max_connections = 50
        
        # Register cleanup on exit
        atexit.register(self.cleanup_all)
    
    def get_client(self, config_name="default", **client_kwargs):
        """Get or create a client with connection pooling"""
        
        if config_name not in self.clients:
            if self.active_connections >= self.max_connections:
                raise RuntimeError(f"Maximum connections ({self.max_connections}) exceeded")
            
            client_config = {
                "pool_connections": 10,
                "pool_maxsize": 20,
                "max_retries": 3,
                **client_kwargs
            }
            
            self.clients[config_name] = Client(**client_config)
            self.active_connections += 1
        
        return self.clients[config_name]
    
    @contextmanager
    def managed_repository(self, repo_id, config_name="default", **client_kwargs):
        """Context manager for repository operations with automatic cleanup"""
        
        client = self.get_client(config_name, **client_kwargs)
        repo = lakefs.Repository(repo_id, client=client)
        
        try:
            yield repo
        finally:
            # Cleanup could be added here if needed
            pass
    
    def cleanup_all(self):
        """Cleanup all connections"""
        for client in self.clients.values():
            # Perform any necessary cleanup
            pass
        self.clients.clear()
        self.active_connections = 0

# Global connection manager
connection_manager = ConnectionManager()

# Usage
with connection_manager.managed_repository("my-repo") as repo:
    branch = repo.branch("main")
    
    # Perform operations
    branch.object("data/test.txt").upload(data="test content")
    
    # Repository and client are automatically managed
```

## Advanced I/O Patterns

### Custom Serialization and Formats

```python
import pickle
import json
import csv
import io
import gzip
import base64
from typing import Any, Dict, List

class AdvancedSerializer:
    """Advanced serialization for different data types and formats"""
    
    @staticmethod
    def serialize_python_object(obj: Any, compression=True) -> bytes:
        """Serialize Python object with optional compression"""
        data = pickle.dumps(obj)
        
        if compression:
            data = gzip.compress(data)
        
        return data
    
    @staticmethod
    def deserialize_python_object(data: bytes, compression=True) -> Any:
        """Deserialize Python object with optional decompression"""
        if compression:
            data = gzip.decompress(data)
        
        return pickle.loads(data)
    
    @staticmethod
    def serialize_dataframe(df, format='parquet', compression=True) -> bytes:
        """Serialize pandas DataFrame in various formats"""
        buffer = io.BytesIO()
        
        if format == 'parquet':
            df.to_parquet(buffer, compression='gzip' if compression else None)
        elif format == 'csv':
            csv_data = df.to_csv(index=False)
            if compression:
                csv_data = gzip.compress(csv_data.encode('utf-8'))
            else:
                csv_data = csv_data.encode('utf-8')
            buffer.write(csv_data)
        elif format == 'json':
            json_data = df.to_json(orient='records', indent=2)
            if compression:
                json_data = gzip.compress(json_data.encode('utf-8'))
            else:
                json_data = json_data.encode('utf-8')
            buffer.write(json_data)
        
        return buffer.getvalue()
    
    @staticmethod
    def deserialize_dataframe(data: bytes, format='parquet', compression=True):
        """Deserialize pandas DataFrame from various formats"""
        import pandas as pd
        
        if compression and format in ['csv', 'json']:
            data = gzip.decompress(data)
        
        buffer = io.BytesIO(data)
        
        if format == 'parquet':
            return pd.read_parquet(buffer)
        elif format == 'csv':
            return pd.read_csv(buffer)
        elif format == 'json':
            return pd.read_json(buffer, orient='records')

def advanced_data_storage(branch, data_items):
    """Store various data types with optimal serialization"""
    
    serializer = AdvancedSerializer()
    
    for item_name, item_data in data_items.items():
        if isinstance(item_data, dict):
            # Store as compressed JSON
            json_data = json.dumps(item_data, indent=2)
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            
            branch.object(f"data/{item_name}.json.gz").upload(
                data=compressed_data,
                content_type="application/gzip",
                metadata={"format": "json", "compression": "gzip"}
            )
            
        elif hasattr(item_data, 'to_parquet'):  # pandas DataFrame
            # Store as compressed Parquet
            parquet_data = serializer.serialize_dataframe(
                item_data, format='parquet', compression=True
            )
            
            branch.object(f"data/{item_name}.parquet").upload(
                data=parquet_data,
                content_type="application/octet-stream",
                metadata={"format": "parquet", "compression": "gzip"}
            )
            
        else:
            # Store as compressed pickle for arbitrary Python objects
            pickle_data = serializer.serialize_python_object(
                item_data, compression=True
            )
            
            branch.object(f"data/{item_name}.pkl.gz").upload(
                data=pickle_data,
                content_type="application/octet-stream",
                metadata={"format": "pickle", "compression": "gzip"}
            )

# Usage
import pandas as pd

data_items = {
    "config": {"version": "1.0", "settings": {"debug": True}},
    "users": pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}),
    "model": {"weights": [0.1, 0.2, 0.3], "bias": 0.05}
}

advanced_data_storage(branch, data_items)
```

### Pre-signed URL Management

```python
import time
from urllib.parse import urlparse
import requests

class PreSignedURLManager:
    """Manage pre-signed URLs for direct storage access"""
    
    def __init__(self, branch):
        self.branch = branch
        self.url_cache = {}
        self.cache_duration = 3600  # 1 hour
    
    def get_presigned_upload_url(self, path, content_type=None, cache=True):
        """Get pre-signed URL for direct upload"""
        
        cache_key = f"upload:{path}:{content_type}"
        
        if cache and cache_key in self.url_cache:
            cached_url, timestamp = self.url_cache[cache_key]
            if time.time() - timestamp < self.cache_duration:
                return cached_url
        
        # Get object stats to get pre-signed URL
        obj = self.branch.object(path)
        
        # For uploads, we need to use the writer with pre-sign
        with obj.writer(mode='wb', pre_sign=True, content_type=content_type) as writer:
            # The writer provides access to pre-signed URL
            presigned_url = writer._pre_signed_url if hasattr(writer, '_pre_signed_url') else None
        
        if cache and presigned_url:
            self.url_cache[cache_key] = (presigned_url, time.time())
        
        return presigned_url
    
    def get_presigned_download_url(self, path, cache=True):
        """Get pre-signed URL for direct download"""
        
        cache_key = f"download:{path}"
        
        if cache and cache_key in self.url_cache:
            cached_url, timestamp = self.url_cache[cache_key]
            if time.time() - timestamp < self.cache_duration:
                return cached_url
        
        # Get object stats with pre-sign enabled
        obj = self.branch.object(path)
        stats = obj.stat(pre_sign=True)
        presigned_url = stats.physical_address
        
        if cache and presigned_url:
            self.url_cache[cache_key] = (presigned_url, time.time())
        
        return presigned_url
    
    def direct_upload_via_presigned(self, path, data, content_type=None):
        """Upload data directly using pre-signed URL"""
        
        presigned_url = self.get_presigned_upload_url(path, content_type)
        
        if not presigned_url:
            # Fallback to regular upload
            return self.branch.object(path).upload(
                data=data, 
                content_type=content_type
            )
        
        # Direct upload to storage
        headers = {}
        if content_type:
            headers['Content-Type'] = content_type
        
        response = requests.put(presigned_url, data=data, headers=headers)
        response.raise_for_status()
        
        return self.branch.object(path)
    
    def direct_download_via_presigned(self, path):
        """Download data directly using pre-signed URL"""
        
        presigned_url = self.get_presigned_download_url(path)
        
        if not presigned_url:
            # Fallback to regular download
            return self.branch.object(path).reader().read()
        
        # Direct download from storage
        response = requests.get(presigned_url)
        response.raise_for_status()
        
        return response.content

# Usage
url_manager = PreSignedURLManager(branch)

# Direct upload
data = b"Large binary data that benefits from direct upload"
obj = url_manager.direct_upload_via_presigned(
    "data/large-file.bin", 
    data, 
    content_type="application/octet-stream"
)

# Direct download
downloaded_data = url_manager.direct_download_via_presigned("data/large-file.bin")
```

## Logging and Debugging

### Comprehensive Logging Setup

```python
import logging
import sys
from datetime import datetime

class LakeFSLogger:
    """Comprehensive logging setup for lakeFS operations"""
    
    def __init__(self, name="lakefs_app", level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """Set up logging handlers"""
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(
            f"lakefs_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setLevel(logging.DEBUG)
        
        # Formatters
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        console_handler.setFormatter(console_format)
        file_handler.setFormatter(file_format)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Enable lakeFS SDK logging
        lakefs_logger = logging.getLogger('lakefs')
        lakefs_logger.setLevel(logging.DEBUG)
        lakefs_logger.addHandler(file_handler)
    
    def log_operation(self, operation_name, func, *args, **kwargs):
        """Log operation execution with timing"""
        
        start_time = time.time()
        self.logger.info(f"Starting operation: {operation_name}")
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            self.logger.info(f"Completed operation: {operation_name} ({duration:.2f}s)")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Failed operation: {operation_name} ({duration:.2f}s) - {e}")
            raise
    
    def log_transaction(self, tx_func):
        """Decorator for logging transaction operations"""
        
        def wrapper(branch, commit_message, *args, **kwargs):
            self.logger.info(f"Starting transaction: {commit_message}")
            
            try:
                with branch.transact(commit_message=commit_message) as tx:
                    result = tx_func(tx, *args, **kwargs)
                    self.logger.info(f"Transaction completed: {commit_message}")
                    return result
                    
            except Exception as e:
                self.logger.error(f"Transaction failed: {commit_message} - {e}")
                raise
        
        return wrapper

# Usage
logger = LakeFSLogger("my_app", level=logging.DEBUG)

# Log regular operations
def upload_data(branch, path, data):
    return branch.object(path).upload(data=data)

result = logger.log_operation(
    "upload_user_data",
    upload_data,
    branch, "data/users.json", '{"users": []}'
)

# Log transactions
@logger.log_transaction
def process_data_transaction(tx, input_path, output_path):
    # Read input
    input_obj = tx.object(input_path)
    data = json.loads(input_obj.reader().read())
    
    # Process data
    processed = [{"id": item["id"], "processed": True} for item in data]
    
    # Write output
    tx.object(output_path).upload(
        data=json.dumps(processed, indent=2),
        content_type="application/json"
    )
    
    return len(processed)

# Execute logged transaction
processed_count = process_data_transaction(
    branch, 
    "Transaction: Process user data",
    "raw/users.json", 
    "processed/users.json"
)
```

### Performance Monitoring and Profiling

```python
import time
import psutil
import threading
from collections import defaultdict, deque

class PerformanceMonitor:
    """Monitor performance metrics for lakeFS operations"""
    
    def __init__(self, max_history=1000):
        self.metrics = defaultdict(deque)
        self.max_history = max_history
        self.active_operations = {}
        self.lock = threading.Lock()
    
    def start_operation(self, operation_id, operation_type):
        """Start monitoring an operation"""
        with self.lock:
            self.active_operations[operation_id] = {
                "type": operation_type,
                "start_time": time.time(),
                "start_memory": psutil.Process().memory_info().rss,
                "start_cpu": psutil.Process().cpu_percent()
            }
    
    def end_operation(self, operation_id, success=True, error=None):
        """End monitoring an operation"""
        with self.lock:
            if operation_id not in self.active_operations:
                return
            
            start_info = self.active_operations.pop(operation_id)
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss
            
            duration = end_time - start_info["start_time"]
            memory_delta = end_memory - start_info["start_memory"]
            
            metric = {
                "operation_id": operation_id,
                "type": start_info["type"],
                "duration": duration,
                "memory_delta": memory_delta,
                "success": success,
                "error": str(error) if error else None,
                "timestamp": end_time
            }
            
            # Store metric
            self.metrics[start_info["type"]].append(metric)
            
            # Limit history
            if len(self.metrics[start_info["type"]]) > self.max_history:
                self.metrics[start_info["type"]].popleft()
    
    def get_stats(self, operation_type=None):
        """Get performance statistics"""
        with self.lock:
            if operation_type:
                operations = [operation_type] if operation_type in self.metrics else []
            else:
                operations = list(self.metrics.keys())
            
            stats = {}
            
            for op_type in operations:
                metrics = list(self.metrics[op_type])
                if not metrics:
                    continue
                
                successful = [m for m in metrics if m["success"]]
                failed = [m for m in metrics if not m["success"]]
                
                durations = [m["duration"] for m in successful]
                memory_deltas = [m["memory_delta"] for m in successful]
                
                stats[op_type] = {
                    "total_operations": len(metrics),
                    "successful": len(successful),
                    "failed": len(failed),
                    "success_rate": len(successful) / len(metrics) * 100 if metrics else 0,
                    "avg_duration": sum(durations) / len(durations) if durations else 0,
                    "max_duration": max(durations) if durations else 0,
                    "min_duration": min(durations) if durations else 0,
                    "avg_memory_delta": sum(memory_deltas) / len(memory_deltas) if memory_deltas else 0,
                    "recent_errors": [m["error"] for m in failed[-5:] if m["error"]]
                }
            
            return stats
    
    def monitored_operation(self, operation_type):
        """Decorator for monitoring operations"""
        
        def decorator(func):
            def wrapper(*args, **kwargs):
                operation_id = f"{operation_type}_{int(time.time() * 1000)}"
                self.start_operation(operation_id, operation_type)
                
                try:
                    result = func(*args, **kwargs)
                    self.end_operation(operation_id, success=True)
                    return result
                except Exception as e:
                    self.end_operation(operation_id, success=False, error=e)
                    raise
            
            return wrapper
        return decorator

# Global performance monitor
perf_monitor = PerformanceMonitor()

# Usage with decorator
@perf_monitor.monitored_operation("object_upload")
def monitored_upload(branch, path, data):
    return branch.object(path).upload(data=data)

@perf_monitor.monitored_operation("object_download")
def monitored_download(branch, path):
    return branch.object(path).reader().read()

@perf_monitor.monitored_operation("transaction")
def monitored_transaction(branch, commit_message, operations):
    with branch.transact(commit_message=commit_message) as tx:
        results = []
        for op in operations:
            if op["type"] == "upload":
                result = tx.object(op["path"]).upload(data=op["data"])
                results.append(result)
        return results

# Perform monitored operations
for i in range(10):
    monitored_upload(branch, f"test/file_{i}.txt", f"Content {i}")
    content = monitored_download(branch, f"test/file_{i}.txt")

# Get performance statistics
stats = perf_monitor.get_stats()
for op_type, metrics in stats.items():
    print(f"\n{op_type.upper()} Statistics:")
    print(f"  Total operations: {metrics['total_operations']}")
    print(f"  Success rate: {metrics['success_rate']:.1f}%")
    print(f"  Average duration: {metrics['avg_duration']:.3f}s")
    print(f"  Max duration: {metrics['max_duration']:.3f}s")
    print(f"  Average memory delta: {metrics['avg_memory_delta']/1024/1024:.1f} MB")
```

## Production Deployment Strategies

### Configuration Management

```python
import os
import yaml
from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class LakeFSConfig:
    """Configuration class for lakeFS deployment"""
    
    # Connection settings
    endpoint: str
    access_key_id: str
    secret_access_key: str
    
    # SSL/TLS settings
    verify_ssl: bool = True
    ssl_ca_cert: Optional[str] = None
    
    # Connection pooling
    pool_connections: int = 20
    pool_maxsize: int = 50
    pool_block: bool = False
    
    # Retry settings
    max_retries: int = 3
    backoff_factor: float = 0.3
    retry_on_status: list = None
    
    # Timeout settings
    timeout: int = 60
    connect_timeout: int = 10
    read_timeout: int = 50
    
    # Proxy settings
    proxy: Optional[str] = None
    proxy_headers: Optional[Dict[str, str]] = None
    
    # Application settings
    default_branch: str = "main"
    batch_size: int = 100
    max_workers: int = 10
    
    def __post_init__(self):
        if self.retry_on_status is None:
            self.retry_on_status = [500, 502, 503, 504]

class ConfigManager:
    """Manage lakeFS configuration from multiple sources"""
    
    @staticmethod
    def from_environment() -> LakeFSConfig:
        """Load configuration from environment variables"""
        
        return LakeFSConfig(
            endpoint=os.getenv('LAKEFS_ENDPOINT', 'http://localhost:8000'),
            access_key_id=os.getenv('LAKEFS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('LAKEFS_SECRET_ACCESS_KEY'),
            
            verify_ssl=os.getenv('LAKEFS_VERIFY_SSL', 'true').lower() == 'true',
            ssl_ca_cert=os.getenv('LAKEFS_CA_CERT_PATH'),
            
            pool_connections=int(os.getenv('LAKEFS_POOL_CONNECTIONS', '20')),
            pool_maxsize=int(os.getenv('LAKEFS_POOL_MAXSIZE', '50')),
            
            max_retries=int(os.getenv('LAKEFS_MAX_RETRIES', '3')),
            timeout=int(os.getenv('LAKEFS_TIMEOUT', '60')),
            
            proxy=os.getenv('HTTPS_PROXY'),
            
            default_branch=os.getenv('LAKEFS_DEFAULT_BRANCH', 'main'),
            batch_size=int(os.getenv('LAKEFS_BATCH_SIZE', '100')),
            max_workers=int(os.getenv('LAKEFS_MAX_WORKERS', '10'))
        )
    
    @staticmethod
    def from_file(config_path: str) -> LakeFSConfig:
        """Load configuration from YAML file"""
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return LakeFSConfig(**config_data.get('lakefs', {}))
    
    @staticmethod
    def from_dict(config_dict: Dict[str, Any]) -> LakeFSConfig:
        """Load configuration from dictionary"""
        
        return LakeFSConfig(**config_dict)

class ProductionLakeFSClient:
    """Production-ready lakeFS client with comprehensive configuration"""
    
    def __init__(self, config: LakeFSConfig):
        self.config = config
        self._client = None
        self._health_check_interval = 300  # 5 minutes
        self._last_health_check = 0
    
    @property
    def client(self) -> Client:
        """Get or create lakeFS client"""
        
        if self._client is None:
            self._client = Client(
                host=self.config.endpoint,
                username=self.config.access_key_id,
                password=self.config.secret_access_key,
                
                verify_ssl=self.config.verify_ssl,
                ssl_ca_cert=self.config.ssl_ca_cert,
                
                pool_connections=self.config.pool_connections,
                pool_maxsize=self.config.pool_maxsize,
                pool_block=self.config.pool_block,
                
                max_retries=self.config.max_retries,
                backoff_factor=self.config.backoff_factor,
                retry_on_status=self.config.retry_on_status,
                
                timeout=self.config.timeout,
                connect_timeout=self.config.connect_timeout,
                read_timeout=self.config.read_timeout,
                
                proxy=self.config.proxy,
                proxy_headers=self.config.proxy_headers
            )
        
        return self._client
    
    def health_check(self, force=False) -> bool:
        """Perform health check with caching"""
        
        current_time = time.time()
        
        if not force and (current_time - self._last_health_check) < self._health_check_interval:
            return True  # Assume healthy if recently checked
        
        try:
            # Simple operation to test connectivity
            list(lakefs.repositories(client=self.client, max_amount=1))
            self._last_health_check = current_time
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def get_repository(self, repo_id: str):
        """Get repository with health check"""
        
        if not self.health_check():
            raise RuntimeError("lakeFS health check failed")
        
        return lakefs.Repository(repo_id, client=self.client)

# Usage
# Load configuration
config = ConfigManager.from_environment()

# Create production client
prod_client = ProductionLakeFSClient(config)

# Use with health checking
try:
    repo = prod_client.get_repository("my-repo")
    branch = repo.branch(config.default_branch)
    
    # Perform operations
    branch.object("data/production.txt").upload(data="Production data")
    
except Exception as e:
    logger.error(f"Production operation failed: {e}")
```

## Key Points

- **Comprehensive error handling**: Use specific exception types for robust error recovery
- **Performance optimization**: Implement connection pooling, batch operations, and streaming
- **Resource management**: Use context managers and connection pooling for efficient resource usage
- **Monitoring and logging**: Implement comprehensive logging and performance monitoring
- **Production readiness**: Use proper configuration management and health checking
- **Advanced I/O**: Leverage pre-signed URLs and custom serialization for optimal performance
- **Debugging support**: Enable detailed logging and performance profiling for troubleshooting

## See Also

- **[Repository Management](repositories.md)** - Creating and managing repositories
- **[Branch Operations](branches-and-commits.md)** - Version control operations
- **[Object Operations](objects-and-io.md)** - Individual object management
- **[Transactions](transactions.md)** - Atomic multi-operation workflows
- **[Import Operations](imports-and-exports.md)** - Bulk data operations
- **[Best Practices](../reference/best-practices.md)** - Production deployment guidance
- **[Troubleshooting](../reference/troubleshooting.md)** - Common issues and solutions