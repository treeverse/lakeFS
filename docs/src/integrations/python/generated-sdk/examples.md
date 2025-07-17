---
title: Generated SDK Usage Examples
description: Common usage patterns and examples for the Generated Python SDK
sdk_types: ["generated"]
difficulty: "intermediate"
use_cases: ["examples", "patterns", "direct-api", "custom-operations"]
topics: ["examples", "patterns", "usage", "api-calls"]
audience: ["developers", "advanced-users", "integrators"]
last_updated: "2024-01-15"
---

# Generated SDK Usage Examples

Comprehensive examples and patterns for using the Generated Python SDK effectively in various scenarios.

## Basic Setup and Configuration

### Client Initialization
```python
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient

# Basic client setup
config = lakefs_sdk.Configuration(
    host="http://localhost:8000",
    username="AKIAIOSFODNN7EXAMPLE",
    password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

client = LakeFSClient(configuration=config)
```

### Production Configuration
```python
import os

# Production-ready configuration
config = lakefs_sdk.Configuration(
    host=os.getenv('LAKEFS_ENDPOINT'),
    username=os.getenv('LAKEFS_ACCESS_KEY_ID'),
    password=os.getenv('LAKEFS_SECRET_ACCESS_KEY'),
    verify_ssl=True,
    ssl_ca_cert=os.getenv('LAKEFS_CA_CERT'),
    timeout=30
)

client = LakeFSClient(configuration=config)
```

## Repository Operations

### Repository Management
```python
repositories_api = lakefs_sdk.RepositoriesApi(client)

# Create repository
def create_repository(name, storage_namespace):
    try:
        repo = repositories_api.create_repository(
            repository_creation=lakefs_sdk.RepositoryCreation(
                name=name,
                storage_namespace=storage_namespace,
                default_branch="main"
            )
        )
        print(f"Created repository: {repo.id}")
        return repo
    except lakefs_sdk.ConflictException:
        print(f"Repository {name} already exists")
        return repositories_api.get_repository(repository=name)

# List all repositories
def list_repositories():
    repos = repositories_api.list_repositories()
    for repo in repos.results:
        print(f"Repository: {repo.id}")
        print(f"  Storage: {repo.storage_namespace}")
        print(f"  Default branch: {repo.default_branch}")
        print(f"  Created: {repo.creation_date}")
```

## Branch Operations

### Branch Management
```python
branches_api = lakefs_sdk.BranchesApi(client)

# Create branch from main
def create_feature_branch(repository, branch_name, source_ref="main"):
    try:
        branch = branches_api.create_branch(
            repository=repository,
            branch_creation=lakefs_sdk.BranchCreation(
                name=branch_name,
                source=source_ref
            )
        )
        print(f"Created branch: {branch.id}")
        return branch
    except lakefs_sdk.ConflictException:
        print(f"Branch {branch_name} already exists")
        return branches_api.get_branch(repository=repository, branch=branch_name)

# List branches with details
def list_branches_detailed(repository):
    branches = branches_api.list_branches(repository=repository)
    for branch in branches.results:
        print(f"Branch: {branch.id}")
        print(f"  Commit: {branch.commit_id}")
```

## Object Operations

### Object Upload and Download
```python
objects_api = lakefs_sdk.ObjectsApi(client)

# Upload text content
def upload_text_object(repository, branch, path, content):
    try:
        objects_api.upload_object(
            repository=repository,
            branch=branch,
            path=path,
            content=content.encode('utf-8')
        )
        print(f"Uploaded: {path}")
    except Exception as e:
        print(f"Upload failed: {e}")

# Upload binary file
def upload_file(repository, branch, remote_path, local_path):
    with open(local_path, 'rb') as f:
        content = f.read()
        objects_api.upload_object(
            repository=repository,
            branch=branch,
            path=remote_path,
            content=content
        )

# Download object
def download_object(repository, ref, path):
    try:
        response = objects_api.get_object(
            repository=repository,
            ref=ref,
            path=path
        )
        return response.read()
    except lakefs_sdk.NotFoundException:
        print(f"Object not found: {path}")
        return None

# Get object metadata
def get_object_info(repository, ref, path):
    try:
        stats = objects_api.stat_object(
            repository=repository,
            ref=ref,
            path=path
        )
        print(f"Object: {stats.path}")
        print(f"  Size: {stats.size_bytes} bytes")
        print(f"  Content-Type: {stats.content_type}")
        print(f"  Checksum: {stats.checksum}")
        print(f"  Modified: {stats.mtime}")
        return stats
    except lakefs_sdk.NotFoundException:
        print(f"Object not found: {path}")
        return None
```

### Batch Object Operations
```python
def batch_upload(repository, branch, files_data):
    """Upload multiple files efficiently"""
    uploaded = []
    failed = []
    
    for path, content in files_data.items():
        try:
            objects_api.upload_object(
                repository=repository,
                branch=branch,
                path=path,
                content=content if isinstance(content, bytes) else content.encode('utf-8')
            )
            uploaded.append(path)
        except Exception as e:
            failed.append((path, str(e)))
    
    print(f"Uploaded: {len(uploaded)} files")
    if failed:
        print(f"Failed: {len(failed)} files")
        for path, error in failed:
            print(f"  {path}: {error}")
    
    return uploaded, failed

# Usage
files = {
    "data/file1.txt": "Content 1",
    "data/file2.txt": "Content 2",
    "data/file3.json": '{"key": "value"}'
}

uploaded, failed = batch_upload("my-repo", "main", files)
```

## Commit Operations

### Creating Commits
```python
commits_api = lakefs_sdk.CommitsApi(client)

# Simple commit
def create_commit(repository, branch, message):
    try:
        commit = commits_api.commit(
            repository=repository,
            branch=branch,
            commit_creation=lakefs_sdk.CommitCreation(
                message=message
            )
        )
        print(f"Created commit: {commit.id}")
        return commit
    except Exception as e:
        print(f"Commit failed: {e}")
        return None

# Commit with metadata
def create_commit_with_metadata(repository, branch, message, metadata):
    commit = commits_api.commit(
        repository=repository,
        branch=branch,
        commit_creation=lakefs_sdk.CommitCreation(
            message=message,
            metadata=metadata
        )
    )
    return commit

# Get commit history
def get_commit_history(repository, ref, limit=10):
    commits = commits_api.log_commits(
        repository=repository,
        ref=ref,
        amount=limit
    )
    
    for commit in commits.results:
        print(f"Commit: {commit.id}")
        print(f"  Message: {commit.message}")
        print(f"  Author: {commit.committer}")
        print(f"  Date: {commit.creation_date}")
        if commit.metadata:
            print(f"  Metadata: {commit.metadata}")
        print()
```

## Diff and Merge Operations

### Comparing References
```python
refs_api = lakefs_sdk.RefsApi(client)

# Get diff between branches
def diff_branches(repository, left_ref, right_ref):
    try:
        diff = refs_api.diff_refs(
            repository=repository,
            left_ref=left_ref,
            right_ref=right_ref
        )
        
        print(f"Diff between {left_ref} and {right_ref}:")
        for change in diff.results:
            print(f"  {change.type}: {change.path}")
            if change.size_bytes:
                print(f"    Size: {change.size_bytes} bytes")
        
        return diff
    except Exception as e:
        print(f"Diff failed: {e}")
        return None

# Merge branches
def merge_branches(repository, source_ref, destination_branch, message):
    try:
        result = refs_api.merge_into_branch(
            repository=repository,
            source_ref=source_ref,
            destination_branch=destination_branch,
            merge=lakefs_sdk.Merge(
                message=message,
                strategy="dest-wins"  # or "source-wins"
            )
        )
        print(f"Merge successful: {result.reference}")
        return result
    except lakefs_sdk.ConflictException as e:
        print(f"Merge conflict: {e}")
        return None
    except Exception as e:
        print(f"Merge failed: {e}")
        return None
```

## Tag Management

### Working with Tags
```python
tags_api = lakefs_sdk.TagsApi(api_client)

# Create semantic version tags
def create_version_tag(repository, version, commit_ref):
    """Create a semantic version tag"""
    try:
        tag = tags_api.create_tag(
            repository=repository,
            tag_creation=lakefs_sdk.models.TagCreation(
                id=f"v{version}",
                ref=commit_ref
            )
        )
        print(f"Created tag: {tag.id} -> {tag.commit_id}")
        return tag
    except lakefs_sdk.rest.ApiException as e:
        if e.status == 409:
            print(f"Tag v{version} already exists")
        else:
            print(f"Failed to create tag: {e}")
        return None

# List and filter tags
def list_version_tags(repository):
    """List all version tags"""
    try:
        tags = tags_api.list_tags(
            repository=repository,
            prefix="v",  # Only version tags
            amount=100
        )
        
        version_tags = []
        for tag in tags.results:
            if tag.id.startswith("v"):
                version_tags.append({
                    'version': tag.id,
                    'commit': tag.commit_id,
                    'tag_object': tag
                })
        
        # Sort by version (simple string sort)
        version_tags.sort(key=lambda x: x['version'])
        return version_tags
        
    except Exception as e:
        print(f"Failed to list tags: {e}")
        return []

# Usage example
version_tags = list_version_tags("my-repo")
for tag_info in version_tags:
    print(f"Version: {tag_info['version']} (commit: {tag_info['commit'][:8]})")
```

## Advanced Object Operations

### Presigned URLs and Metadata
```python
# Generate presigned URLs for external access
def generate_presigned_urls(repository, ref, paths, expires_in=3600):
    """Generate presigned URLs for multiple objects"""
    presigned_urls = {}
    
    for path in paths:
        try:
            # Get presigned URL for download
            response = objects_api.get_object(
                repository=repository,
                ref=ref,
                path=path,
                presign=True,
                presign_expires=expires_in
            )
            presigned_urls[path] = response.url
            
        except Exception as e:
            print(f"Failed to generate presigned URL for {path}: {e}")
            presigned_urls[path] = None
    
    return presigned_urls

# Object metadata operations
def update_object_metadata(repository, branch, path, metadata):
    """Update object user metadata"""
    try:
        # Note: This requires the experimental API
        experimental_api = lakefs_sdk.ExperimentalApi(api_client)
        
        result = experimental_api.update_object_user_metadata(
            repository=repository,
            branch=branch,
            path=path,
            object_user_metadata=lakefs_sdk.models.ObjectUserMetadata(
                metadata=metadata
            )
        )
        print(f"Updated metadata for {path}")
        return result
        
    except Exception as e:
        print(f"Failed to update metadata: {e}")
        return None

# Copy objects with metadata preservation
def copy_object_with_metadata(repository, source_branch, dest_branch, source_path, dest_path):
    """Copy object preserving metadata"""
    try:
        # First get source object metadata
        source_stats = objects_api.stat_object(
            repository=repository,
            ref=source_branch,
            path=source_path,
            user_metadata=True
        )
        
        # Copy the object
        copy_result = objects_api.copy_object(
            repository=repository,
            branch=dest_branch,
            dest_path=dest_path,
            object_copy_creation=lakefs_sdk.models.ObjectCopyCreation(
                src_path=source_path,
                src_ref=source_branch
            )
        )
        
        print(f"Copied {source_path} to {dest_path}")
        print(f"Preserved metadata: {source_stats.metadata}")
        return copy_result
        
    except Exception as e:
        print(f"Copy failed: {e}")
        return None
```

## Advanced Pagination Patterns

### Efficient Pagination with Filtering
```python
def list_all_objects_paginated(repository, ref, prefix=""):
    """List all objects handling pagination efficiently"""
    all_objects = []
    after = ""
    
    while True:
        try:
            response = objects_api.list_objects(
                repository=repository,
                ref=ref,
                prefix=prefix,
                after=after,
                amount=1000  # Page size
            )
            
            all_objects.extend(response.results)
            
            if not response.pagination.has_more:
                break
                
            after = response.pagination.next_offset
            
        except Exception as e:
            print(f"Error listing objects: {e}")
            break
    
    return all_objects

def find_objects_by_pattern(repository, ref, pattern, prefix=""):
    """Find objects matching a pattern across all pages"""
    import re
    matching_objects = []
    after = ""
    
    compiled_pattern = re.compile(pattern)
    
    while True:
        try:
            response = objects_api.list_objects(
                repository=repository,
                ref=ref,
                prefix=prefix,
                after=after,
                amount=1000
            )
            
            # Filter objects matching pattern
            for obj in response.results:
                if compiled_pattern.search(obj.path):
                    matching_objects.append(obj)
            
            if not response.pagination.has_more:
                break
                
            after = response.pagination.next_offset
            
        except Exception as e:
            print(f"Error searching objects: {e}")
            break
    
    return matching_objects

# Usage examples
all_csv_files = find_objects_by_pattern("my-repo", "main", r"\.csv$", "data/")
print(f"Found {len(all_csv_files)} CSV files")

large_files = []
for obj in list_all_objects_paginated("my-repo", "main"):
    if obj.size_bytes > 1024 * 1024:  # Files larger than 1MB
        large_files.append(obj)
```

## Concurrent Operations

### Parallel Processing with Threading
```python
import concurrent.futures
import threading
from typing import List, Tuple

def parallel_object_operations(repository: str, branch: str, operations: List[Tuple[str, str, bytes]]):
    """Perform multiple object operations in parallel"""
    
    def upload_single_object(operation):
        op_type, path, content = operation
        try:
            if op_type == "upload":
                result = objects_api.upload_object(
                    repository=repository,
                    branch=branch,
                    path=path,
                    content=content
                )
                return ("success", path, result)
            elif op_type == "delete":
                objects_api.delete_object(
                    repository=repository,
                    branch=branch,
                    path=path
                )
                return ("success", path, None)
        except Exception as e:
            return ("error", path, str(e))
    
    # Execute operations in parallel
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_operation = {
            executor.submit(upload_single_object, op): op 
            for op in operations
        }
        
        for future in concurrent.futures.as_completed(future_to_operation):
            result = future.result()
            results.append(result)
    
    # Summarize results
    successful = [r for r in results if r[0] == "success"]
    failed = [r for r in results if r[0] == "error"]
    
    print(f"Completed {len(successful)} operations successfully")
    if failed:
        print(f"Failed {len(failed)} operations:")
        for status, path, error in failed:
            print(f"  {path}: {error}")
    
    return results

# Usage
operations = [
    ("upload", "data/file1.txt", b"Content 1"),
    ("upload", "data/file2.txt", b"Content 2"),
    ("upload", "data/file3.txt", b"Content 3"),
    ("delete", "old/deprecated.txt", None)
]

results = parallel_object_operations("my-repo", "main", operations)
```

## Advanced Error Handling

### Comprehensive Error Handling
```python
import time
import random
from lakefs_sdk.rest import ApiException

class LakeFSRetryHandler:
    """Advanced retry handler for lakeFS operations"""
    
    def __init__(self, max_retries=3, base_delay=1, max_delay=60):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def execute_with_retry(self, operation, *args, **kwargs):
        """Execute operation with exponential backoff retry"""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
                
            except ApiException as e:
                last_exception = e
                
                # Don't retry client errors (4xx) except rate limiting
                if 400 <= e.status < 500 and e.status != 429:
                    raise
                
                # Don't retry on last attempt
                if attempt == self.max_retries:
                    raise
                
                # Calculate delay with jitter
                delay = min(
                    self.base_delay * (2 ** attempt) + random.uniform(0, 1),
                    self.max_delay
                )
                
                print(f"Attempt {attempt + 1} failed (status: {e.status}), "
                      f"retrying in {delay:.2f}s...")
                time.sleep(delay)
                
            except Exception as e:
                last_exception = e
                
                # Don't retry non-API exceptions on last attempt
                if attempt == self.max_retries:
                    raise
                
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                print(f"Attempt {attempt + 1} failed, retrying in {delay:.2f}s...")
                time.sleep(delay)
        
        # This should never be reached, but just in case
        raise last_exception

# Usage
retry_handler = LakeFSRetryHandler(max_retries=3)

try:
    repo = retry_handler.execute_with_retry(
        repositories_api.get_repository,
        repository="my-repo"
    )
    print(f"Successfully retrieved repository: {repo.id}")
    
except ApiException as e:
    print(f"Failed after retries: HTTP {e.status} - {e.reason}")
    
except Exception as e:
    print(f"Unexpected error: {e}")

# Context manager version
class RetryContext:
    """Context manager for retry operations"""
    
    def __init__(self, max_retries=3):
        self.retry_handler = LakeFSRetryHandler(max_retries=max_retries)
    
    def __enter__(self):
        return self.retry_handler
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

# Usage with context manager
with RetryContext(max_retries=5) as retry:
    branches = retry.execute_with_retry(
        branches_api.list_branches,
        repository="my-repo"
    )
```

## Working with Large Files and Streaming

### Efficient Large File Handling
```python
import os
from pathlib import Path

def upload_large_file_chunked(repository, branch, remote_path, local_path, chunk_size=8*1024*1024):
    """Upload large files efficiently"""
    file_size = os.path.getsize(local_path)
    print(f"Uploading {local_path} ({file_size:,} bytes) to {remote_path}")
    
    try:
        with open(local_path, 'rb') as f:
            content = f.read()
        
        # For very large files, consider using multipart upload
        # (requires experimental API)
        result = objects_api.upload_object(
            repository=repository,
            branch=branch,
            path=remote_path,
            content=content
        )
        
        print(f"Upload completed: {result.path}")
        return result
        
    except Exception as e:
        print(f"Upload failed: {e}")
        return None

def download_with_progress(repository, ref, remote_path, local_path):
    """Download file with progress indication"""
    try:
        # Get object stats first to show progress
        stats = objects_api.stat_object(
            repository=repository,
            ref=ref,
            path=remote_path
        )
        
        print(f"Downloading {remote_path} ({stats.size_bytes:,} bytes)...")
        
        # Download the object
        response = objects_api.get_object(
            repository=repository,
            ref=ref,
            path=remote_path
        )
        
        # Write to local file
        with open(local_path, 'wb') as f:
            content = response.read()
            f.write(content)
        
        print(f"Download completed: {local_path}")
        return True
        
    except Exception as e:
        print(f"Download failed: {e}")
        return False

def batch_download_directory(repository, ref, remote_prefix, local_dir):
    """Download all objects with a prefix to local directory"""
    local_path = Path(local_dir)
    local_path.mkdir(parents=True, exist_ok=True)
    
    # List all objects with the prefix
    objects = list_all_objects_paginated(repository, ref, remote_prefix)
    
    downloaded = []
    failed = []
    
    for obj in objects:
        # Calculate local file path
        relative_path = obj.path[len(remote_prefix):].lstrip('/')
        local_file_path = local_path / relative_path
        
        # Create parent directories
        local_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download the file
        if download_with_progress(repository, ref, obj.path, str(local_file_path)):
            downloaded.append(obj.path)
        else:
            failed.append(obj.path)
    
    print(f"Downloaded {len(downloaded)} files, {len(failed)} failed")
    return downloaded, failed

# Usage examples
upload_large_file_chunked("my-repo", "main", "data/large-dataset.csv", "/path/to/large-file.csv")

downloaded, failed = batch_download_directory(
    "my-repo", "main", "data/exports/", "/local/download/dir"
)
```

### Error Handling Patterns
```python
def robust_api_call(api_func, *args, **kwargs):
    """Wrapper for robust API calls with retry logic"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except lakefs_sdk.ApiException as e:
            if e.status == 429:  # Rate limited
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            elif e.status >= 500:  # Server error
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
            raise
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            raise
    
    raise Exception(f"Failed after {max_retries} attempts")

# Usage
try:
    repo = robust_api_call(
        repositories_api.get_repository,
        repository="my-repo"
    )
except Exception as e:
    print(f"Failed to get repository: {e}")
```

### Working with Large Files
```python
def upload_large_file(repository, branch, remote_path, local_path, chunk_size=8192):
    """Upload large files in chunks"""
    try:
        with open(local_path, 'rb') as f:
            content = f.read()
            
        objects_api.upload_object(
            repository=repository,
            branch=branch,
            path=remote_path,
            content=content
        )
        
        print(f"Uploaded large file: {remote_path}")
        
    except Exception as e:
        print(f"Large file upload failed: {e}")

def download_large_file(repository, ref, remote_path, local_path):
    """Download large files efficiently"""
    try:
        response = objects_api.get_object(
            repository=repository,
            ref=ref,
            path=remote_path
        )
        
        with open(local_path, 'wb') as f:
            f.write(response.read())
            
        print(f"Downloaded large file: {local_path}")
        
    except Exception as e:
        print(f"Large file download failed: {e}")
```

## Next Steps

- Learn about [direct access](direct-access.md) from High-Level SDK
- Review the [complete API reference](api-reference.md)
- Check the [official Generated SDK documentation](https://pydocs-sdk.lakefs.io)