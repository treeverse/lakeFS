---
title: Direct Access from High-Level SDK
description: Accessing the Generated SDK from within the High-Level SDK
sdk_types: ["high-level", "generated"]
difficulty: "intermediate"
use_cases: ["hybrid-usage", "advanced-operations", "direct-api"]
topics: ["integration", "direct-access", "hybrid"]
audience: ["developers", "advanced-users", "python-developers"]
last_updated: "2024-01-15"
---

# Direct Access from High-Level SDK

Learn how to access the Generated SDK client from within the High-Level SDK for operations not covered by the high-level interface.

## Accessing the Generated Client

The High-Level SDK is built on top of the Generated SDK, and you can access the underlying client when needed.

### Basic Access Pattern
```python
import lakefs

# Create High-Level SDK objects
repo = lakefs.repository("my-repo")
branch = repo.branch("main")

# Access the underlying Generated SDK client
generated_client = repo.client.sdk

# Now you can use Generated SDK APIs directly
from lakefs_sdk import ObjectsApi
objects_api = ObjectsApi(generated_client)
```

## When to Use Direct Access

### Operations Not in High-Level SDK
```python
import lakefs
from lakefs_sdk import RefsApi, Merge

# High-Level SDK setup
repo = lakefs.repository("my-repo")
generated_client = repo.client.sdk

# Use Generated SDK for advanced merge options
refs_api = RefsApi(generated_client)

# Advanced merge with specific strategy
merge_result = refs_api.merge_into_branch(
    repository="my-repo",
    source_ref="feature-branch",
    destination_branch="main",
    merge=Merge(
        message="Advanced merge",
        strategy="source-wins",  # Specific merge strategy
        allow_empty=True,        # Allow empty commits
        force=False             # Force merge option
    )
)
```

### Advanced API Parameters
```python
import lakefs
from lakefs_sdk import ObjectsApi

repo = lakefs.repository("my-repo")
objects_api = ObjectsApi(repo.client.sdk)

# List objects with advanced pagination options
objects_response = objects_api.list_objects(
    repository="my-repo",
    ref="main",
    prefix="data/",
    after="data/file100.txt",  # Start after specific object
    amount=500,                # Custom page size
    delimiter="/",             # Directory-style listing
    user_metadata=True         # Include user metadata
)

# Access pagination info
pagination = objects_response.pagination
print(f"Has more: {pagination.has_more}")
print(f"Next offset: {pagination.next_offset}")
print(f"Max per page: {pagination.max_per_page}")
```

## Combining High-Level and Generated SDK

### Mixed Operations Workflow
```python
import lakefs
from lakefs_sdk import CommitsApi, TagsApi

# Use High-Level SDK for common operations
repo = lakefs.repository("my-repo")
branch = repo.branch("feature-branch")

# Upload files using High-Level SDK
branch.object("data/file1.txt").upload(data="Content 1")
branch.object("data/file2.txt").upload(data="Content 2")

# Commit using High-Level SDK
commit_ref = branch.commit(message="Add data files")

# Use Generated SDK for advanced tagging
tags_api = TagsApi(repo.client.sdk)
tag = tags_api.create_tag(
    repository="my-repo",
    tag_creation=lakefs_sdk.TagCreation(
        id="v1.0.0-beta",
        ref=commit_ref.get_commit().id,
        force=True  # Force tag creation
    )
)

print(f"Created tag: {tag.id} at commit {tag.commit_id}")
```

### Advanced Object Operations
```python
import lakefs
from lakefs_sdk import ObjectsApi

repo = lakefs.repository("my-repo")
branch = repo.branch("main")
objects_api = ObjectsApi(repo.client.sdk)

# Use High-Level SDK for simple upload
obj = branch.object("data/simple.txt").upload(data="Simple content")

# Use Generated SDK for advanced object operations
# Get object with presigned URL
presigned_response = objects_api.get_object(
    repository="my-repo",
    ref="main",
    path="data/simple.txt",
    presign=True,           # Generate presigned URL
    presign_expires=3600    # URL expires in 1 hour
)

print(f"Presigned URL: {presigned_response.url}")

# Get object statistics with additional metadata
stats = objects_api.stat_object(
    repository="my-repo",
    ref="main",
    path="data/simple.txt",
    user_metadata=True,     # Include user metadata
    presign=True           # Include presigned URL in stats
)

print(f"Physical address: {stats.physical_address}")
print(f"Presigned URL: {stats.physical_address_expiry}")
```

## Error Handling with Mixed Approach

### Consistent Error Handling
```python
import lakefs
from lakefs_sdk import ObjectsApi
from lakefs_sdk.exceptions import ApiException
from lakefs.exceptions import LakeFSException

def mixed_operation_with_error_handling(repo_name, branch_name, file_path):
    try:
        # High-Level SDK operations
        repo = lakefs.repository(repo_name)
        branch = repo.branch(branch_name)
        
        # Generated SDK operations
        objects_api = ObjectsApi(repo.client.sdk)
        
        # Try High-Level SDK first
        try:
            obj = branch.object(file_path)
            content = obj.reader().read()
            return content
        except LakeFSException as e:
            print(f"High-Level SDK error: {e}")
            
        # Fallback to Generated SDK
        try:
            response = objects_api.get_object(
                repository=repo_name,
                ref=branch_name,
                path=file_path
            )
            return response.read()
        except ApiException as e:
            print(f"Generated SDK error: {e}")
            
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
```

## Advanced Integration Patterns

### Custom Repository Manager
```python
import lakefs
from lakefs_sdk import RepositoriesApi, BranchesApi

class AdvancedRepositoryManager:
    def __init__(self, repo_name):
        self.repo_name = repo_name
        self.high_level_repo = lakefs.repository(repo_name)
        self.generated_client = self.high_level_repo.client.sdk
        
        # Initialize Generated SDK APIs
        self.repos_api = RepositoriesApi(self.generated_client)
        self.branches_api = BranchesApi(self.generated_client)
    
    def create_with_advanced_options(self, storage_namespace, **kwargs):
        """Create repository with advanced options"""
        try:
            repo = self.repos_api.create_repository(
                repository_creation=lakefs_sdk.RepositoryCreation(
                    name=self.repo_name,
                    storage_namespace=storage_namespace,
                    default_branch=kwargs.get('default_branch', 'main'),
                    sample_data=kwargs.get('sample_data', False)
                )
            )
            return repo
        except Exception as e:
            print(f"Advanced repository creation failed: {e}")
            return None
    
    def bulk_branch_operations(self, branch_configs):
        """Create multiple branches with different configurations"""
        created_branches = []
        
        for config in branch_configs:
            try:
                branch = self.branches_api.create_branch(
                    repository=self.repo_name,
                    branch_creation=lakefs_sdk.BranchCreation(
                        name=config['name'],
                        source=config.get('source', 'main')
                    )
                )
                created_branches.append(branch)
            except Exception as e:
                print(f"Failed to create branch {config['name']}: {e}")
        
        return created_branches

# Usage
manager = AdvancedRepositoryManager("advanced-repo")

# Create repository with advanced options
repo = manager.create_with_advanced_options(
    storage_namespace="s3://bucket/advanced-repo",
    default_branch="develop",
    sample_data=True
)

# Create multiple branches
branch_configs = [
    {"name": "feature-1", "source": "develop"},
    {"name": "feature-2", "source": "develop"},
    {"name": "hotfix", "source": "main"}
]

branches = manager.bulk_branch_operations(branch_configs)
```

### Performance Optimization
```python
import lakefs
from lakefs_sdk import ObjectsApi
import concurrent.futures

def optimized_batch_operations(repo_name, branch_name, operations):
    """Perform batch operations with optimized Generated SDK calls"""
    repo = lakefs.repository(repo_name)
    objects_api = ObjectsApi(repo.client.sdk)
    
    def execute_operation(operation):
        op_type, path, data = operation
        
        if op_type == "upload":
            return objects_api.upload_object(
                repository=repo_name,
                branch=branch_name,
                path=path,
                content=data
            )
        elif op_type == "delete":
            return objects_api.delete_object(
                repository=repo_name,
                branch=branch_name,
                path=path
            )
    
    # Execute operations in parallel using Generated SDK
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(execute_operation, op) for op in operations]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    return results

# Usage
operations = [
    ("upload", "data/file1.txt", b"Content 1"),
    ("upload", "data/file2.txt", b"Content 2"),
    ("delete", "old/file.txt", None)
]

results = optimized_batch_operations("my-repo", "main", operations)
```

## Best Practices

### When to Use Each Approach

**Use High-Level SDK for:**
- Common operations (upload, download, commit)
- Transaction-based workflows
- Streaming I/O operations
- Simplified error handling

**Use Generated SDK for:**
- Advanced API parameters
- Operations not available in High-Level SDK
- Performance-critical batch operations
- Custom integrations

### Consistent Client Management
```python
import lakefs

class UnifiedLakeFSClient:
    def __init__(self, repo_name):
        self.repo_name = repo_name
        self.high_level_repo = lakefs.repository(repo_name)
        self.generated_client = self.high_level_repo.client.sdk
    
    def get_high_level_branch(self, branch_name):
        """Get High-Level SDK branch object"""
        return self.high_level_repo.branch(branch_name)
    
    def get_generated_api(self, api_class):
        """Get Generated SDK API instance"""
        return api_class(self.generated_client)
    
    def mixed_operation(self, branch_name, file_path, content):
        """Example of mixed High-Level and Generated SDK usage"""
        # Use High-Level SDK for upload
        branch = self.get_high_level_branch(branch_name)
        obj = branch.object(file_path).upload(data=content)
        
        # Use Generated SDK for advanced stats
        objects_api = self.get_generated_api(lakefs_sdk.ObjectsApi)
        stats = objects_api.stat_object(
            repository=self.repo_name,
            ref=branch_name,
            path=file_path,
            presign=True
        )
        
        return obj, stats

# Usage
client = UnifiedLakeFSClient("my-repo")
obj, stats = client.mixed_operation("main", "data/file.txt", "Hello World")
```

## Next Steps

- Review [High-Level SDK documentation](../high-level-sdk/) for comparison
- Check [Generated SDK API reference](api-reference.md) for complete API coverage
- Explore [best practices](../reference/best-practices.md) for optimal usage patterns