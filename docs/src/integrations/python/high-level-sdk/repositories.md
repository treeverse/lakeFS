---
title: Repository Management
description: Managing repositories with the High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "beginner"
use_cases: ["repository-management", "setup", "configuration"]
topics: ["repositories", "management", "configuration", "metadata"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# Repository Management

Learn how to create, configure, and manage lakeFS repositories using the High-Level Python SDK. Repositories are the top-level containers in lakeFS that hold all your data, branches, and version history.

## Repository Concepts

### Repository Structure
A lakeFS repository consists of:
- **Storage namespace**: The underlying storage location (S3 bucket, Azure container, etc.)
- **Default branch**: The main branch created automatically (typically "main")
- **Metadata**: Repository-level configuration and properties
- **Branches and tags**: Version control references within the repository

### Lazy Initialization
Repository objects are created lazily - instantiating a `Repository` object doesn't immediately connect to the server. Operations only execute when you call action methods.

## Creating Repositories

### Basic Repository Creation

```python
import lakefs

# Create a new repository
repo = lakefs.repository("my-repo").create(
    storage_namespace="s3://my-bucket/repos/my-repo"
)

print(f"Repository created: {repo.id}")
print(f"Storage namespace: {repo.properties.storage_namespace}")
print(f"Default branch: {repo.properties.default_branch}")
```

**Expected Output:**
```
Repository created: my-repo
Storage namespace: s3://my-bucket/repos/my-repo
Default branch: main
```

### Repository with Custom Configuration

```python
# Create repository with custom settings
repo = lakefs.repository("custom-repo").create(
    storage_namespace="s3://my-bucket/repos/custom",
    default_branch="develop",
    include_samples=True  # Include sample data
)

print(f"Created with default branch: {repo.properties.default_branch}")
```

**Expected Output:**
```
Created with default branch: develop
```

### Safe Repository Creation

```python
from lakefs.exceptions import ConflictException

try:
    # Try to create repository
    repo = lakefs.repository("existing-repo").create(
        storage_namespace="s3://my-bucket/repos/existing",
        exist_ok=False  # Fail if repository exists
    )
    print("Repository created successfully")
except ConflictException:
    print("Repository already exists")
    # Connect to existing repository instead
    repo = lakefs.repository("existing-repo")
```

### Using exist_ok Parameter

```python
# Create repository or connect to existing one
repo = lakefs.repository("safe-repo").create(
    storage_namespace="s3://my-bucket/repos/safe",
    exist_ok=True  # Don't fail if repository exists
)

# This will either create a new repository or return the existing one
print(f"Repository ready: {repo.id}")
```

## Connecting to Existing Repositories

### Basic Connection

```python
# Connect to existing repository (no server call yet)
repo = lakefs.repository("existing-repo")

# Access properties (triggers server call)
print(f"Repository: {repo.id}")
print(f"Created: {repo.properties.creation_date}")
print(f"Storage: {repo.properties.storage_namespace}")
```

### With Custom Client

```python
from lakefs.client import Client

# Use custom client configuration
client = Client(
    username="custom-access-key",
    password="custom-secret-key",
    host="https://my-lakefs.example.com"
)

repo = lakefs.Repository("my-repo", client=client)
```

## Repository Properties and Metadata

### Accessing Repository Properties

```python
repo = lakefs.repository("my-repo")

# Get repository properties
props = repo.properties
print(f"Repository ID: {props.id}")
print(f"Creation Date: {props.creation_date}")
print(f"Default Branch: {props.default_branch}")
print(f"Storage Namespace: {props.storage_namespace}")

# Properties are cached after first access
print(f"Same properties object: {props is repo.properties}")
```

**Expected Output:**
```
Repository ID: my-repo
Creation Date: 1640995200
Default Branch: main
Storage Namespace: s3://my-bucket/repos/my-repo
Same properties object: True
```

### Repository Metadata

```python
# Access repository metadata
metadata = repo.metadata
print(f"Metadata: {metadata}")

# Metadata is a dictionary of key-value pairs
for key, value in metadata.items():
    print(f"{key}: {value}")
```

**Expected Output:**
```
Metadata: {'created_by': 'admin', 'purpose': 'production'}
created_by: admin
purpose: production
```

## Listing Repositories

### List All Repositories

```python
# List all repositories (using default client)
for repo in lakefs.repositories():
    print(f"Repository: {repo.id}")
    print(f"  Storage: {repo.properties.storage_namespace}")
    print(f"  Default branch: {repo.properties.default_branch}")
    print(f"  Created: {repo.properties.creation_date}")
    print()
```

**Expected Output:**
```
Repository: repo1
  Storage: s3://bucket1/repos/repo1
  Default branch: main
  Created: 1640995200

Repository: repo2
  Storage: s3://bucket2/repos/repo2
  Default branch: develop
  Created: 1641081600
```

### Filtered Repository Listing

```python
# List repositories with prefix filter
for repo in lakefs.repositories(prefix="prod-"):
    print(f"Production repo: {repo.id}")

# List repositories with pagination
for repo in lakefs.repositories(after="repo-m", max_amount=10):
    print(f"Repository: {repo.id}")
```

### Using Custom Client for Listing

```python
from lakefs.client import Client

client = Client(host="https://my-lakefs.example.com")

# List repositories using custom client
for repo in lakefs.repositories(client=client, prefix="team-"):
    print(f"Team repository: {repo.id}")
```

## Repository Navigation

### Accessing Branches

```python
repo = lakefs.repository("my-repo")

# Get specific branch
main_branch = repo.branch("main")
dev_branch = repo.branch("development")

# List all branches
print("All branches:")
for branch in repo.branches():
    print(f"  {branch.id}")

# List branches with filtering
print("Feature branches:")
for branch in repo.branches(prefix="feature-"):
    print(f"  {branch.id}")
```

### Accessing Tags

```python
# Get specific tag
v1_tag = repo.tag("v1.0.0")

# List all tags
print("All tags:")
for tag in repo.tags():
    print(f"  {tag.id}")

# List recent tags
print("Recent tags:")
for tag in repo.tags(max_amount=5):
    print(f"  {tag.id}")
```

### Accessing References

```python
# Access any reference (branch, commit, or tag)
main_ref = repo.ref("main")
commit_ref = repo.ref("c7a632d74f46c...")
tag_ref = repo.ref("v1.0.0")

# Using ref expressions
previous_commit = repo.ref("main~1")  # Previous commit on main
head_commit = repo.commit("c7a632d74f46c...")  # Specific commit
```

## Repository Operations

### Repository Information

```python
repo = lakefs.repository("my-repo")

# Display comprehensive repository information
def show_repo_info(repo):
    props = repo.properties
    metadata = repo.metadata
    
    print(f"Repository: {props.id}")
    print(f"Created: {props.creation_date}")
    print(f"Storage: {props.storage_namespace}")
    print(f"Default Branch: {props.default_branch}")
    
    if metadata:
        print("Metadata:")
        for key, value in metadata.items():
            print(f"  {key}: {value}")
    
    # Count branches and tags
    branch_count = len(list(repo.branches(max_amount=1000)))
    tag_count = len(list(repo.tags(max_amount=1000)))
    
    print(f"Branches: {branch_count}")
    print(f"Tags: {tag_count}")

show_repo_info(repo)
```

### Repository Statistics

```python
def get_repo_stats(repo):
    """Get comprehensive repository statistics"""
    stats = {
        'id': repo.id,
        'properties': repo.properties._asdict(),
        'metadata': repo.metadata,
        'branches': [],
        'tags': [],
        'total_objects': 0
    }
    
    # Collect branch information
    for branch in repo.branches():
        branch_info = {
            'id': branch.id,
            'commit_id': branch.get_commit().id,
            'object_count': len(list(branch.objects(max_amount=1000)))
        }
        stats['branches'].append(branch_info)
        stats['total_objects'] += branch_info['object_count']
    
    # Collect tag information
    for tag in repo.tags():
        stats['tags'].append({
            'id': tag.id,
            'commit_id': tag.get_commit().id
        })
    
    return stats

# Get and display stats
stats = get_repo_stats(repo)
print(f"Repository {stats['id']} has {len(stats['branches'])} branches and {len(stats['tags'])} tags")
print(f"Total objects across all branches: {stats['total_objects']}")
```

## Repository Deletion

### Basic Deletion

```python
from lakefs.exceptions import NotFoundException

repo = lakefs.repository("repo-to-delete")

try:
    repo.delete()
    print("Repository deleted successfully")
except NotFoundException:
    print("Repository not found")
```

### Safe Deletion with Confirmation

```python
def delete_repository_safely(repo_id: str, confirm: bool = False):
    """Safely delete a repository with confirmation"""
    repo = lakefs.repository(repo_id)
    
    if not confirm:
        print(f"This will permanently delete repository '{repo_id}'")
        print("Set confirm=True to proceed")
        return False
    
    try:
        # Show repository info before deletion
        props = repo.properties
        print(f"Deleting repository: {props.id}")
        print(f"Storage namespace: {props.storage_namespace}")
        
        repo.delete()
        print("Repository deleted successfully")
        return True
        
    except NotFoundException:
        print(f"Repository '{repo_id}' not found")
        return False
    except Exception as e:
        print(f"Error deleting repository: {e}")
        return False

# Usage
delete_repository_safely("test-repo", confirm=True)
```

## Error Handling

### Common Repository Errors

```python
from lakefs.exceptions import (
    NotFoundException, 
    ConflictException, 
    NotAuthorizedException,
    ServerException
)

def handle_repository_operations():
    try:
        # Try various repository operations
        repo = lakefs.repository("my-repo").create(
            storage_namespace="s3://my-bucket/repos/my-repo"
        )
        
    except ConflictException:
        print("Repository already exists")
        repo = lakefs.repository("my-repo")
        
    except NotAuthorizedException:
        print("Not authorized to create repository")
        return None
        
    except ServerException as e:
        print(f"Server error: {e}")
        return None
        
    try:
        # Access repository properties
        props = repo.properties
        print(f"Repository: {props.id}")
        
    except NotFoundException:
        print("Repository not found")
        return None
        
    return repo
```

### Validation and Best Practices

```python
def validate_repository_config(repo_id: str, storage_namespace: str):
    """Validate repository configuration before creation"""
    
    # Validate repository ID
    if not repo_id or not repo_id.replace('-', '').replace('_', '').isalnum():
        raise ValueError("Repository ID must contain only alphanumeric characters, hyphens, and underscores")
    
    # Validate storage namespace
    if not storage_namespace.startswith(('s3://', 'gs://', 'azure://', 'file://')):
        raise ValueError("Storage namespace must start with a valid protocol (s3://, gs://, azure://, file://)")
    
    print(f"Configuration valid for repository: {repo_id}")
    return True

# Usage
try:
    validate_repository_config("my-repo", "s3://my-bucket/repos/my-repo")
    repo = lakefs.repository("my-repo").create(
        storage_namespace="s3://my-bucket/repos/my-repo"
    )
except ValueError as e:
    print(f"Configuration error: {e}")
```

## Advanced Repository Patterns

### Repository Factory Pattern

```python
class RepositoryManager:
    """Centralized repository management"""
    
    def __init__(self, client=None):
        self.client = client or lakefs.Client()
        self._repositories = {}
    
    def get_or_create_repository(self, repo_id: str, storage_namespace: str, **kwargs):
        """Get existing repository or create new one"""
        if repo_id in self._repositories:
            return self._repositories[repo_id]
        
        try:
            repo = lakefs.Repository(repo_id, client=self.client).create(
                storage_namespace=storage_namespace,
                exist_ok=True,
                **kwargs
            )
            self._repositories[repo_id] = repo
            return repo
            
        except Exception as e:
            print(f"Failed to get/create repository {repo_id}: {e}")
            return None
    
    def list_managed_repositories(self):
        """List all managed repositories"""
        return list(self._repositories.keys())

# Usage
manager = RepositoryManager()
repo1 = manager.get_or_create_repository("repo1", "s3://bucket/repo1")
repo2 = manager.get_or_create_repository("repo2", "s3://bucket/repo2")
```

### Repository Cloning Pattern

```python
def clone_repository_structure(source_repo_id: str, target_repo_id: str, 
                             target_storage: str):
    """Clone repository structure (branches and tags) to new repository"""
    
    source = lakefs.repository(source_repo_id)
    target = lakefs.repository(target_repo_id).create(
        storage_namespace=target_storage,
        exist_ok=True
    )
    
    # Clone branches
    for branch in source.branches():
        if branch.id != source.properties.default_branch:
            try:
                target.branch(branch.id).create(
                    source_reference=source.properties.default_branch
                )
                print(f"Cloned branch: {branch.id}")
            except ConflictException:
                print(f"Branch {branch.id} already exists")
    
    # Clone tags
    for tag in source.tags():
        try:
            commit_id = tag.get_commit().id
            target.tag(tag.id).create(commit_id=commit_id)
            print(f"Cloned tag: {tag.id}")
        except ConflictException:
            print(f"Tag {tag.id} already exists")
    
    return target

# Usage
cloned_repo = clone_repository_structure("source-repo", "target-repo", "s3://bucket/target")
```

## Key Points

- **Lazy evaluation**: Repository objects don't connect to server until you access properties or call methods
- **Caching**: Repository properties are cached after first access for performance
- **Error handling**: Use specific exception types for robust error handling
- **Navigation**: Use repository objects as entry points to access branches, tags, and references
- **Metadata**: Repository metadata provides additional configuration and information

## See Also

**High-Level SDK Workflow:**
- **[Quickstart Guide](quickstart.md)** - Basic repository operations and setup
- **[Branch Operations](branches-and-commits.md)** - Working with branches and commits
- **[Object Management](objects-and-io.md)** - Managing objects within repositories
- **[Transaction Patterns](transactions.md)** - Atomic operations across repositories
- **[Import/Export Operations](imports-and-exports.md)** - Bulk data operations

**Repository Management:**
- **[High-Level SDK Overview](index.md)** - Architecture and key concepts
- **[Advanced Features](advanced.md)** - Performance optimization and patterns
- **[Generated SDK Access](../generated-sdk/direct-access.md)** - Direct API access for advanced operations

**Alternative Approaches:**
- **[Generated SDK Repository API](../generated-sdk/api-reference.md#repository-operations)** - Direct API access
- **[lakefs-spec Limitations](../lakefs-spec/index.md#when-to-use-lakefs-spec)** - Why lakefs-spec doesn't support repository management
- **[Boto3 Limitations](../boto3/index.md#key-features)** - S3 compatibility doesn't include repository operations

**Learning Resources:**
- **[Data Science Tutorial](../tutorials/data-science-workflow.md)** - Repository setup for data science workflows
- **[ETL Pipeline Tutorial](../tutorials/etl-pipeline.md)** - Repository management in data pipelines
- **[ML Experiment Tracking](../tutorials/ml-experiment-tracking.md)** - Repository organization for ML projects

**Reference Materials:**
- **[API Comparison](../reference/api-comparison.md#core-repository-operations)** - Repository features across SDKs
- **[Best Practices](../reference/best-practices.md#repository-management)** - Production deployment guidance
- **[Troubleshooting](../reference/troubleshooting.md#repository-issues)** - Common repository issues and solutions

**External Resources:**
- **[lakeFS Repository Concepts](https://docs.lakefs.io/understand/model.html#repository){:target="_blank"}** - Core lakeFS repository concepts
- **[High-Level SDK API Reference](https://pydocs-lakefs.lakefs.io/repository.html){:target="_blank"}** - Complete repository API documentation