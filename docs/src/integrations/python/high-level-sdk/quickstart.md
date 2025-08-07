---
title: High-Level SDK Quickstart
description: Quick start guide for the lakeFS High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "beginner"
use_cases: ["getting-started", "basic-operations", "first-steps"]
topics: ["quickstart", "examples", "workflow"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# High-Level SDK Quickstart

Get started quickly with the lakeFS High-Level Python SDK. This guide covers the essential operations you'll need for most workflows.

## Installation

```bash
pip install lakefs
```

## Prerequisites

Before starting, ensure you have:
- A running lakeFS server
- Valid credentials (access key and secret key)
- A storage namespace (S3 bucket, Azure container, or local path)

## Authentication Setup

The SDK automatically discovers credentials from multiple sources:

### Environment Variables
```bash
export LAKEFS_ACCESS_KEY_ID="your-access-key"
export LAKEFS_SECRET_ACCESS_KEY="your-secret-key"
export LAKEFS_ENDPOINT="http://localhost:8000"
```

### Configuration File
Create `~/.lakectl.yaml`:
```yaml
credentials:
  access_key_id: your-access-key
  secret_access_key: your-secret-key
server:
  endpoint_url: http://localhost:8000
```

### Explicit Configuration
```python
import lakefs
from lakefs.client import Client

client = Client(
    username="your-access-key",
    password="your-secret-key", 
    host="http://localhost:8000"
)

repo = lakefs.Repository("my-repo", client=client)
```

## Basic Workflow

### 1. Import the SDK

```python
import lakefs
```

### 2. Create a Repository

```python
# Create a new repository
repo = lakefs.repository("quickstart-repo").create(
    storage_namespace="s3://my-bucket/repos/quickstart",
    default_branch="main",
    exist_ok=True  # Don't fail if repository already exists
)

print(f"Repository created: {repo.id}")
print(f"Default branch: {repo.properties.default_branch}")
```

**Expected Output:**
```
Repository created: quickstart-repo
Default branch: main
```

### 3. Work with Branches

```python
# Get the main branch
main_branch = repo.branch("main")

# Create a new feature branch
feature_branch = repo.branch("feature-branch").create(source_reference="main")

print(f"Created branch: {feature_branch.id}")
print(f"Source: {feature_branch.get_commit().id}")
```

**Expected Output:**
```
Created branch: feature-branch
Source: c7a632d74f46c...
```

### 4. Upload and Manage Objects

```python
# Upload a simple text file
obj = feature_branch.object("data/sample.txt").upload(
    data="Hello from lakeFS!",
    content_type="text/plain"
)

print(f"Uploaded: {obj.path}")
print(f"Size: {obj.size_bytes} bytes")

# Upload binary data
with open("local-file.csv", "rb") as f:
    csv_obj = feature_branch.object("data/dataset.csv").upload(
        data=f,
        content_type="text/csv"
    )

print(f"Uploaded CSV: {csv_obj.path}")
```

**Expected Output:**
```
Uploaded: data/sample.txt
Size: 18 bytes
Uploaded CSV: data/dataset.csv
```

### 5. Read Objects

```python
# Read text content directly
content = feature_branch.object("data/sample.txt").reader().read()
print(f"Content: {content}")

# Stream large files
with feature_branch.object("data/dataset.csv").reader(mode="r") as f:
    for line_num, line in enumerate(f, 1):
        print(f"Line {line_num}: {line.strip()}")
        if line_num >= 3:  # Show first 3 lines
            break
```

**Expected Output:**
```
Content: Hello from lakeFS!
Line 1: name,age,city
Line 2: Alice,30,New York
Line 3: Bob,25,San Francisco
```

### 6. Commit Changes

```python
# Check what's changed
changes = list(feature_branch.uncommitted())
print(f"Uncommitted changes: {len(changes)}")
for change in changes:
    print(f"  {change.type}: {change.path}")

# Commit the changes
commit = feature_branch.commit(
    message="Add sample data files",
    metadata={"author": "quickstart-guide", "purpose": "demo"}
)

print(f"Commit ID: {commit.id}")
print(f"Message: {commit.message}")
```

**Expected Output:**
```
Uncommitted changes: 2
  added: data/sample.txt
  added: data/dataset.csv
Commit ID: a1b2c3d4e5f6...
Message: Add sample data files
```

### 7. Merge to Main

```python
# Merge feature branch to main
merge_result = main_branch.merge(
    source_ref="feature-branch",
    message="Merge feature branch with sample data"
)

print(f"Merge commit: {merge_result.id}")

# Verify the files are now in main
main_objects = list(main_branch.objects(prefix="data/"))
print(f"Objects in main: {[obj.path for obj in main_objects]}")
```

**Expected Output:**
```
Merge commit: f6e5d4c3b2a1...
Objects in main: ['data/sample.txt', 'data/dataset.csv']
```

## Common Patterns

### Working with Existing Repositories

```python
# Connect to existing repository
repo = lakefs.repository("existing-repo")

# List all branches
branches = list(repo.branches())
print(f"Branches: {[b.id for b in branches]}")

# Get specific branch
dev_branch = repo.branch("development")
```

### Batch Operations

```python
# Upload multiple files efficiently
files_to_upload = [
    ("data/file1.txt", "Content 1"),
    ("data/file2.txt", "Content 2"), 
    ("data/file3.txt", "Content 3")
]

branch = repo.branch("batch-demo").create(source_reference="main")

for path, content in files_to_upload:
    branch.object(path).upload(data=content)

# Commit all at once
commit = branch.commit("Add multiple files")
```

### Error Handling

```python
from lakefs.exceptions import NotFoundException, ConflictException

try:
    # Try to create a branch that might already exist
    branch = repo.branch("existing-branch").create(source_reference="main")
except ConflictException:
    # Branch already exists, get it instead
    branch = repo.branch("existing-branch")
    print("Using existing branch")

try:
    # Try to read a file that might not exist
    content = branch.object("missing-file.txt").reader().read()
except NotFoundException:
    print("File not found, creating it...")
    branch.object("missing-file.txt").upload(data="Default content")
```

## Complete Example

Here's a complete workflow that demonstrates the key concepts:

```python
import lakefs
from lakefs.exceptions import ConflictException

def quickstart_workflow():
    # Create or connect to repository
    repo = lakefs.repository("demo-repo").create(
        storage_namespace="s3://my-bucket/demo",
        exist_ok=True
    )
    
    # Create feature branch
    try:
        feature = repo.branch("add-data").create(source_reference="main")
    except ConflictException:
        feature = repo.branch("add-data")
    
    # Add some data
    feature.object("users.csv").upload(
        data="name,email\nAlice,alice@example.com\nBob,bob@example.com"
    )
    
    feature.object("config.json").upload(
        data='{"version": "1.0", "environment": "production"}'
    )
    
    # Commit changes
    commit = feature.commit("Add user data and configuration")
    
    # Merge to main
    main = repo.branch("main")
    merge_result = main.merge(
        source_ref="add-data",
        message="Merge user data"
    )
    
    print(f"Workflow complete! Merge commit: {merge_result.id}")
    
    # List final objects
    objects = list(main.objects())
    print(f"Repository now contains: {[obj.path for obj in objects]}")

if __name__ == "__main__":
    quickstart_workflow()
```

## Key Points

- **Lazy evaluation**: Objects are created without server calls until you perform actions
- **Fluent interface**: Chain operations for concise workflows
- **Automatic error handling**: Comprehensive exception types for different scenarios
- **Streaming support**: Efficient handling of large files with file-like objects
- **Batch operations**: Upload multiple objects before committing for better performance

## Next Steps

Now that you understand the basics, explore more advanced features:

- **[Repository Management](repositories.md)** - Advanced repository operations and metadata
- **[Branch Operations](branches-and-commits.md)** - Detailed branching, merging, and versioning
- **[Object I/O](objects-and-io.md)** - Streaming, metadata, and advanced object operations
- **[Transactions](transactions.md)** - Atomic operations and rollback scenarios
- **[Import Operations](imports-and-exports.md)** - Bulk data import and export workflows

## See Also

**Prerequisites and Setup:**
- [Installation Guide](../getting-started.md) - Complete setup instructions
- [Authentication Methods](../getting-started.md#authentication-and-configuration) - All credential configuration options
- [SDK Selection Guide](../index.md#sdk-selection-decision-matrix) - Choose the right SDK

**High-Level SDK Deep Dive:**
- [High-Level SDK Overview](index.md) - Architecture and key concepts
- [Repository Management](repositories.md) - Advanced repository operations and metadata
- [Branch Operations](branches-and-commits.md) - Detailed branching, merging, and versioning
- [Object I/O](objects-and-io.md) - Streaming, metadata, and advanced object operations
- [Transactions](transactions.md) - Atomic operations and rollback scenarios
- [Import Operations](imports-and-exports.md) - Bulk data import and export workflows
- [Advanced Features](advanced.md) - Performance optimization and patterns

**Alternative SDK Options:**
- [Generated SDK Examples](../generated-sdk/examples.md) - Direct API access patterns
- [lakefs-spec Quickstart](../lakefs-spec/filesystem-api.md) - Filesystem-like operations
- [Boto3 S3 Operations](../boto3/s3-operations.md) - S3-compatible interface

**Learning Resources:**
- [Data Science Tutorial](../tutorials/data-science-workflow.md) - End-to-end data analysis workflow
- [ETL Pipeline Tutorial](../tutorials/etl-pipeline.md) - Building production data pipelines
- [ML Experiment Tracking](../tutorials/ml-experiment-tracking.md) - Model versioning workflow

**Reference Materials:**
- [API Comparison](../reference/api-comparison.md) - Feature comparison across all SDKs
- [Best Practices](../reference/best-practices.md) - Production deployment guidance
- [Troubleshooting](../reference/troubleshooting.md) - Common issues and solutions
- [Error Handling Patterns](../reference/troubleshooting.md#error-handling) - Exception handling strategies

**External Resources:**
- [High-Level SDK API Reference](https://pydocs-lakefs.lakefs.io){:target="_blank"} - Complete API documentation
- [lakeFS Concepts](https://docs.lakefs.io/understand/){:target="_blank"} - Core lakeFS concepts and terminology