---
title: lakefs-spec Integration
description: Filesystem API for lakeFS using lakefs-spec with comprehensive setup and configuration guide
sdk_types: ["lakefs-spec"]
difficulty: "beginner"
use_cases: ["data-science", "filesystem-operations", "pandas-integration", "jupyter-notebooks"]
topics: ["filesystem", "fsspec", "data-science", "uri-patterns"]
audience: ["data-scientists", "analysts", "python-developers"]
last_updated: "2024-01-15"
---

# lakefs-spec Integration

The [lakefs-spec](https://lakefs-spec.org/) project provides a filesystem-like interface to lakeFS, built on the [fsspec](https://github.com/fsspec/filesystem_spec) ecosystem. This enables seamless integration with data science libraries like pandas, dask, and other fsspec-compatible tools.

## Overview

lakefs-spec implements the fsspec protocol for lakeFS, allowing you to use lakeFS repositories as if they were regular filesystems. This approach is particularly powerful for data science workflows where you want to leverage existing fsspec-compatible libraries without changing your code structure.

### Key Features

- **Filesystem API** - Standard filesystem operations (read, write, list, delete, metadata)
- **fsspec Compatibility** - Works seamlessly with pandas, dask, and 100+ other fsspec libraries
- **URI-based Access** - Simple `lakefs://` URI scheme for intuitive path handling
- **Transaction Support** - Atomic operations with automatic rollback capabilities
- **Data Science Integration** - Native support for popular data science workflows
- **Path Resolution** - Intelligent handling of lakeFS repository, branch, and object paths

### When to Use lakefs-spec

Choose lakefs-spec when you need:

- **Data Science Workflows** - Working with pandas, dask, or other data science tools
- **Filesystem Semantics** - Standard filesystem operations and path handling
- **Existing fsspec Code** - Migrating from other fsspec filesystems (S3, GCS, Azure)
- **Simple Integration** - Minimal code changes for lakeFS adoption
- **Library Compatibility** - Access to the broad fsspec ecosystem

## Installation and Setup

### Installation

Install lakefs-spec using pip:

```bash
# Basic installation
pip install lakefs-spec

# With optional dependencies for enhanced functionality
pip install lakefs-spec[pandas,dask]

# Development installation with all dependencies
pip install lakefs-spec[dev]
```

### System Requirements

- Python 3.8 or higher
- lakeFS server 0.90.0 or higher
- Network access to your lakeFS instance

## Configuration and Authentication

lakefs-spec supports multiple authentication methods to connect to your lakeFS instance.

### Method 1: Environment Variables

Set environment variables for automatic authentication:

```bash
export LAKEFS_ENDPOINT="https://your-lakefs-instance.com"
export LAKEFS_ACCESS_KEY_ID="your-access-key"
export LAKEFS_SECRET_ACCESS_KEY="your-secret-key"
```

```python
from lakefs_spec import LakeFSFileSystem

# Automatically uses environment variables
fs = LakeFSFileSystem()
```

### Method 2: lakectl Configuration File

lakefs-spec automatically discovers credentials from your lakectl configuration:

```yaml
# ~/.lakectl.yaml
credentials:
  access_key_id: your-access-key
  secret_access_key: your-secret-key
server:
  endpoint_url: https://your-lakefs-instance.com
```

```python
from lakefs_spec import LakeFSFileSystem

# Automatically uses ~/.lakectl.yaml
fs = LakeFSFileSystem()
```

### Method 3: Direct Configuration

Pass configuration directly to the filesystem:

```python
from lakefs_spec import LakeFSFileSystem

# Direct configuration
fs = LakeFSFileSystem(
    host="https://your-lakefs-instance.com",
    username="your-access-key",
    password="your-secret-key"
)
```

### Method 4: Configuration Dictionary

Use a configuration dictionary for complex setups:

```python
from lakefs_spec import LakeFSFileSystem

config = {
    "host": "https://your-lakefs-instance.com",
    "username": "your-access-key", 
    "password": "your-secret-key",
    "verify_ssl": True,
    "timeout": 30,
    "retries": 3
}

fs = LakeFSFileSystem(**config)
```

### Advanced Configuration Options

lakefs-spec supports additional configuration options for production environments:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem(
    host="https://your-lakefs-instance.com",
    username="your-access-key",
    password="your-secret-key",
    
    # SSL Configuration
    verify_ssl=True,
    ssl_cert_path="/path/to/cert.pem",
    
    # Connection Settings
    timeout=60,
    retries=5,
    retry_delay=1.0,
    
    # Performance Settings
    cache_size=1000,
    block_size=5 * 1024 * 1024,  # 5MB blocks
    
    # Logging
    log_level="INFO"
)
```

## URI Patterns and Path Resolution

lakefs-spec uses a structured URI scheme to access lakeFS resources:

### URI Structure

```
lakefs://[repository]/[branch]/[path/to/object]
```

### Path Components

- **Repository**: The lakeFS repository name
- **Branch**: The branch or commit reference
- **Path**: The object path within the repository

### URI Examples

```python
# Basic file access
'lakefs://my-repo/main/data/file.csv'

# Nested directory structure
'lakefs://analytics/feature-branch/datasets/2024/january/sales.parquet'

# Root directory listing
'lakefs://my-repo/main/'

# Specific commit reference
'lakefs://my-repo/c7a4b2f8d9e1a3b5c6d7e8f9/data/snapshot.json'
```

### Path Resolution Examples

```python
from lakefs_spec import LakeFSFileSystem
import pandas as pd

fs = LakeFSFileSystem()

# List repositories
repos = fs.ls('lakefs://')
print("Available repositories:", repos)

# List branches in a repository
branches = fs.ls('lakefs://my-repo/')
print("Available branches:", branches)

# List objects in a branch
objects = fs.ls('lakefs://my-repo/main/')
print("Objects in main branch:", objects)

# Check if path exists
exists = fs.exists('lakefs://my-repo/main/data/file.csv')
print("File exists:", exists)

# Get path information
info = fs.info('lakefs://my-repo/main/data/file.csv')
print("File info:", info)
```

### Working with Different Path Types

```python
# Absolute paths
df = pd.read_csv('lakefs://repo/main/data/file.csv')

# Relative paths (when working within a specific context)
fs = LakeFSFileSystem()
with fs.transaction('lakefs://repo/feature-branch/') as tx:
    # Within transaction, paths are relative to the transaction root
    df = pd.read_csv('data/file.csv')  # Resolves to lakefs://repo/feature-branch/data/file.csv
```

## Quick Start Example

Here's a complete example showing lakefs-spec setup and basic usage:

```python
import pandas as pd
from lakefs_spec import LakeFSFileSystem

# Initialize filesystem (uses environment variables or ~/.lakectl.yaml)
fs = LakeFSFileSystem()

# Create sample data
data = {
    'timestamp': pd.date_range('2024-01-01', periods=100, freq='H'),
    'value': range(100),
    'category': ['A', 'B'] * 50
}
df = pd.DataFrame(data)

# Write data to lakeFS
df.to_parquet('lakefs://my-repo/main/data/sample.parquet', index=False)

# Read data back
df_loaded = pd.read_parquet('lakefs://my-repo/main/data/sample.parquet')
print(f"Loaded {len(df_loaded)} rows")

# List files in the data directory
files = fs.ls('lakefs://my-repo/main/data/')
print("Files in data directory:", files)

# Check file metadata
info = fs.info('lakefs://my-repo/main/data/sample.parquet')
print("File size:", info['size'], "bytes")
```

## Filesystem API Concepts

lakefs-spec implements standard filesystem operations that work consistently across different storage backends:

### Core Operations

- **Read Operations**: `open()`, `cat()`, `cat_file()`
- **Write Operations**: `open()` with write mode, `pipe()`
- **Directory Operations**: `ls()`, `mkdir()`, `rmdir()`
- **File Operations**: `rm()`, `mv()`, `cp()`
- **Metadata Operations**: `info()`, `exists()`, `size()`

### Transaction Support

lakefs-spec provides atomic operations through transactions:

```python
# Atomic multi-file operation
with fs.transaction('lakefs://repo/branch/') as tx:
    # All operations within this block are atomic
    df1.to_parquet('data/file1.parquet')
    df2.to_parquet('data/file2.parquet')
    # Automatically commits on successful completion
    # Automatically rolls back on any error
```

## Documentation Sections

Explore detailed documentation for specific use cases:

- **[Filesystem API](filesystem-api.md)** - Complete filesystem operations reference
- **[Data Science Integrations](integrations.md)** - pandas, dask, and other library examples
- **[Transactions](transactions.md)** - Atomic operation patterns and best practices

## Troubleshooting

### Common Issues

**Authentication Errors**
```python
# Verify your configuration
fs = LakeFSFileSystem()
try:
    repos = fs.ls('lakefs://')
    print("Authentication successful")
except Exception as e:
    print(f"Authentication failed: {e}")
```

**Path Resolution Issues**
```python
# Check if repository and branch exist
if fs.exists('lakefs://my-repo/'):
    print("Repository exists")
    if fs.exists('lakefs://my-repo/main/'):
        print("Branch exists")
    else:
        print("Branch not found")
else:
    print("Repository not found")
```

**Connection Issues**
```python
# Test connection with timeout
fs = LakeFSFileSystem(timeout=10)
try:
    fs.ls('lakefs://')
    print("Connection successful")
except TimeoutError:
    print("Connection timeout - check network and endpoint")
```

## Next Steps

- Learn about [filesystem operations](filesystem-api.md) for detailed API usage
- Explore [data science integrations](integrations.md) for pandas, dask, and other libraries
- Understand [transaction patterns](transactions.md) for atomic operations
- Check the [lakefs-spec documentation](https://lakefs-spec.org/) for advanced features

## See Also

**SDK Selection and Comparison:**
- [Python SDK Overview](../index.md) - Compare all Python SDK options
- [SDK Decision Matrix](../index.md#sdk-selection-decision-matrix) - Choose the right SDK for your use case
- [API Feature Comparison](../reference/api-comparison.md) - Detailed feature comparison across SDKs

**lakefs-spec Documentation:**
- [Filesystem API Reference](filesystem-api.md) - Complete filesystem operations guide
- [Data Science Integrations](integrations.md) - pandas, dask, and other library examples
- [Transaction Patterns](transactions.md) - Atomic operations and rollback scenarios

**Alternative SDK Options:**
- [High-Level SDK](../high-level-sdk/index.md) - Simplified Python interface with transactions
- [High-Level SDK Quickstart](../high-level-sdk/quickstart.md) - Object-oriented interface
- [Generated SDK](../generated-sdk/index.md) - Direct API access for custom operations
- [Boto3 Integration](../boto3/index.md) - S3-compatible operations

**Setup and Configuration:**
- [Installation Guide](../getting-started.md) - Complete setup instructions for all SDKs
- [Authentication Methods](../getting-started.md#authentication-and-configuration) - All credential configuration options
- [Best Practices](../reference/best-practices.md#configuration) - Production configuration guidance

**Data Science Workflows:**
- [Data Science Tutorial](../tutorials/data-science-workflow.md) - End-to-end data analysis workflow
- [pandas Integration Examples](integrations.md#pandas-integration) - Working with DataFrames
- [Jupyter Notebook Patterns](../tutorials/data-science-workflow.md#jupyter-notebooks) - Interactive analysis

**Learning Resources:**
- [ETL Pipeline Tutorial](../tutorials/etl-pipeline.md) - Building data pipelines with filesystem operations
- [ML Experiment Tracking](../tutorials/ml-experiment-tracking.md) - Model versioning workflows
- [fsspec Ecosystem Guide](integrations.md#fsspec-ecosystem) - Compatible libraries and tools

**Reference Materials:**
- [URI Pattern Guide](filesystem-api.md#uri-patterns) - Understanding lakefs:// URIs
- [Error Handling](../reference/troubleshooting.md#lakefs-spec-issues) - Common issues and solutions
- [Performance Optimization](../reference/best-practices.md#lakefs-spec-performance) - Optimize filesystem operations

**External Resources:**
- [lakefs-spec Documentation](https://lakefs-spec.org/){:target="_blank"} - Official lakefs-spec documentation
- [fsspec Documentation](https://filesystem-spec.readthedocs.io/){:target="_blank"} - Core filesystem specification
- [pandas I/O Documentation](https://pandas.pydata.org/docs/user_guide/io.html){:target="_blank"} - pandas file operations
- [dask DataFrame Documentation](https://docs.dask.org/en/stable/dataframe.html){:target="_blank"} - dask integration patterns