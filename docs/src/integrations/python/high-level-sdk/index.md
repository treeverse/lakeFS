---
title: High-Level Python SDK
description: Comprehensive documentation for the lakeFS High-Level Python SDK
sdk_types: ["high-level"]
difficulty: "beginner"
use_cases: ["general", "data-pipelines", "etl", "transactions"]
topics: ["overview", "architecture", "concepts"]
audience: ["data-engineers", "developers", "python-developers"]
last_updated: "2024-01-15"
---

# High-Level Python SDK

The High-Level Python SDK provides a simplified, Pythonic interface for working with lakeFS. Built on top of the Generated SDK, it offers advanced features like transactions, streaming I/O, and intuitive object management while maintaining the full power of the underlying API.

## Key Concepts

### Repository-Centric Design
The High-Level SDK is organized around repositories as the primary entry point. All operations flow from repository objects to branches, commits, and objects, providing a natural hierarchy that mirrors lakeFS's data model.

### Lazy Evaluation
Objects are created lazily - creating a `Repository`, `Branch`, or `StoredObject` instance doesn't immediately interact with the server. Operations only execute when you call action methods like `create()`, `upload()`, or `commit()`.

### Fluent Interface
The SDK supports method chaining for common workflows:
```python
repo = lakefs.repository("my-repo").create(storage_namespace="s3://bucket/path")
commit = repo.branch("main").object("file.txt").upload(data="content").commit("Add file")
```

### Built-in Error Handling
All operations include comprehensive error handling with specific exception types for different failure scenarios, making it easier to build robust applications.

## Key Features

- **Simplified API** - Pythonic interface that abstracts complex operations
- **Transaction Support** - Atomic operations with automatic rollback capabilities
- **Streaming I/O** - File-like objects for efficient handling of large datasets
- **Import Management** - Sophisticated data import operations with progress tracking
- **Batch Operations** - Efficient bulk operations for better performance
- **Generated SDK Access** - Direct access to underlying Generated SDK when needed
- **Automatic Authentication** - Seamless credential discovery from environment or config files

## Architecture Overview

The High-Level SDK is structured in layers:

```
High-Level SDK (lakefs package)
├── Repository Management
├── Branch & Reference Operations  
├── Object I/O & Streaming
├── Transaction Management
├── Import/Export Operations
└── Generated SDK (lakefs_sdk)
    └── Direct API Access
```

## Core Classes

### Repository
The main entry point for all operations. Represents a lakeFS repository and provides access to branches, tags, and metadata.

### Branch
Extends Reference with write capabilities. Supports object uploads, commits, merges, and transaction management.

### Reference  
Read-only access to any lakeFS reference (branch, commit, or tag). Provides object listing and reading capabilities.

### StoredObject & WriteableObject
Represent objects in lakeFS with full I/O capabilities including streaming, metadata management, and batch operations.

### ImportManager
Handles complex data import operations with support for various source types and progress monitoring.

## Documentation Sections

- **[Quickstart](quickstart.md)** - Get started with basic operations
- **[Repositories](repositories.md)** - Repository management operations
- **[Branches & Commits](branches-and-commits.md)** - Version control operations
- **[Objects & I/O](objects-and-io.md)** - Object operations and streaming
- **[Imports & Exports](imports-and-exports.md)** - Data import/export operations
- **[Transactions](transactions.md)** - Atomic operation patterns
- **[Advanced Features](advanced.md)** - Advanced patterns and optimization

## Quick Example

```python
import lakefs

# Create a repository
repo = lakefs.repository("my-repo").create(
    storage_namespace="s3://my-bucket/repos/my-repo"
)

# Create a branch and upload data
branch = repo.branch("feature-branch").create(source_reference="main")
obj = branch.object("data/file.txt").upload(data="Hello, lakeFS!")

# Commit changes
commit = branch.commit(message="Add new data file")
print(f"Committed: {commit.id}")
```

## Installation

```bash
pip install lakefs
```

## Authentication

The SDK automatically discovers credentials from:
1. Environment variables (`LAKEFS_ACCESS_KEY_ID`, `LAKEFS_SECRET_ACCESS_KEY`, `LAKEFS_ENDPOINT`)
2. Configuration file (`~/.lakectl.yaml`)
3. Explicit client configuration

## When to Use High-Level SDK

Choose the High-Level SDK when you need:
- **Simplified workflows** - Common operations with minimal code
- **Transaction support** - Atomic operations across multiple changes
- **Streaming I/O** - Efficient handling of large files
- **Import management** - Complex data ingestion workflows
- **Python-first experience** - Pythonic interfaces and error handling

For direct API control or operations not covered by the high-level interface, you can access the underlying Generated SDK through the `client` property.

## Next Steps

Start with the [quickstart guide](quickstart.md) to learn the basics, then explore specific features in the detailed sections.

## See Also

**Getting Started:**
- [Python SDK Overview](../index.md) - Compare all Python SDK options
- [Installation Guide](../getting-started.md) - Setup and authentication
- [SDK Selection Guide](../index.md#sdk-selection-decision-matrix) - Choose the right SDK

**High-Level SDK Documentation:**
- [Quickstart Guide](quickstart.md) - Basic operations and examples
- [Repository Management](repositories.md) - Create, configure, and manage repositories
- [Version Control](branches-and-commits.md) - Branches, commits, and merging
- [Object Operations](objects-and-io.md) - Upload, download, and streaming I/O
- [Data Import/Export](imports-and-exports.md) - Bulk data operations
- [Transaction Patterns](transactions.md) - Atomic operations and rollback
- [Advanced Features](advanced.md) - Performance optimization and patterns

**Alternative SDK Options:**
- [Generated SDK](../generated-sdk/index.md) - Direct API access for advanced use cases
- [lakefs-spec](../lakefs-spec/index.md) - Filesystem interface for data science
- [Boto3 Integration](../boto3/index.md) - S3-compatible operations

**Learning Resources:**
- [Data Science Tutorial](../tutorials/data-science-workflow.md) - End-to-end data analysis workflow
- [ETL Pipeline Tutorial](../tutorials/etl-pipeline.md) - Building data pipelines
- [ML Experiment Tracking](../tutorials/ml-experiment-tracking.md) - Model versioning

**Reference Materials:**
- [API Comparison](../reference/api-comparison.md) - Feature comparison across SDKs
- [Best Practices](../reference/best-practices.md) - Production deployment guidance
- [Troubleshooting](../reference/troubleshooting.md) - Common issues and solutions

**External Resources:**
- [High-Level SDK API Reference](https://pydocs-lakefs.lakefs.io){:target="_blank"} - Complete API documentation
- [Generated SDK Access](../generated-sdk/direct-access.md) - Using Generated SDK from High-Level SDK