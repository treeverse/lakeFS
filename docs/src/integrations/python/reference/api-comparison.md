---
title: API Comparison
description: Comprehensive feature comparison across all Python SDK options
sdk_types: ["high-level", "generated", "lakefs-spec"]
difficulty: "intermediate"
use_cases: ["general", "decision-making"]
---

# API Comparison

This comprehensive comparison helps you choose the right Python SDK for your specific use case by comparing features, performance characteristics, and trade-offs across all available options.

## Quick Decision Matrix

| Use Case | Recommended SDK | Alternative |
|----------|----------------|-------------|
| **Data Science & Analytics** | lakefs-spec | High-Level SDK |
| **Production ETL Pipelines** | High-Level SDK | Generated SDK |
| **Custom API Operations** | Generated SDK | High-Level SDK |
| **Jupyter Notebooks** | lakefs-spec | High-Level SDK |
| **ML Experiment Tracking** | High-Level SDK | lakefs-spec |
| **Large File Processing** | lakefs-spec | High-Level SDK |
| **Microservices Integration** | Generated SDK | High-Level SDK |

## Feature Comparison Matrix

### Core Repository Operations

| Feature | High-Level SDK | Generated SDK | lakefs-spec |
|---------|----------------|---------------|-------------|
| **Repository Management** |
| Create Repository | ✅ Full | ✅ Full | ❌ None |
| Delete Repository | ✅ Full | ✅ Full | ❌ None |
| List Repositories | ✅ Full | ✅ Full | ❌ None |
| Repository Metadata | ✅ Full | ✅ Full | ❌ None |
| **Branch Operations** |
| Create Branch | ✅ Full | ✅ Full | ✅ Limited |
| Delete Branch | ✅ Full | ✅ Full | ✅ Limited |
| List Branches | ✅ Full | ✅ Full | ✅ Limited |
| Branch Protection | ✅ Full | ✅ Full | ❌ None |
| **Commit Operations** |
| Create Commit | ✅ Full | ✅ Full | ✅ Full |
| List Commits | ✅ Full | ✅ Full | ✅ Limited |
| Commit Metadata | ✅ Full | ✅ Full | ✅ Limited |
| Cherry Pick | ✅ Full | ✅ Full | ❌ None |

### Object Operations

| Feature | High-Level SDK | Generated SDK | lakefs-spec |
|---------|----------------|---------------|-------------|
| **Basic Operations** |
| Upload Object | ✅ Full | ✅ Full | ✅ Full |
| Download Object | ✅ Full | ✅ Full | ✅ Full |
| Delete Object | ✅ Full | ✅ Full | ✅ Full |
| List Objects | ✅ Full | ✅ Full | ✅ Full |
| **Advanced Operations** |
| Streaming I/O | ✅ Full | 🔶 Manual | ✅ Full |
| Batch Operations | ✅ Full | 🔶 Manual | ✅ Full |
| Object Metadata | ✅ Full | ✅ Full | ✅ Full |
| Presigned URLs | ✅ Full | ✅ Full | ❌ None |
| Multipart Upload | ✅ Full | ✅ Full | ✅ Full |

### Data Management Features

| Feature | High-Level SDK | Generated SDK | lakefs-spec |
|---------|----------------|---------------|-------------|
| **Transactions** |
| Atomic Operations | ✅ Full | 🔶 Manual | ✅ Full |
| Rollback Support | ✅ Full | 🔶 Manual | ✅ Full |
| Context Managers | ✅ Full | ❌ None | ✅ Full |
| **Import/Export** |
| Data Import | ✅ Full | ✅ Full | ❌ None |
| Import Status | ✅ Full | ✅ Full | ❌ None |
| Export Operations | ✅ Full | ✅ Full | ❌ None |
| **Merge Operations** |
| Branch Merging | ✅ Full | ✅ Full | ❌ None |
| Conflict Resolution | ✅ Full | ✅ Full | ❌ None |
| Merge Strategies | ✅ Full | ✅ Full | ❌ None |

### Integration Capabilities

| Feature | High-Level SDK | Generated SDK | lakefs-spec |
|---------|----------------|---------------|-------------|
| **Data Science Libraries** |
| Pandas Integration | ✅ Full | 🔶 Manual | ✅ Native |
| Dask Integration | ✅ Full | 🔶 Manual | ✅ Native |
| PyArrow Integration | ✅ Full | 🔶 Manual | ✅ Native |
| **File System Interface** |
| fsspec Compatibility | 🔶 Limited | ❌ None | ✅ Native |
| Path-like Operations | ✅ Full | 🔶 Manual | ✅ Native |
| Glob Patterns | ✅ Full | 🔶 Manual | ✅ Native |

## Performance Characteristics

### Throughput Comparison

| Operation Type | High-Level SDK | Generated SDK | lakefs-spec |
|----------------|----------------|---------------|-------------|
| **Small Files (< 1MB)** |
| Single Upload | Good | Good | Excellent |
| Batch Upload | Excellent | Good | Excellent |
| Single Download | Good | Good | Excellent |
| Batch Download | Excellent | Good | Excellent |
| **Large Files (> 100MB)** |
| Streaming Upload | Excellent | Good | Excellent |
| Streaming Download | Excellent | Good | Excellent |
| Multipart Upload | Excellent | Good | Excellent |
| **Metadata Operations** |
| List Objects | Good | Good | Excellent |
| Object Stats | Good | Good | Excellent |
| Branch Operations | Excellent | Good | Good |

### Memory Usage

| SDK | Memory Efficiency | Notes |
|-----|------------------|-------|
| **High-Level SDK** | Good | Optimized for common patterns, connection pooling |
| **Generated SDK** | Fair | Direct API access, manual optimization needed |
| **lakefs-spec** | Excellent | Designed for large datasets, streaming-first |

### Latency Characteristics

| Operation | High-Level SDK | Generated SDK | lakefs-spec |
|-----------|----------------|---------------|-------------|
| **Connection Setup** | Fast | Fast | Fast |
| **Authentication** | Fast | Fast | Fast |
| **First Request** | Medium | Medium | Fast |
| **Subsequent Requests** | Fast | Fast | Fast |
| **Batch Operations** | Fast | Medium | Fast |

## Trade-offs Analysis

### High-Level SDK

**Strengths:**
- Comprehensive feature set with advanced capabilities
- Built-in transaction support and error handling
- Optimized for common lakeFS workflows
- Excellent documentation and examples
- Connection pooling and performance optimizations

**Weaknesses:**
- Additional abstraction layer may hide some API details
- Larger dependency footprint
- May not expose all Generated SDK capabilities immediately

**Best For:**
- Production applications requiring robust error handling
- Complex workflows with transactions
- Teams wanting comprehensive lakeFS integration
- Applications requiring advanced features like imports/exports

### Generated SDK

**Strengths:**
- Direct access to all lakeFS API capabilities
- Minimal abstraction, maximum control
- Automatically updated with API changes
- Smaller dependency footprint
- Full async support where available

**Weaknesses:**
- Requires more boilerplate code
- Manual error handling and retry logic
- No built-in transaction support
- Less optimized for common patterns

**Best For:**
- Custom integrations requiring specific API access
- Microservices with minimal dependencies
- Applications needing fine-grained control
- Integration with existing API client patterns

### lakefs-spec

**Strengths:**
- Native fsspec integration for data science workflows
- Excellent performance for file operations
- Seamless integration with pandas, dask, and other libraries
- Optimized for large dataset operations
- Familiar filesystem interface

**Weaknesses:**
- Limited repository management capabilities
- No direct access to advanced lakeFS features
- Focused primarily on file operations
- Third-party maintenance dependency

**Best For:**
- Data science and analytics workflows
- Jupyter notebook environments
- Large dataset processing
- Integration with existing fsspec-based tools
- Teams familiar with filesystem interfaces



## Decision Guidelines

### Choose High-Level SDK When:

- Building production applications with complex lakeFS workflows
- Need transaction support and advanced error handling
- Want comprehensive feature access with minimal code
- Team prefers high-level abstractions
- Building ETL pipelines or data management systems

```python
# Example: Complex workflow with transactions
import lakefs

client = lakefs.Client()
repo = client.repository("my-repo")

with repo.branch("feature").transaction() as tx:
    # Multiple operations in atomic transaction
    tx.upload("data/file1.csv", data1)
    tx.upload("data/file2.csv", data2)
    # Automatically commits or rolls back
```

### Choose Generated SDK When:

- Need access to specific API endpoints not covered by High-Level SDK
- Building microservices with minimal dependencies
- Require fine-grained control over API interactions
- Integrating with existing API client patterns
- Need async support for specific operations

```python
# Example: Direct API access for custom operations
from lakefs_sdk import LakeFSApi, Configuration

config = Configuration(host="http://localhost:8000")
api = LakeFSApi(config)

# Direct API call with full control
response = api.list_repositories(
    prefix="project-",
    amount=100,
    after="cursor"
)
```

### Choose lakefs-spec When:

- Working primarily with data science libraries
- Processing large datasets with streaming requirements
- Using Jupyter notebooks for analysis
- Need filesystem-like interface
- Integrating with existing fsspec-based workflows

```python
# Example: Data science workflow
import pandas as pd
import lakefs_spec

# Direct pandas integration
df = pd.read_parquet("lakefs://repo/branch/data/dataset.parquet")
processed_df = df.groupby("category").sum()
processed_df.to_parquet("lakefs://repo/branch/results/summary.parquet")
```



## Migration Paths

### From File Systems to lakeFS

1. **Start with lakefs-spec**: Familiar filesystem interface
2. **Add High-Level SDK**: For repository management and advanced features
3. **Consider Generated SDK**: For custom integrations and specific API needs

### Between lakeFS SDKs

- **Generated → High-Level**: Gradual migration, can access Generated SDK through High-Level
- **High-Level → Generated**: For specific API access, use `client.sdk` property
- **Any SDK → lakefs-spec**: For data science workflows, can run in parallel

## See Also

**SDK Selection and Setup:**
- [Python SDK Overview](../index.md) - Complete SDK overview and selection guide
- [SDK Decision Matrix](../index.md#sdk-selection-decision-matrix) - Interactive decision guide
- [Getting Started Guide](../getting-started.md) - Installation and setup for all SDKs
- [Authentication Methods](../getting-started.md#authentication-and-configuration) - Credential configuration

**SDK-Specific Documentation:**
- [High-Level SDK Overview](../high-level-sdk/index.md) - Detailed High-Level SDK documentation
- [High-Level SDK Quickstart](../high-level-sdk/quickstart.md) - Basic operations and examples
- [Generated SDK Overview](../generated-sdk/index.md) - Direct API access patterns
- [Generated SDK Examples](../generated-sdk/examples.md) - Common usage patterns
- [lakefs-spec Overview](../lakefs-spec/index.md) - Filesystem interface documentation
- [lakefs-spec Integrations](../lakefs-spec/integrations.md) - Data science library examples

**Feature-Specific Guides:**
- [Transaction Patterns](../high-level-sdk/transactions.md) - Atomic operations across SDKs
- [Object I/O Operations](../high-level-sdk/objects-and-io.md) - File handling patterns
- [Data Import/Export](../high-level-sdk/imports-and-exports.md) - Bulk data operations
- [Filesystem Operations](../lakefs-spec/filesystem-api.md) - File-like operations

**Learning Resources:**
- [Data Science Tutorial](../tutorials/data-science-workflow.md) - End-to-end workflow examples
- [ETL Pipeline Tutorial](../tutorials/etl-pipeline.md) - Building data pipelines
- [ML Experiment Tracking](../tutorials/ml-experiment-tracking.md) - Model versioning workflows

**Reference Materials:**
- [Best Practices](best-practices.md) - Production deployment guidelines
- [Performance Optimization](best-practices.md#performance) - SDK performance tuning
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Error Handling Patterns](troubleshooting.md#error-handling) - Exception handling strategies

**Migration Guides:**
- [SDK Migration Strategies](best-practices.md#sdk-migration) - Moving between SDKs
- [Legacy Integration](best-practices.md#legacy-integration) - Integrate with existing systems

**External Resources:**
- [High-Level SDK API Reference](https://pydocs-lakefs.lakefs.io){:target="_blank"} - Complete API documentation
- [Generated SDK API Reference](https://pydocs-sdk.lakefs.io){:target="_blank"} - Auto-generated API docs
- [lakefs-spec Documentation](https://lakefs-spec.org/){:target="_blank"} - Third-party filesystem interface