---
title: Boto3 Integration
description: Comprehensive guide for using Boto3 with lakeFS for S3-compatible operations
sdk_types: ["boto3"]
difficulty: "beginner"
use_cases: ["s3-migration", "legacy-integration", "hybrid-workflows"]
---

# Boto3 Integration

Boto3 provides S3-compatible access to lakeFS, enabling seamless migration of existing S3-based applications with minimal code changes. This integration allows you to leverage lakeFS's versioning and branching capabilities while maintaining familiar S3 operations.

## Overview

lakeFS provides full S3 API compatibility through its S3 Gateway, allowing you to use Boto3 with minimal configuration changes. This approach is ideal for:

- **Existing S3 Applications**: Migrate applications with minimal code changes
- **Legacy Systems**: Integrate lakeFS into established workflows
- **Team Familiarity**: Leverage existing S3/Boto3 expertise
- **Gradual Migration**: Incrementally adopt lakeFS features

### How It Works

lakeFS repositories appear as S3 buckets, and branches/commits are represented in the object key path:

```
s3://my-repo/main/path/to/file.txt        # main branch
s3://my-repo/feature-branch/path/to/file.txt  # feature branch
s3://my-repo/c1a2b3c4d5e6f7g8/path/to/file.txt  # specific commit
```

## Key Features

### S3 API Compatibility
- **Complete S3 Operations** - PUT, GET, DELETE, LIST, HEAD operations
- **Multipart Uploads** - Support for large file uploads
- **Presigned URLs** - Generate temporary access URLs
- **Object Metadata** - Custom metadata and tagging support
- **Bucket Operations** - List repositories as buckets

### lakeFS Integration Benefits
- **Version Control** - Every change is versioned and tracked
- **Branching** - Create isolated development environments
- **Atomic Operations** - Commit multiple changes atomically
- **Rollback Capability** - Easily revert to previous states
- **Audit Trail** - Complete history of all changes

### Migration Advantages
- **Minimal Code Changes** - Usually just endpoint URL modification
- **Gradual Adoption** - Migrate services one at a time
- **Risk Reduction** - Test changes in isolated branches
- **Backward Compatibility** - Existing S3 tools continue to work

## When to Use Boto3 with lakeFS

### Ideal Use Cases
- **S3 Migration** - Moving existing S3-based applications to lakeFS
- **Legacy Integration** - Adding version control to existing systems
- **Data Pipeline Migration** - Converting ETL workflows to use lakeFS
- **Multi-Cloud Strategy** - Standardizing on S3 API across providers

### Consider Alternatives When
- **New Development** - [High-Level SDK](../high-level-sdk/) offers more features
- **Advanced Features** - Need transactions, streaming, or advanced operations
- **Performance Critical** - Direct API access may be more efficient
- **Complex Workflows** - [lakefs-spec](../lakefs-spec/) better for data science

## Documentation Sections

- **[Configuration](configuration.md)** - Setup and configuration options
- **[S3 Operations](s3-operations.md)** - S3-compatible operations with lakeFS
- **[S3 Router](s3-router.md)** - Hybrid S3/lakeFS routing

## Quick Example

```python
import boto3

# Configure Boto3 client for lakeFS
s3 = boto3.client('s3',
    endpoint_url='http://localhost:8000',
    aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)

# Use standard S3 operations
s3.put_object(
    Bucket='my-repo',
    Key='main/data/file.txt',
    Body=b'Hello, lakeFS!'
)

# List objects
response = s3.list_objects_v2(Bucket='my-repo', Prefix='main/')
for obj in response.get('Contents', []):
    print(obj['Key'])
```

## Installation

```bash
pip install boto3
```

## Next Steps

- Start with [configuration setup](configuration.md)
- Learn about [S3 operations](s3-operations.md)
- Explore [S3 Router](s3-router.md) for hybrid workflows

## See Also

**SDK Selection and Comparison:**
- [Python SDK Overview](../index.md) - Compare all Python SDK options
- [SDK Decision Matrix](../index.md#sdk-selection-decision-matrix) - Choose the right SDK for your use case
- [API Feature Comparison](../reference/api-comparison.md) - Detailed feature comparison across SDKs

**Boto3 Integration Documentation:**
- [Configuration Guide](configuration.md) - Complete setup and authentication options
- [S3 Operations](s3-operations.md) - S3-compatible operations with lakeFS
- [S3 Router](s3-router.md) - Hybrid S3/lakeFS routing for gradual migration
- [Troubleshooting](troubleshooting.md) - Common issues and solutions

**Migration and Integration:**
- [S3 Migration Patterns](s3-operations.md#migration-patterns) - Convert existing S3 code
- [Hybrid Workflows](s3-router.md#hybrid-configurations) - Combine S3 and lakeFS
- [Legacy System Integration](../reference/best-practices.md#legacy-integration) - Integration strategies

**Alternative SDK Options:**
- [High-Level SDK](../high-level-sdk/index.md) - More features for new development
- [High-Level SDK Quickstart](../high-level-sdk/quickstart.md) - Object-oriented interface
- [Generated SDK](../generated-sdk/index.md) - Direct API access for custom operations
- [lakefs-spec](../lakefs-spec/index.md) - Filesystem interface for data science

**Setup and Configuration:**
- [Installation Guide](../getting-started.md) - Complete setup instructions for all SDKs
- [Authentication Methods](../getting-started.md#authentication-and-configuration) - All credential configuration options
- [Best Practices](../reference/best-practices.md#boto3-configuration) - Production configuration guidance

**Learning Resources:**
- [ETL Pipeline Tutorial](../tutorials/etl-pipeline.md) - Building data pipelines with S3 operations
- [Data Migration Examples](s3-operations.md#data-migration) - Real-world migration scenarios
- [Batch Processing Patterns](../tutorials/etl-pipeline.md#batch-processing) - Large-scale data operations

**Reference Materials:**
- [S3 API Compatibility](s3-operations.md#api-compatibility) - Supported S3 operations
- [Error Handling](../reference/troubleshooting.md#boto3-issues) - Common issues and solutions
- [Performance Optimization](../reference/best-practices.md#boto3-performance) - Optimize S3 operations

**External Resources:**
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html){:target="_blank"} - Official Boto3 documentation
- [AWS S3 API Reference](https://docs.aws.amazon.com/s3/latest/API/){:target="_blank"} - S3 API specification
- [lakeFS S3 Gateway](https://docs.lakefs.io/integrations/aws_cli.html){:target="_blank"} - lakeFS S3 compatibility documentation
- [S3 Migration Best Practices](https://aws.amazon.com/s3/migration/){:target="_blank"} - AWS migration guidance