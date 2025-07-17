---
title: Python Reference
description: Reference documentation for Python and lakeFS
sdk_types: ["high-level", "generated", "lakefs-spec", "boto3"]
difficulty: "intermediate"
use_cases: ["reference", "comparison", "troubleshooting", "best-practices"]
topics: ["reference", "documentation", "resources"]
audience: ["developers", "data-engineers", "advanced-users"]
last_updated: "2024-01-15"
---

# Python Reference

Reference documentation and resources for Python integration with lakeFS.

## Reference Sections

- **[API Comparison](api-comparison.md)** - Feature comparison across all Python SDKs
- **[Best Practices](best-practices.md)** - Production best practices and guidelines
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions
- **[Changelog](changelog.md)** - SDK changes and updates

## Quick Reference

### SDK Selection Guide
- **High-Level SDK**: Most users, simplified API, transactions
- **Generated SDK**: Direct API access, custom operations
- **lakefs-spec**: Data science, fsspec compatibility
- **Boto3**: S3 migration, existing S3 workflows

### Common Patterns
- Authentication via environment variables or config files
- Use transactions for atomic operations
- Leverage streaming for large files
- Handle errors gracefully with try/catch blocks

## External Resources

- [High-Level SDK Documentation](https://pydocs-lakefs.lakefs.io)
- [Generated SDK Documentation](https://pydocs-sdk.lakefs.io)
- [lakefs-spec Documentation](https://lakefs-spec.org/)
- [Boto S3 Router](https://github.com/treeverse/boto-s3-router)

## See Also

**Getting Started:**
- **[Python SDK Overview](../index.md)** - Complete overview of all Python SDK options
- **[Getting Started Guide](../getting-started.md)** - Installation and setup for all SDKs
- **[SDK Selection Guide](../index.md#sdk-selection-decision-matrix)** - Choose the right SDK

**Reference Documentation:**
- **[API Comparison](api-comparison.md)** - Comprehensive feature comparison across all SDKs
- **[Best Practices](best-practices.md)** - Production deployment and optimization guidance
- **[Troubleshooting](troubleshooting.md)** - Common issues, solutions, and debugging techniques
- **[Changelog](changelog.md)** - SDK updates, changes, and migration notes

**SDK-Specific Documentation:**
- **[High-Level SDK](../high-level-sdk/index.md)** - Comprehensive High-Level SDK documentation
- **[Generated SDK](../generated-sdk/index.md)** - Direct API access patterns and examples
- **[lakefs-spec](../lakefs-spec/index.md)** - Filesystem interface for data science workflows
- **[Boto3 Integration](../boto3/index.md)** - S3-compatible operations and migration

**Learning Resources:**
- **[Quickstart Guide](../high-level-sdk/quickstart.md)** - Basic operations and examples
- **[Tutorial Collection](../tutorials/index.md)** - Real-world examples and workflows
- **[Data Science Tutorial](../tutorials/data-science-workflow.md)** - End-to-end data analysis
- **[ETL Pipeline Tutorial](../tutorials/etl-pipeline.md)** - Production data pipeline patterns

**Feature-Specific Guides:**
- **[Transaction Patterns](../high-level-sdk/transactions.md)** - Atomic operations across SDKs
- **[Object I/O Operations](../high-level-sdk/objects-and-io.md)** - File handling and streaming
- **[Repository Management](../high-level-sdk/repositories.md)** - Repository operations
- **[Branch Operations](../high-level-sdk/branches-and-commits.md)** - Version control patterns

**Integration Guides:**
- **[Data Science Integrations](../lakefs-spec/integrations.md)** - pandas, dask, and other libraries
- **[S3 Migration Patterns](../boto3/s3-operations.md)** - Convert existing S3 workflows
- **[Filesystem Operations](../lakefs-spec/filesystem-api.md)** - File-like operations

**External Resources:**
- **[lakeFS Documentation](https://docs.lakefs.io){:target="_blank"}** - Complete lakeFS documentation
- **[lakeFS API Reference](https://docs.lakefs.io/reference/api.html){:target="_blank"}** - REST API specification
- **[Community Examples](https://github.com/treeverse/lakeFS-samples){:target="_blank"}** - Sample projects and notebooks