---
title: Python
description: Use Python to interact with lakeFS
---

# Use Python to interact with lakeFS

!!! warning
    If your project is currently using the [legacy Python `lakefs-client`][legacy-pypi], please be aware that this version has been [deprecated][legacy-deprecated].
    As of release **v1.44.0**, it's no longer supported for new updates or features.

## Getting Started

New to lakeFS Python SDK? Start with the **[High-Level SDK Guide](./python-getting-started.md)** to install and configure the recommended Python package.

The High-Level SDK provides an intuitive interface for:

- [Branches & Merging](./python-versioning-branches.md) - Feature branch workflows
- [References, Commits & Tags](./python-refs.md) - References, Commits & Tags
- [Transactions](./python-transactions.md) - Atomic operations
- [Data Operations](./python-data-operations.md) - Batch operations and cleanup

## Integration Options

lakeFS provides multiple Python packages to suit different use cases and preferences:

| Package | Abstraction | Best For | Installation | Learning Curve |
|---------|-------------|----------|--------------|-----------------|
| **[High-Level SDK](./python-getting-started.md)** | High | Versioning operations, data operations, transactions | `pip install lakefs` | Low |
| **[Generated SDK](./python-sdk.md)** | Low | Direct API access, full API surface, programmatic control | `pip install lakefs-sdk` | Medium |
| **[lakefs-spec](./python-lakefs-spec.md)** | High | File system operations, pandas/data science integration, S3-like interface | `pip install lakefs-spec` | Low |
| **[Boto / S3 Gateway](./python-boto.md)** | Medium | S3-compatible operations, existing S3 workflows, direct gateway access | `pip install boto3` | Low |

## References & Resources

- **High Level Python SDK Documentation**: [https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)
- **Generated Python SDK Documentation**: [https://pydocs-sdk.lakefs.io](https://pydocs-sdk.lakefs.io)
- **lakefs-spec Project**: [https://lakefs-spec.org](https://lakefs-spec.org)
- **Boto S3 Router**: [https://github.com/treeverse/boto-s3-router](https://github.com/treeverse/boto-s3-router)

[legacy-deprecated]:  ../posts/deprecate-py-legacy.md
[legacy-pypi]:  https://pypi.org/project/lakefs-client/
