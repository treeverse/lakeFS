---
title: Python
description: Use Python to interact with your objects on lakeFS
---

# Use Python to interact with your objects on lakeFS

!!! warning
    If your project is currently using the [legacy Python `lakefs-client`][legacy-pypi], please be aware that this version has been [deprecated][legacy-deprecated].
    As of release **v1.44.0**, it's no longer supported for new updates or features.

## Overview

lakeFS provides multiple Python packages and integrations to suit different use cases and preferences. Choose the integration that best fits your workflow:

| Package | Abstraction | Best For | Installation | Learning Curve |
|---------|-------------|----------|--------------|-----------------|
| **High-Level SDK** | High | Versioning operations, branch management, commits, tags, transactions | `pip install lakefs` | Low |
| **Generated SDK** | Low | Direct API access, full API surface, programmatic control | `pip install lakefs-sdk` | Medium |
| **lakefs-spec** | High | File system operations, pandas/data science integration, S3-like interface | `pip install lakefs-spec` | Low |
| **Boto3** | Medium | S3-compatible operations, existing S3 workflows, direct gateway access | `pip install boto3` | Low |

## Quick Start

New to lakeFS Python SDK? Start with the **[Getting Started Guide](./python-getting-started.md)** to install and configure the High-Level SDK.

## Python Integration Options

Choose the integration that best fits your needs:

- **[High-Level SDK](./python-getting-started.md)** - Recommended for most use cases
    - [Branches & Merging](./python-versioning-branches.md) - Feature branch workflows
    - [Tags & Snapshots](./python-versioning-tags.md) - Version management and releases
    - [References & Commits](./python-references-commits.md) - Commit history and lineage
    - [Transactions](./python-transactions.md) - Atomic operations
    - [Data Operations](./python-data-operations.md) - Batch operations and cleanup

- **[Generated SDK](./python-sdk.md)** - For direct API access based on OpenAPI specification

- **[lakefs-spec](./python-lakefs-spec.md)** - For file system operations and data science integration

- **[Boto / S3 Gateway](./python-boto.md)** - For S3-compatible operations

## References & Resources

- **High Level Python SDK Documentation**: [https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)
- **Generated Python SDK Documentation**: [https://pydocs-sdk.lakefs.io](https://pydocs-sdk.lakefs.io)
- **lakefs-spec Project**: [https://lakefs-spec.org](https://lakefs-spec.org)
- **Boto S3 Router**: [https://github.com/treeverse/boto-s3-router](https://github.com/treeverse/boto-s3-router)

[legacy-deprecated]:  ../posts/deprecate-py-legacy.md
[legacy-pypi]:  https://pypi.org/project/lakefs-client/
