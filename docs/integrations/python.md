---
title: Python Integration
description: Use Python to interact with your data lake and manage data versions with lakeFS
parent: Integrations
redirect_from:
  - /using/python.html
  - /using/boto.html
  - /integrations/boto.html
---

# Using Python with lakeFS

lakeFS offers several ways to integrate Python into your data workflows, enabling you to manage data versions, perform object operations, and automate your data pipelines. Whether you prefer a high-level abstracted SDK, direct API access, a file-system like interface, or compatibility with existing S3 tools, there's a Python solution for you.

{% raw %}{% include toc.html %}{% endraw %}

{: .warning }
> If your project is currently using the [legacy Python `lakefs-client`][legacy-pypi], please be aware that this version has been [deprecated][legacy-deprecated].
> As of release v1.44.0, it's no longer supported for new updates or features. We strongly recommend migrating to the new [High-Level Python SDK](./python/high_level_sdk.md).

## Choosing the Right Python Tool

Here's a brief overview of the primary Python tools for interacting with lakeFS:

*   **[High-Level Python SDK (`lakefs`)](./python/high_level_sdk.md)**: <span class="badge mr-1">Recommended</span>
    The primary SDK for most use cases. It provides an intuitive, Pythonic interface for common lakeFS operations like repository and branch management, object I/O (upload, download, read, write), commits, merges, and transactions. This SDK is actively developed and includes features designed to simplify your interaction with lakeFS.

*   **[Generated Python SDK (`lakefs_sdk`)](./python/generated_sdk.md)**:
    For users who need direct access to the complete lakeFS REST API. This SDK is auto-generated from the lakeFS OpenAPI specification, ensuring comprehensive coverage of API endpoints. It's suitable for advanced use cases or when you need to access newer API features not yet exposed in the High-Level SDK.

*   **[lakefs-spec (`fsspec` compatible)](./python/lakefs_spec.md)**:
    If you're working with libraries that utilize the `fsspec` (filesystem-spec) interface (like Pandas, Dask, etc.), `lakefs-spec` allows you to interact with lakeFS as if it were a regular file system. This is excellent for integrating lakeFS into existing data science and processing workflows. (Note: This is a third-party library).

*   **[Boto3 (S3 Gateway)](./python/boto3.md)**:
    Leverage the lakeFS S3 gateway to use Boto3 (the AWS SDK for Python) for object operations. This is useful if you have existing scripts or tools that use Boto3 for S3 and you want to adapt them to work with lakeFS.

*   **[Best Practices & Performance Guide](./python/best_practices.md)**:
    Consult this guide for tips on choosing the right tool, optimizing performance, and understanding common patterns when using Python with lakeFS.

## Quick Links to SDK Documentation

- **High-Level Python SDK (`lakefs`) Docs**: [https://pydocs-lakefs.lakefs.io](https://pydocs-lakefs.lakefs.io)
- **Generated Python SDK (`lakefs_sdk`) Docs**: [https://pydocs-sdk.lakefs.io](https://pydocs-sdk.lakefs.io)
- **lakefs-spec API Reference**: [https://lakefs-spec.org/latest/reference/lakefs_spec/](https://lakefs-spec.org/latest/reference/lakefs_spec/)

Explore the dedicated pages linked above for detailed installation instructions, usage examples, and API references for each tool.

## Common Use Cases with Python SDKs

The lakeFS Python SDKs can be used to address a wide variety of data management challenges. Here are a few common scenarios:

### 1. Automated Data Ingestion and Versioning

Automate your ETL/ELT pipelines to ingest data into lakeFS and version it consistently. This ensures that every dataset that lands in your data lake is versioned and auditable.

*   **How**:
    *   Use the [High-Level SDK](./python/high_level_sdk.md) to upload new data files (e.g., Parquet, CSV, images) to a development branch.
    *   Perform data quality checks.
    *   Commit the changes with meaningful messages and metadata.
    *   Merge the new data into your main production branch.
    *   **Examples**: See "Object Operations (I/O)", "Committing Changes", and "Merging Branches" sections in the [High-Level Python SDK documentation](./python/high_level_sdk.md). The "Importing Data into lakeFS" section is also relevant for bulk ingestion.

### 2. Managing Data Science Experiments

Isolate your data science experiments by creating separate branches for each. This allows you to work with different versions of datasets, models, and parameters without impacting the main data or other experiments.

*   **How**:
    *   Create a new branch from your main data branch using the [High-Level SDK](./python/high_level_sdk.md).
    *   Within this branch, upload new datasets, process existing data, or save model artifacts.
    *   Commit changes specific to the experiment.
    *   If the experiment is successful, merge the branch back. If not, the branch can be discarded without affecting your main data.
    *   **Examples**: Refer to "Creating a Branch", "Object Operations (I/O)", and "Committing Changes" in the [High-Level Python SDK documentation](./python/high_level_sdk.md). The `lakefs-spec` library can also be useful for integrating with tools like Pandas, as shown in its [dedicated page](./python/lakefs_spec.md).

### 3. CI/CD for Data using Hooks and Python

Implement CI/CD workflows for your data by using lakeFS hooks that trigger Python scripts. This can automate validation, testing, and promotion of data through different stages.

*   **How**:
    *   Define lakeFS action hooks (e.g., pre-commit, pre-merge) that call your Python scripts.
    *   Your Python scripts, using the [High-Level SDK](./python/high_level_sdk.md) or [Generated SDK](./python/generated_sdk.md), can perform actions like:
        *   Validating data schema or quality before a commit is finalized.
        *   Running tests on data before a merge is allowed.
        *   Tagging commits or promoting data to another location upon successful merge.
    *   **Guidance**: While specific hook examples are in the [Hooks documentation](../../howto/hooks/index.md), your Python scripts will leverage SDK functionalities like reading objects, listing changes, or applying tags, found in the SDK guides.

### 4. Reproducibility and Rollback

Access specific, consistent versions of your data for model training, analysis, or to reproduce previous results. If issues arise in production, easily roll back to a known good version of your data.

*   **How**:
    *   Reference data by commit ID or tag when reading it with the [High-Level SDK](./python/high_level_sdk.md) or `lakefs-spec`.
    *   To roll back, you can reset a branch to a specific commit ID or merge an older state back into your main line.
    *   **Examples**: Reading data by referencing a commit ID is an implicit feature when you use a commit ID as the `ref` in `repo.branch("commit_id").object("path/to/file")`. For rollback strategies, see the conceptual guide under [Rollback](../../understand/use_cases/rollback.md) and implement using branch merging or reset operations (which might be more advanced and potentially use the Generated SDK for specific reset APIs if not in high-level yet).

These are just a few examples. The flexibility of the Python SDKs allows for a wide range of automation and integration possibilities with lakeFS.

[legacy-deprecated]:  {{ '/posts/deprecate-py-legacy.html' | relative_url }}
[legacy-pypi]:  https://pypi.org/project/lakefs-client/
---
