---
title: Metadata Search
description: Search objects using versioned object metadata
status: enterprise
---

# Metadata Search

!!! info
    Available in **lakeFS Enterprise**

!!! tip
    lakeFS Iceberg REST Catalog is currently in private preview for [lakeFS Enterprise](../enterprise/index.md) customers.
    [Contact us](https://lakefs.io/lp/iceberg-rest-catalog/) to get started!

## Overview

lakeFS Metadata Search makes large-scale data lakes easily searchable by object metadata, while adding the power of 
versioning to search. This enables reproducible queries which are essential in collaborative and ML-driven environments
where data evolves constantly and metadata is key to making informed decisions. 

With lakeFS Metadata Search, you can query both:
* **System metadata**: Automatically captured properties such as object path, size, last modified time, and committer.
* **User-defined metadata**: Custom annotations, labels, and operational context - often added during ingestion, processing, or curation.

![metadata search](../assets/img/mds/mds_how_it_works.png)

Metadata is exposed as versioned Iceberg tables, fully compatible with clients like DuckDB, PyIceberg, Spark, Trino, and
others - enabling fast, expressive queries across any lakeFS version. See [How It Works](#how-it-works) for details.

## Benefits

* **Scalable and Fast**: Search metadata across millions or billions of objects keeping fast query response time.
* **Query Reproducibility**: Run metadata queries against specific commits or tags for consistent results.
* **No infrastructure burden**: lakeFS manages metadata collection and indexing natively: no need to build, deploy or 
maintain a separate metadata tracking system.

## Use cases 

* **Data Discovery & Exploration**: Quickly find relevant data using flexible filters (e.g., annotations, object size, timestamps).
* **Data Governance**: Audit metadata tags, detect sensitive data (like PII), and ensure objects are properly labeled with 
ownership or classification to support internal policies and external compliance requirements.
* **Operational Troubleshooting**: Filter and inspect data using metadata like workflow ID or publish time to trace lineage, 
debug pipeline issues, and understand how data was created or modified - all within a specific lakeFS version. 

## How it Works

lakeFS Metadata Search is built on top of [lakeFS Iceberg support](../integrations/iceberg.md#what-is-lakefs-iceberg-rest-catalog),
using internal system tables to manage and expose object metadata.

For every searchable repository and branch, lakeFS creates an Iceberg **metadata table** at: `<repo>.<branch>.system.object_metadata`. 
These tables are fully managed by lakeFS and optimized for metadata search.

Metadata is versioned, allowing you to query by branch names and immutable references like commit IDs and tags. 
(See [Understanding References](#understanding-references) for more on querying by different reference types.)

!!! note
    You can use Metadata Search even if you’re not licensed for full lakeFS Iceberg support.
    If you're already using another Iceberg REST catalog, you don’t need to switch — metadata search will still work using 
    the lakeFS-managed catalog, which operates independently.

### Metadata Table Schema

Each row in the lakeFS metadata table represents the latest metadata for an object on the branch the table corresponds to.
lakeFS Metadata Search runs a background processing pipeline that keeps these tables eventually consistent with changes 
made to the branch.

Because metadata tables are versioned by lakeFS, they always reflect the current state of the branch. Each table includes 
at most one row per object, ensuring that the number of records matches the number of objects present on that branch, 
keeping performance consistent and predictable. 

| Column name        | Required| Data Type          | Description                                                                                                                                          |
|--------------------|---------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| repository         | yes     | sting              | The name of the repository where the object is stored.                                                                                               |
| path               | yes     | string             | The unique path identifying the object within the repository.                                                                                        | 
| commit_id          | yes     | string             | The **latest commit ID** where the object was added or modified.                                                                                     |
| size               | yes     | Long               | The object's size in bytes.                                                                                                                          |
| last_modified_date | yes     | Timestamp          | The time the object was last modified.                                                                                                                   |
| etag               | yes     | string             | The object’s ETag (content hash). This reflects changes to the object's content only, not its metadata.                   |
| user_metadata      | no      | Map<string, string>| User-defined metadata (e.g., annotations, tags). If none exists, an empty map is stored. |
| committer          | yes     | string             | The user who committed the object’s latest change.                                                                                                     |
| content_type       | no      | string             | The MIME type of the object (e.g., `application/json`, `image/png`).                                                                                                                                                     |
| creation_date      | yes     | Timestamp          | The original creation timestamp of the object in the repository.                                                                                                                             |

## Getting Started

## Configuring Metadata Search

### Understanding References

How to use branches, commit IDs, and tags in metadata queries

## Writing Queries

by what query engines...

Describe how to query via SQL or Python (e.g., DuckDB), what table names to use, and structure of queries.

### Example Queries


## Limitations

* lakeFS metadata tables doesn't apply to any objects that already existed in your repository before enabling the feature. 
  It only captures metadata of objects changed after you have enabled the feature.
* No direct tags support, see understanding refrences for more details.
