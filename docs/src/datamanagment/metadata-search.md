---
title: Metadata Search
description: Search objects using versioned object metadata
status: enterprise
---

# Metadata Search

!!! info
    Available in **lakeFS Enterprise**

!!! tip
    lakeFS Metadata search is currently in private preview for [lakeFS Enterprise](../enterprise/index.md) customers.
    [Contact us](https://lakefs.io/lp/iceberg-rest-catalog/) to get started!

## Overview

lakeFS Metadata Search makes large-scale data lakes easily searchable by object metadata, while adding the power of 
versioning to search. This enables reproducible queries which are essential in collaborative and ML-driven environments
where data evolves constantly and metadata is key to making informed decisions. 

With lakeFS Metadata Search, you can query both:
* **System metadata**: Automatically captured properties such as object path, size, last modified time, and committer.
* **User-defined metadata**: Custom labels, annotations, or tags stored as lakeFS object metadata — typically added during ingestion, processing, or curation.

![metadata search](../assets/img/mds/mds_how_it_works.png)

To enable simple and scalable search, lakeFS exposes object metadata as versioned Iceberg tables, fully compatible with
clients like DuckDB, PyIceberg, Spark, Trino, and others - enabling fast, expressive queries across any lakeFS version. 
See [How It Works](#how-it-works) for details. 

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

For every searchable repository and branch, lakeFS creates an Iceberg **metadata table** at: `<repo>-metadata.<branch>.system.object_metadata`. 
These tables are fully managed by lakeFS and optimized for metadata search.

Metadata is versioned, allowing you to query by branch names and immutable references like commit IDs and tags. 
(See [Understanding References](#understanding-references) for more on querying by different reference types.)

!!! info
    You can use Metadata Search even if you’re not licensed for full lakeFS Iceberg support.
    If you're already using another Iceberg REST catalog, you don’t need to switch — metadata search will still work using 
    the lakeFS-managed catalog, which operates independently.

### Metadata Table Schema

Each row in the lakeFS metadata table represents the latest metadata for an object on the branch the table corresponds to.
lakeFS Metadata Search runs a background processing pipeline that keeps these tables **eventually consistent** with changes 
made to the branch.

Because metadata tables are versioned by lakeFS, they reflect the current state of the branch. Each table includes 
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

!!! info
    lakeFS object metadata tables are eventually consistent, which means it may take up to a few minutes for newly committed 
    objects to become searchable. Metadata becomes searchable **atomically** — either all object metadata from the commit is available, or none of it
    is. There is no partial availability.
    To check whether the most recent state of your repository is available for metadata search,
    run the following query:
    ```sql
    SELECT * from <repo>-metadata.<branch>.system.object_metadata
    WHERE commit_id = <head_commit> -- Replace with the head commit ID
    LIMIT 1;
    ```
    If the query returns results, you can safely search using the latest metadata.

## Getting Started

This section assumes that you are already using lakeFS object metadata. Not already using it? see... for quickly get started, 
make your metadata manageable and enrich you data context. 

## Configuring Metadata Search

### Understanding References

TODO
How to use branches, commit IDs, and tags in metadata queries

## Writing Queries

You can write metadata queries using any engine that supports Iceberg tables, including DuckDB, Trino, Spark, and PyIceberg.
Query performance depends on both the engine used (e.g., Trino typically outperforms PyIceberg) and the size of the lakeFS
branch being queried.

TODO
See the Iceberg docs for how to query using duckDB (because it is different)  

### Example Queries

TODO

#### Filter by Object Annotation

#### Filter by Object Annotation on past commit

#### View Metadata from AI-Powered Annotators

#### Filter by File Extension & Size

#### Detect Errors in Sensitive Data Tagging

Assume all objects under a specific path are expected to have PII=true. To identify tagging errors, query for objects in
that path where PII=false or where the PII annotation is missing.

#### Filter objects by addition Time

Find all objects added to a branch after certain timeframe

## Limitations

* Applies to new or changed objects only: lakeFS metadata tables do not include objects that existed in your repository 
before the feature was enabled. Only objects added or modified after metadata search was turned on will be indexed.
* No direct commit and tag support: Metadata tables do not natively support referencing commits and tags. To query by 
commit or tag, see the [Understanding References](#understanding-references) section for the recommended approach.
