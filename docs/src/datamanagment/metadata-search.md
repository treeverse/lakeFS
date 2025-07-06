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

Metadata is exposed as versioned Iceberg tables, fully compatible with clients like DuckDB, Spark, Trino, and others - 
enabling fast, expressive queries across any lakeFS version. See [How It Works](#how-it-works) for details.

## Benefits

* **Scalable and Fast**: Search metadata across millions or billions of objects keeping fast query response time.
* **Query Reproducibility**: Run metadata queries against specific commits or tags for consistent, versioned results.
* **No infrastructure burden**: lakeFS manages metadata collection and indexing natively; no need to build, deploy or 
maintain a separate metadata system.

## Use cases 

* **Data Discovery & Exploration**: Quickly find relevant data using flexible filters (e.g., annotations, object size, timestamps).
* **Data Governance**: Audit metadata tags, detect sensitive data (like PII), and ensure objects are properly labeled with 
ownership or classification to support internal policies and external compliance requirements.
* **Operational Troubleshooting**: Filter and inspect data using metadata like workflow ID or publish time to trace lineage, 
debug pipeline issues, and understand how data was created or modified - all within a specific lakeFS version. 

## How it Works

lakeFS Metadata Search creates lakeFS managed Iceberg tables, using lakeFS Iceberg REST catalog. 


### Metadata Table Schema 

| In lakeFS          | required              | Data Type          |
|--------------------|-----------------------|--------------------|
| repository         | yes                   | sting              |
| path               | yes                   | string             |
| commit_id          | yes                   | string             |
| size               | yes                   | Long               |
| last_modified_date | yes                   | Timestamp          |
| etag               | ye s                  | string             |
| user_metadata      | no                    | Map<string, string>|
| committer          | yes                   | string             |
| content_type       | no                    | string             |
| creation_date      | yes                   | Timestamp          |

Can I use lakeFS metadata search even if i'm not using lakeFS' Iceberg REST catalog? 
Yes. 

* Keep a proportional size to snapshot size


### Consistency Model


## Getting Started

## Configuring Metadata Search

### Understanding References

How to use branches, commit IDs, and tags in metadata queries

## Writing Queries

by what query engines...

Describe how to query via SQL or Python (e.g., DuckDB), what table names to use, and structure of queries.

### Example Queries


## Limitations

* No direct tags support, see understanding refrences for more details.
