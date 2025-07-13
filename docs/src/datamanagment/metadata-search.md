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

lakeFS Metadata Search is built on top of [lakeFS Iceberg support](../integrations/iceberg.md#what-is-lakefs-iceberg-rest-catalog), using internal system tables to manage and expose object metadata.

For every searchable repository and branch, lakeFS creates an Iceberg **object metadata table** at: `<repository_id>-metadata.<branch>.system.object_metadata`. 
These tables are fully managed by lakeFS and optimized for metadata search.

Metadata is versioned, allowing you to query by branch names and immutable references like commit IDs and tags. 
(See [Writing reproducible queries](#writing-reproducible-queries) for more on querying by different reference types.)

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
| repository         | yes     | sting              | The name of the repository where the object is **stored**.                                                                                               |
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

!!! info 
    This section assumes that you are already using lakeFS [object metadata](../understand/glossary.md#object-metadata).

## Configuring Metadata Search

TODO
* Data repo vs. Meatadata Search repo
* metadata server configurations 
* Iceberg catalog configurations

## How to Search by Metadata

To search object metadata in lakeFS, you query the Iceberg metadata tables automatically maintained by lakeFS.

Because these are standard Iceberg tables, you can use any engine that supports Iceberg, including DuckDB, Trino, Spark, or PyIceberg.

!!! tip
    Query performance depends heavily on the engine. For example, Trino typically delivers better performance than PyIceberg.

If you're using DuckDB, see the Iceberg REST Catalog [guide](../integrations/iceberg.md#relative-namespace-support) for 
details on how to reference object_metadata tables.

#### Search Steps

To run a metadata search:
1. Load the lakeFS Iceberg catalog.
2. Load the metadata table you want to query.
3. Use SQL to search by system or user-defined metadata.

Here’s an example using PyIceberg and DuckDB: 
```python
from pyiceberg.catalog import load_catalog
from datetime import datetime, timedelta

one_week_ago = (datetime.now() - timedelta(weeks=1)).isoformat()

catalog = load_catalog(uri='host:port/iceberg/api')

con = catalog.load_table('repo-metadata.main.system.object_metadata').scan().to_duckdb('object_metadata')

query = f"""
SELECT path   
FROM object_metadata
WHERE user_metadata['animal'] = 'cat' 
  AND creation_date > TIMESTAMP '{one_week_ago}' 
"""

df = con.execute(query).df()
```

This query finds all newly added cat images (added in the past week), demonstrating how you can combine user-defined and
system metadata fields in powerful, version-aware searches.

### Writing Reproducible Queries

When you search metadata on a branch, the results reflect the state of the branch’s head commit at the time the query is
run. However, since a branch’s HEAD is mutable, it moves forward as new commits are added. Therefore, queries using branch
names are not reproducible over time.

To make queries reproducible, you must use immutable references, such as lakeFS [commits](../understand/glossary.md#commit)
or [tags](../understand/glossary.md#tag), which always point to a specific snapshot of your data.

Let’s walk through a concrete example:

Assume your main branch has the following commit history: `c0 → c1`

#### Using Branch Names

Querying the table: `repo-metadata.main.system.object_metadata` will return metadata reflecting the current HEAD of
main (in this case, commit `c1`). As new commits are added, the results may change.

#### Using Commit IDs

To query metadata for a specific historical snapshot (e.g., commit `c0`), use: `repo-metadata.commit-c0.system.object_metadata`

!!! info
    When querying by commit, you must prefix the commit ID with `commit-`. You must also use the **full** commit SHA 
    (not a shortened version). This requirement is temporary and will be simplified in future versions.

#### Using Tags

To search metadata associated with a specific tag:
1. Retrieve the commit ID the tag points to.
2. Use the commit-based pattern described [above](#using-commit-ids-).

Example using lakefs and PyIceberg:
```python
import lakefs
from pyiceberg.catalog import load_catalog

# Load the Iceberg catalog
catalog = load_catalog(uri='host:port/iceberg/api')

repo = lakefs.repository("data-repo")
main_branch = repo.branch("main")

# Create a tag pointing to current HEAD
tag = lakefs.Tag(repo.id, "v1.2").create(main_branch.id)
tag_commit_id = tag.get_commit()
```

You would then query metadata using: `repo-metadata.commit-<tag_commit_id>.system.object_metadata`.

!!! info
Direct querying by tag name (e.g., `repo-metadata.v1.2.system.object_metadata`) is not yet supported. You must
resolve the tag to a commit ID and use the `commit-<id>` pattern instead.

### Example Queries

This section showcases how to use lakeFS Metadata Search to answer different types of questions using standard SQL.

For simplicity and readability, the examples are written in Trino SQL. If you're using another engine (e.g., DuckDB, 
Spark, or PyIceberg), you may need to adjust the syntax accordingly.

#### Filter by Object Labels

The following example returns images labeled as dogs that are not in a sitting position:
```sql
USE "repo-metadata.main.system";
SHOW TABLES;

SELECT * FROM object_metadata
WHERE path LIKE `%.jpg`
  AND user_metadata['animal'] = 'dog'
  AND user_metadata['position'] != 'sitting';
```

#### Filter by Object Labels on past commit 

This example demonstrates how to write reproducible queries by referencing a specific lakeFS commit.
It returns images labeled as dogs that are not sitting, based on the metadata state at that commit:

```sql
USE "repo-metadata.commit-dc3117ec3a727104226c896bf7ab9350ee5da06ae052406262840e9a4a8c9ffb.system";
SHOW TABLES;

SELECT * FROM object_metadata
WHERE path LIKE `%.jpg`
  AND user_metadata['animal'] = 'dog'
  AND user_metadata['position'] != 'sitting';
```

#### View Metadata from AI-Powered Annotators

Assume that any object annotated by an AI-powered tool includes the object metadata key-value pair `source: autolabel` to 
indicate its origin. The following example returns all such AI-annotated objects: 

```sql
USE "repo-metadata.main.system";

SELECT *
FROM object_metadata
WHERE user_metadata['source'] = 'autolabel';
```

#### Filter by File Extension & Size

Find all `.png` files larger than 2MB.

```sql
USE "repo-metadata.main.system";

SELECT *
FROM object_metadata
WHERE path LIKE '%.png'
  AND size::INT > 2000000;
```

#### Detect Errors in Sensitive Data Tagging

Assume all objects under `customers/` must have user metadata `PII=true`. 
This example returns objects where `PII=false`, or PII key is missing. 

```sql
USE "repo-metadata.main.system";

SELECT *
FROM object_metadata
WHERE path LIKE 'customers/%'
  AND (
    user_metadata['PII'] = 'false'
        OR user_metadata['PII'] IS NULL
    );
```

#### Filter objects by addition Time

This example finds all objects added in the last 7 days.

```sql
USE "repo-metadata.main.system";

SELECT *
FROM object_metadata
WHERE creation_date >= current_timestamp - interval '7' day;
```

## Limitations

* Applies to new or changed objects only: lakeFS metadata tables do not include objects that existed in your repository 
before the feature was enabled. Only objects added or modified after metadata search was turned on will be indexed.
* No direct commit and tag support: Metadata tables do not natively support referencing commits and tags. To query by 
commit or tag, see the [Understanding References](#writing-reproducible-queries) section for the recommended approach.
