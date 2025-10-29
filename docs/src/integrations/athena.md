---
title: Amazon Athena
description: This section shows how you can start querying data from lakeFS using Amazon Athena.
---

# Using lakeFS with Amazon Athena

[Amazon Athena](https://aws.amazon.com/athena/) is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL.

## Integration Overview

To query lakeFS data from Athena, you'll need to first export data to AWS Glue, and then use Athena to query it directly. To do that, use the automated [Data Catalog Exports](../howto/catalog_exports.md) feature, which allows you to:

- Query data directly from lakeFS branches and commits
- Access tables using branch names as schemas
- Leverage lakeFS versioning capabilities in your SQL queries

## Getting Started

For a complete step-by-step guide on setting up Athena with lakeFS, see the [Glue Data Catalog integration guide](./glue_metastore.md), which includes:

1. **Table Configuration**: Define your table schema using `_lakefs_tables/<table>.yaml`
2. **Automated Export**: Set up Lua hooks to export table metadata to Glue Catalog
3. **Query Setup**: Use Athena to query your lakeFS data with branch-specific schemas


