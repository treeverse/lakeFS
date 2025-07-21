---
title: Glue / Hive metastore
description: This section explains how to query data from lakeFS branches in services backed by Glue/Hive Metastore.
---

# Using lakeFS with the Glue/Hive Metastore

The integration between lakeFS and Glue/Hive Metastore is handled through [Data Catalog Exports](../howto/catalog_exports.md).

## Integration Features

- **Automated Export**: Hook-based automation that exports table metadata when specific events occur
- **Branch-aware Schemas**: Query different branches as separate schemas in your query engine
- **Multiple Format Support**: Works with Hive tables, Delta Lake, and other formats

## Available Integration Guides

Choose the integration guide that matches your use case:

### For AWS Glue Data Catalog Users
If you're using AWS Glue as your metastore (which backs Amazon Athena), see the comprehensive [Glue Data Catalog integration guide](./glue_metastore.md).

### For Hive Metastore Users  
If you're using Hive Metastore directly, refer to the [Data Catalog Exports documentation](../howto/catalog_exports.md) which covers:

- Table descriptor configuration (`_lakefs_tables/<table>.yaml`)
- Symlink exporter for Hive-compatible formats
- Hook configuration for automated exports


