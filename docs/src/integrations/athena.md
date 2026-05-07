---
title: AWS Glue & Athena
description: Query lakeFS-managed Iceberg tables from Amazon Athena using AWS Glue Catalog Federation.
status: enterprise
---

# Using lakeFS with AWS Glue & Amazon Athena

!!! info
    Available in **lakeFS Enterprise**

!!! tip
    This integration requires the [lakeFS Iceberg REST Catalog](./iceberg.md) to be enabled.
    [Contact us](https://lakefs.io/lp/iceberg-rest-catalog/) to get started!

## Overview

[Amazon Athena](https://aws.amazon.com/athena/) can query lakeFS-managed Apache Iceberg tables directly through [AWS Glue Catalog Federation](https://docs.aws.amazon.com/glue/latest/dg/catalog-federation.html) -- no data copying or metadata syncing required.

Athena discovers table metadata in real time through lakeFS and reads the underlying data files directly from S3.

## Setup

Before querying from Athena, you need to create a federated Glue catalog that connects to your lakeFS Iceberg REST Catalog. Follow the [Glue Data Catalog integration guide](./glue_metastore.md) for step-by-step instructions on:

1. Installing the [`lakefs-glue`](https://github.com/treeverse/lakefs-glue-federation) CLI tool.
2. Creating a federated catalog pointing to a lakeFS repository and branch.
3. Granting access to the appropriate IAM principals.

## Querying from Athena

Once the federated catalog is created, query your lakeFS tables directly from Athena using the catalog name as a prefix:

```sql
SELECT * FROM "lakefs-catalog"."default"."my_table" LIMIT 10;
```

Run aggregations and joins across lakeFS-managed tables:

```sql
SELECT 
    category, 
    COUNT(*) AS total, 
    SUM(amount) AS total_amount
FROM "lakefs-catalog"."default"."transactions"
GROUP BY category
ORDER BY total_amount DESC;
```

## Comparing Data Across Branches

By creating [separate catalogs for different refs](./glue_metastore.md#working-with-multiple-branches-and-refs), you can compare data across branches, tags, or commits directly from Athena:

```sql
-- Compare row counts between production and dev
SELECT 'main' AS branch, COUNT(*) AS row_count 
FROM "my-repo-main"."default"."my_table"
UNION ALL
SELECT 'dev' AS branch, COUNT(*) AS row_count 
FROM "my-repo-dev"."default"."my_table";
```

## Limitations

- **Read-only**: AWS Glue Catalog Federation only supports read queries. `INSERT`, `CREATE TABLE`, and other write operations are not supported through Athena.
- **Single ref per catalog**: Each federated catalog points to one lakeFS ref. Create [multiple catalogs](./glue_metastore.md#working-with-multiple-branches-and-refs) to query multiple branches or tags.
- **Flat namespaces only**: AWS Glue Catalog Federation supports only flat `catalog.namespace.table` structures -- nested namespaces are not supported.
