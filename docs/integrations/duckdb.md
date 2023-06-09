---
layout: default
title: DuckDB
description: This section explains how you can start using lakeFS with DuckDB, an open-source SQL OLAP database management system.
parent: Integrations
nav_order: 60
has_children: false
---

# Using lakeFS with DuckDB

[DuckDB](https://duckdb.org/){:target="_blank"} is an in-process SQL OLAP database management system.

{% include toc.html %}

## Configuration

Querying data in lakeFS from DuckDB is similar to querying data in S3 from DuckDB. It is done using the [httpfs extension](https://duckdb.org/docs/extensions/httpfs.html){:target="_blank"} connecting to the S3 Gateway that lakeFS provides.

If not loaded already, install and load the HTTPFS extension: 

```sql
INSTALL httpfs;
LOAD httpfs;
```

Then run the following to configure the connection. 

```sql
SET s3_region='us-east-1';
SET s3_endpoint='lakefs.example.com';
SET s3_access_key_id='AKIAIOSFODNN7EXAMPLE';
SET s3_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';
SET s3_url_style='path';

-- Uncomment in case the endpoint listen on non-secure, for example running lakeFS locally.
-- SET s3_use_ssl=false;
```

* `s3_endpoint` is the host (and port, if necessary) of your lakeFS server
* `s3_access_key_id` and `s3_secret_access_key` are the access credentials for your lakeFS user
* `s3_url_style` needs to be set to `path`
* `s3_region` is the S3 region on which your bucket resides. If local storage, or not S3, then just set it to `us-east-1`. 

## Querying Data

Once configured, you can query data using the lakeFS S3 Gateway using the following URI pattern:

```text
s3://<REPOSITORY NAME>/<REFERENCE ID>/<PATH TO DATA>
```

Since the S3 Gateway implemenets all S3 functionality required by DuckDB, you can query using globs and patterns, including support for Hive-partitioned data.

Example:

```sql
SELECT * 
FROM parquet_scan('s3://example-repo/main/data/population/by-region/*.parquet', HIVE_PARTITIONING=1) 
ORDER BY name;
```

## Writing Data

No special configuration required for writing to a branch. Assuming the configuration above and write permissions to a `dev` branch,
a write operation would look like any DuckDB write:

```sql
CREATE TABLE sampled_population AS SELECT * 
FROM parquet_scan('s3://example-repo/main/data/population/by-region/*.parquet', HIVE_PARTITIONING=1) 
USING SAMPLE reservoir(50000 ROWS) REPEATABLE (100);

COPY sampled_population TO 's3://example-repo/main/data/population/sample.parquet'; -- actual write happens here
```
