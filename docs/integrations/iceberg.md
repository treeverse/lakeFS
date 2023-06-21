---
layout: default
title: Iceberg
description: How to integrate lakeFS with Apache Iceberg
parent: Integrations
nav_order: 10000 # last! TODO: put this near the top once we fully support Iceberg :)
has_children: false
---

# Using lakeFS with Iceberg

## lakeFS Icerberg Catalog

lakeFS enriches your Iceberg tables with Git capabilities: create a branch and make your changes in isolation, without affecting other team members.

### Install

Use the following Maven dependency to install the lakeFS custom catalog:

```xml
<dependency>
  <groupId>io.lakefs</groupId>
  <artifactId>lakefs-iceberg</artifactId>
  <version>0.1.0</version>
</dependency>
```

### Configure

Here is how to configure the lakeFS custom catalog in Spark:
```scala
conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://<LAKEFS_REPO>");
conf.set("spark.sql.catalog.lakefs.uri", "<LAKEFS_ENDPOINT>")
```

You will also need to configure the S3A Hadoop FileSystem to interact with lakeFS:
```scala
conf.set("fs.s3a.access.key", "<LAKEFS_ACCESS_KEY>")
conf.set("fs.s3a.secret.key", "<LAKEFS_SECRET_KEY>")
conf.set("fs.s3a.endpoint", "<LAKEFS_ENDPOINT>")
conf.set("fs.s3a.path.style.access", "true")
```

### Table refernce

To reference the iceberg table in lakeFS you'll need to specify lakeFS branch you are working on:
```sql
CREATE TABLE catalog_name.lakefs_branch.table_name (id int, data string);
```

### Create a table

To create a table on your main branch, use the following syntax:

```sql
CREATE TABLE lakefs.main.table1 (id int, data string);
```

### Create a branch

We can now commit the creation of the table to the main branch:

```
lakectl commit lakefs://example-repo/main -m "my first iceberg commit"
```

Then, create a branch:

```
lakectl branch create lakefs://example-repo/dev -s lakefs://example-repo/main
```

### Make changes on the branch

We can now make changes on the branch:

```sql
INSERT INTO lakefs.dev.table1 VALUES (3, 'data3');
```

### Query the table

If we query the table on the branch, we will see the data we inserted:

```sql
SELECT * FROM lakefs.dev.table1;
```

Results in:
```
+----+------+
| id | data |
+----+------+
| 1  | data1|
| 2  | data2|
| 3  | data3|
+----+------+
```

However, if we query the table on the main branch, we will not see the new changes:

```sql
SELECT * FROM lakefs.main.table1;
```

Results in:
```
+----+------+
| id | data |
+----+------+
| 1  | data1|
| 2  | data2|
+----+------+
```

### Migrate to lakeFS Catalog