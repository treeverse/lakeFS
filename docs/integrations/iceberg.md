---
layout: default
title: Apache Iceberg
description: How to integrate lakeFS with Apache Iceberg
parent: Integrations
nav_order: 10000 # last! TODO: put this near the top once we fully support Iceberg :)
has_children: false
---

# Using lakeFS with Apache Iceberg

{% include toc_2-3.html %}

lakeFS provides its own implementation of the `SparkCatalog`, which handles the writing of the Iceberg data to lakeFS as well as reading from it and branching. Using straightforward table identifiers you can switch between branches when reading and writing data: 

```sql
SELECT * FROM catalog.ref.db.table
```

## Setup

<div class="tabs">
  <ul>
    <li><a href="#maven">Maven</a></li>
    <li><a href="#pyspark">PySpark</a></li>
  </ul>
  <div markdown="1" id="maven">


Use the following Maven dependency to install the lakeFS custom catalog:

```xml
<dependency>
  <groupId>io.lakefs</groupId>
  <artifactId>lakefs-iceberg</artifactId>
  <version>0.1.1</version>
</dependency>
```

</div>
<div markdown="1" id="pyspark">
  Include the `lakefs-iceberg` jar in your package list along with Iceberg. For example: 

```python
.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,io.lakefs:lakefs-iceberg:0.1.1")
```  
</div>
</div>

## Configure

1. Set up the Spark SQL catalog: 

    ```python
    .config("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog") \
    .config("spark.sql.catalog.lakefs.warehouse", f"lakefs://{repo_name}") \
    .config("spark.sql.catalog.lakefs.uri", lakefsEndPoint) \
    ```

2. Optionally, you can set the `lakeFS` catalog to be the default one, which means that you don't need to include the prefix when referencing tables. 

    ```python
    .config("spark.sql.defaultCatalog", "lakefs") \
    ```

3. Configure the S3A Hadoop FileSystem for lakeFS. 

    ```python
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", lakefsEndPoint) \
    .config("spark.hadoop.fs.s3a.access.key", lakefsAccessKey) \
    .config("spark.hadoop.fs.s3a.secret.key", lakefsSecretKey) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    ```

## Using Iceberg tables with lakeFS

When referencing tables you need to ensure that they have a database specified (as you would anyway), and then a lakeFS [reference](/understand/model.html#ref-expressions) prefix. 

A reference is one of: 

* Branch
* Tag
* Expression (for example, `main~17` means _17 commits before main_)

_If you have not set your default catalog then you need to include this as a prefix to the lakeFS reference._

Here are some examples: 

* The table `db.table1` on the `main` branch of lakeFS: 

    ```sql
    SELECT * FROM main.db.table1;
    ```

* The table `db.table1` on the `dev` branch of lakeFS: 

    ```sql
    SELECT * FROM dev.db.table1;
    ```

* The table `db.table1` on the `dev` branch of lakeFS, configured through the Spark SQL catalog `foo`: 

    ```sql
    SELECT * FROM foo.dev.db.table1;
    ```

* One commit previous to the table `db.table1` on the `dev` branch of lakeFS

    ```sql
    SELECT * FROM `dev~1`.db.table1;
    ```

* Only committed data on the table `db.table1` on the `dev` branch of lakeFS

    ```sql
    SELECT * FROM `dev@`.db.table1;
    ```

## Migrating an existing Iceberg Table to lakeFS Catalog

This is done through an incremental copy from the original table into lakeFS. 

1. Create a new lakeFS repository `lakectl repo create lakefs://example-repo <base storage path>`
2. Initiate a spark session that can interact with the source iceberg table and the target lakeFS catalog. 

    Here's an example of Hadoop and S3 session and lakeFS catalog with [per-bucket config](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/bk_cloud-data-access/content/s3-per-bucket-configs.html): 

    ```java
    SparkConf conf = new SparkConf();
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true");

    // set hadoop on S3 config (source tables we want to copy) for spark
    conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
    conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop");
    conf.set("spark.sql.catalog.hadoop_prod.warehouse", "s3a://my-bucket/warehouse/hadoop/");
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    conf.set("spark.hadoop.fs.s3a.bucket.my-bucket.access.key", "<AWS_ACCESS_KEY>");
    conf.set("spark.hadoop.fs.s3a.bucket.my-bucket.secret.key", "<AWS_SECRET_KEY>");

    // set lakeFS config (target catalog and repository)
    conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
    conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
    conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://example-repo");
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    conf.set("spark.hadoop.fs.s3a.bucket.example-repo.access.key", "<LAKEFS_ACCESS_KEY>");
    conf.set("spark.hadoop.fs.s3a.bucket.example-repo.secret.key", "<LAKEFS_SECRET_KEY>");
    conf.set("spark.hadoop.fs.s3a.bucket.example-repo.endpoint"  , "<LAKEFS_ENDPOINT>");
    ```

3. Create Schema in lakeFS and copy the data 

    Example of copy with spark-sql: 

    ```SQL
    -- Create Iceberg Schema in lakeFS
    CREATE SCHEMA IF NOT EXISTS <lakefs-catalog>.<branch>.<db>
    -- Create new iceberg table in lakeFS from the source table (pre-lakeFS)
    CREATE TABLE IF NOT EXISTS <lakefs-catalog>.<branch>.<db> USING iceberg AS SELECT * FROM <iceberg-original-table>
    ```
