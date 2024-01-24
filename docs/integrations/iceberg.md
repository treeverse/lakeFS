---
title: Apache Iceberg
description: How to integrate lakeFS with Apache Iceberg
parent: Integrations
---

# Using lakeFS with Apache Iceberg

{% include toc_2-3.html %}

To enrich your Iceberg tables with lakeFS capabilities, you can use the lakeFS implementation of the Iceberg catalog.
You will then be able to query your Iceberg tables using lakeFS references, such as branches, tags, and commit hashes: 

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
  <version>0.1.4</version>
</dependency>
```

</div>
<div markdown="1" id="pyspark">
  Include the `lakefs-iceberg` jar in your package list along with Iceberg. For example: 

```python
.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,io.lakefs:lakefs-iceberg:0.1.4")
```  
</div>
</div>

## Configure

<div class="tabs">
  <ul>
    <li><a href="#conf-pyspark">PySpark</a></li>
    <li><a href="#conf-sparkshell">Spark Shell</a></li>
  </ul>
  <div markdown="1" id="conf-pyspark">

Set up the Spark SQL catalog: 
```python
.config("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog") \
.config("spark.sql.catalog.lakefs.warehouse", f"lakefs://{repo_name}") \ 
.config("spark.sql.catalog.lakefs.cache-enabled", "false")
```

Configure the S3A Hadoop FileSystem with your lakeFS connection details.
Note that these are your lakeFS endpoint and credentials, not your S3 ones.
    
```python
.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.hadoop.fs.s3a.endpoint", "https://example-org.us-east-1.lakefscloud.io") \
.config("spark.hadoop.fs.s3a.access.key", "AKIAIO5FODNN7EXAMPLE") \
.config("spark.hadoop.fs.s3a.secret.key", "wJalrXUtnFEMI/K3MDENG/bPxRfiCYEXAMPLEKEY") \
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```

  </div>

  <div markdown="1" id="conf-sparkshell">
```shell
spark-shell --conf spark.sql.catalog.lakefs="org.apache.iceberg.spark.SparkCatalog" \
   --conf spark.sql.catalog.lakefs.catalog-impl="io.lakefs.iceberg.LakeFSCatalog" \
   --conf spark.sql.catalog.lakefs.warehouse="lakefs://example-repo" \
   --conf spark.sql.catalog.lakefs.cache-enabled="false" \
   --conf spark.hadoop.fs.s3.impl="org.apache.hadoop.fs.s3a.S3AFileSystem" \
   --conf spark.hadoop.fs.s3a.endpoint="https://example-org.us-east-1.lakefscloud.io" \
   --conf spark.hadoop.fs.s3a.access.key="AKIAIO5FODNN7EXAMPLE" \
   --conf spark.hadoop.fs.s3a.secret.key="wJalrXUtnFEMI/K3MDENG/bPxRfiCYEXAMPLEKEY" \
   --conf spark.hadoop.fs.s3a.path.style.access="true"
```
  </div>
</div>


## Using Iceberg tables with lakeFS

### Create a table

To create a table on your main branch, use the following syntax:

```sql
CREATE TABLE lakefs.main.db1.table1 (id int, data string);
```

### Insert data into the table
    
```sql
INSERT INTO lakefs.main.db1.table1 VALUES (1, 'data1');
INSERT INTO lakefs.main.db1.table1 VALUES (2, 'data2');
```

### Create a branch

We can now commit the creation of the table to the main branch:

```shell
lakectl commit lakefs://example-repo/main -m "my first iceberg commit"
```

Then, create a branch:

```shell
lakectl branch create lakefs://example-repo/dev -s lakefs://example-repo/main
```

### Make changes on the branch

We can now make changes on the branch:

```sql
INSERT INTO lakefs.dev.db1.table1 VALUES (3, 'data3');
```

### Query the table

If we query the table on the branch, we will see the data we inserted:

```sql
SELECT * FROM lakefs.dev.db1.table1;
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
SELECT * FROM lakefs.main.db1.table1;
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

[ref-expr]:  {% link understand/model.md %}#ref-expressions
