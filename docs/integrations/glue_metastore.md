---
title: Glue / Athena support
description: This section explains how to query data from lakeFS branches in services backed by Glue Metastore in particular Athena.
parent: Integrations
redirect_from: /using/glue_metastore.html
---


# Using lakeFS with the Glue Metastore


{% include toc_2-3.html %}

## About Glue Metastore 

[AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html) has a metastore that can store metadata related to Hive and other services (such as Spark and Trino). It has metadata such as the location of the table, information about columns, partitions and much more.


## Support in lakeFS

The integration between Glue and lakeFS is based on [Data Catalog Exports]({% link integrations/catalog_exports.md %}).

### What is supported 

- Creating a uniqueue table in Glue Catalog per lakeFS repository / ref / commit. 
- No data copying is required, the table location is a path to a symlinks structure in S3 based on Hive's [SymlinkTextInputFormat](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r2.1.1/api/org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.html) and the [table partitions](https://docs.aws.amazon.com/glue/latest/dg/tables-described.html#tables-partition) are maintained.
- Tables are described via [Hive format in `_lakefs_tables/<my_table>.yaml`]({% link integrations/catalog_exports.md %}#hive-tables).
- Currently the data query in Glue metastore is Read-Only operation and mutating data requires writting to lakeFS and letting the export hook run.

### How it works 

Based on event such post-commit an Action will run a script that will create Symlink structures in S3 and then will register a table in Glue.
The Table data location will be the generated Symlinks root path.

There are 4 key pieces:

1. Table description at `_lakefs_tables/<your-table>.yaml`
2. Lua script that will do the export using [symlink_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportsymlink_exporter) and [glue_exporter]({% link howto/hooks/lua.md %}#lakefscatalogexportglue_exporter) packages.
3. [Action Lua Hook]({% link integrations/catalog_exports.md %}#running-an-exporter) to execute the lua hook.
4. Write some lakeFS table data ([Spark]({% link integrations/spark.md %}), CSV, etc)

To learn more check [Data Catalog Exports]({% link integrations/catalog_exports.md %}).

## Example: Using Athena to query lakeFS data

### Pre-requisite 

1. Glue Database to use (lakeFS does not create a database).
2. AWS Credentials with permission to manage Glue, Athena Query and S3 access.
3. lakeFS [Actions]({% link howto/hooks/index.md %}) enabled and configured with S3 blockstore.

### Create lakeFS repository 

Let's create a repository named `catalogs`. 

```bash
lakectl repo create lakefs://catalogs s3://<lakefs-bucket>/<repo-prefix>
```
### Add table descriptor

Let's define a table and commit to lakeFS. 
Save the YAML below as `animals.yaml` and upload it to lakeFS. 

```bash
lakectl fs upload lakefs://catalogs/main/_lakefs_tables/animals.yaml -s ./animals.yaml && \
lakectl commit lakefs://lua/main -m "added table"
```

```yaml 
name: animals
type: hive
# data location root in lakeFS
path: tables/animals
# partitions order
partition_columns: ['type', 'weight']
schema:
  type: struct
  # all the columns spec
  fields:
    - name: type
      type: string
      nullable: true
      metadata:
        comment: axolotl, cat, dog, fish etc
    - name: weight
      type: integer
      nullable: false
      metadata: {}
    - name: name
      type: string
      nullable: false
      metadata: {}
```

### Write some table data

Add some table data to lakeFS for example  `animals` table: 

|type   |weight|name   |
|-------|------|-------|
|axolotl|10    |jayson |
|axolotl|10    |john   |
|axolotl|12    |mark   |
|turtle |12    |mrgreen|
|bear   |30    |winnie |

{: .note}
> Writing data to a table can be done in many ways and formats, most common way would be using [Spark]({% link integrations/spark.md %}) and parquet format.

<div class="tabs">
  <ul>
    <li><a href="#csv">CSV</a></li>
    <li><a href="#spark-parquet">Spark Parquet</a></li>
  </ul> 
  <div markdown="1" id="csv">
#### CSV

The simplest way would be add a mock CSV table with `lakectl`.

Run the following script to upload the table data as Hive partitions:

```bash
repo='catalogs'
branch='main'
data_path='tables/animals'

cat <<EOF | lakectl fs upload lakefs://$repo/$branch/$data_path/type=axolotl/weight=10/partition.csv -s -
name
jayson
john
EOF

cat <<EOF | lakectl fs upload lakefs://$repo/$branch/$data_path/type=axolotl/weight=12/partition.csv -s -
name
mark
EOF

cat <<EOF | lakectl fs upload lakefs://$repo/$branch/$data_path/type=turtle/weight=12/partition.csv -s -
name
mrgreen
EOF

cat <<EOF | lakectl fs upload lakefs://$repo/$branch/$data_path/type=bear/weight=30/partition.csv -s -
name
winnie
EOF

lakectl commit lakefs://$repo/$branch -m "add table data"
```


  </div>
  <div markdown="1" id="spark-parquet">
#### Spark Parquet

The following example will create the table in spark DF, write to lakeFS via S3 Gateway and commit the data.

```python
import lakefs_client
from lakefs_client.models import CommitCreation
from lakefs_client.client import LakeFSClient
from pyspark.sql import SparkSession
# lakefs config 
lakefs_endpoint='http://localhost:8000' 
lakefs_access_key_id='<LAKEFS_KEY_ID<'
lakefs_secret_access_key='<LAKEFS_SECRET_ACCESS_KEY>'
# init lakeFS client 
configuration = lakefs_client.Configuration(host=lakefs_endpoint,username=lakefs_access_key_id,password=lakefs_secret_access_key)
configuration.verify_ssl = False
lakefsApi = LakeFSClient(configuration)
# init spark session 
builder = SparkSession.builder.appName("MyApp") \
    .config("spark.hadoop.fs.s3a.access.key", lakefs_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key",lakefs_secret_access_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", lakefs_endpoint) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark = builder.getOrCreate()
# write table data as parquet files to lakeFS 
repo="catalogs"
ref = "main"
tablePath = f"s3a://{repo}/{ref}/tables/animals"
data=[
    ("axolotl", 10, "jayson"),
    ("axolotl", 10, "john"),
    ("axolotl", 12, "mark"),
    ("turtle", 12, "mrgreen"),
    ("bear", 30, "winnie"),
]
columns=["type","weight","name"]
df=spark.createDataFrame(data,columns)
df.write.partitionBy("type","weight").mode("overwrite").parquet(f"{tablePath}/data.parquet")
# commit data to lakeFS 
lakefsApi.commits.commit(repository=repo,branch=ref,commit_creation=CommitCreation(message="data"))
```
  </div>

</div>