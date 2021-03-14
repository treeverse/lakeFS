# lakeFS Spark Client

Utilize the power of Spark to interact with the metadata on lakeFS. Possible use-cases include:

* Create a DataFrame for listing the objects in a specific commit or branch.
* Compute changes between two commits.
* Export your data for consumption outside lakeFS.
* Bulk operations on underlying storage.

## Getting Started
Start Spark Shell / PySpark with the `--packages` flag:

```bash
spark-shell --packages io.treeverse:lakefs-spark-client_2.12:0.1.0-SNAPSHOT
```

## Configuration

1. To read metadata from lakeFS, the client should be configured with your lakeFS endpoint and credentials, using the following Hadoop configurations:

    | Configuration                        | Description                                                  |
    |--------------------------------------|--------------------------------------------------------------|
    | `spark.hadoop.lakefs.api.url`        | lakeFS API endpoint, e.g: `http://lakefs.example.com/api/v1` |
    | `spark.hadoop.lakefs.api.access_key` | The access key to use for fetching metadata from lakeFS      |
    | `spark.hadoop.lakefs.api.secret_key` | Corresponding lakeFS secret key                              |

1. The client will also directly interact with your storage using Hadoop FileSystem. Therefore, your Spark session must be able to access the underlying storage of your lakeFS repository.
For instance, running as a user with a personal account on S3 (not in production) you might add:
    | Configuration | Description |
    |--------|-------|
    | `spark.hadoop.fs.s3a.access.key` | Access key to use for accessing underlying storage on S3 |
    | `spark.hadoop.fs.s3a.secret.key`  | Corresponding secret key to use with S3 access key |


## Examples

1. Get a DataFrame for listing all objects in a commit:
    
    ```scala
    import io.treeverse.clients.LakeFSContext
    
    val commitID = "a1b2c3d4"
    val df = LakeFSContext.newDF(spark, "example-repo", commitID)
    df.show
    /* output example:
       +------------+--------------------+--------------------+-------------------+----+
       |        key |             address|                etag|      last_modified|size|
       +------------+--------------------+--------------------+-------------------+----+
       |     file_1 |791457df80a0465a8...|7b90878a7c9be5a27...|2021-03-05 11:23:30|  36|
       |     file_2 |e15be8f6e2a74c329...|95bee987e9504e2c3...|2021-03-05 11:45:25|  36|
       |     file_3 |f6089c25029240578...|32e2f296cb3867d57...|2021-03-07 13:43:19|  36|
       |     file_4 |bef38ef97883445c8...|e920efe2bc220ffbb...|2021-03-07 13:43:11|  13|
       +------------+--------------------+--------------------+-------------------+----+
     */
    ```
1. Run SQL queries on your metadata:
   
   ```scala
    df.createOrReplaceTempView("files")
    spark.sql("SELECT DATE(last_modified), COUNT(*) FROM files GROUP BY 1 ORDER BY 1")
    /* output example:
       +----------+--------+
       |        dt|count(1)|
       +----------+--------+
       |2021-03-05|       2|
       |2021-03-07|       2|
       +----------+--------+
     */
   ```


