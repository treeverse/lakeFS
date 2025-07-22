---
title: Spark Client
description: The lakeFS Spark client performs operations on lakeFS committed metadata stored in the object store.
---

# lakeFS Spark Metadata Client

Utilize the power of Spark to interact with the metadata on lakeFS. Possible use cases include:

* Creating a DataFrame for listing the objects in a specific commit or branch.
* Computing changes between two commits.
* Exporting your data for consumption outside lakeFS.
* Bulk operations on the underlying storage.

## Getting Started

!!! note
    Please note that Spark **2.x** is no longer supported with the lakeFS metadata client.


The Spark metadata client is compiled for Spark 3.1.2 with Hadoop 3.2.1, but
can work for other Spark versions and higher Hadoop versions.


=== "PySpark, spark-shell, spark-submit, spark-sql"
    Start Spark Shell / PySpark with the `--packages` flag, for instance:

    ```bash
    spark-shell --packages io.lakefs:lakefs-spark-client_2.12:0.15.0
    ```

    Alternatively use the assembled jar (an "Überjar") on S3, from
    `s3://treeverse-clients-us-east/lakefs-spark-client/0.15.0/lakefs-spark-client-assembly-0.15.0.jar`
    by passing its path to `--jars`.

    The assembled jar is larger but shades several common libraries.  Use it if Spark
    complains about bad classes or missing methods.

=== "Databricks"
    Include this assembled jar (an "Überjar") from S3, from
    `s3://treeverse-clients-us-east/lakefs-spark-client/0.15.0/lakefs-spark-client-assembly-0.15.0.jar`.


## Configuration

To read metadata from lakeFS, the client should be configured with your lakeFS endpoint and credentials, using the following Hadoop configurations:

   | Configuration                        | Description                                                  |
   |--------------------------------------|--------------------------------------------------------------|
   | `spark.hadoop.lakefs.api.url`        | lakeFS API endpoint, e.g: `http://lakefs.example.com/api/v1` |
   | `spark.hadoop.lakefs.api.access_key` | The access key to use for fetching metadata from lakeFS      |
   | `spark.hadoop.lakefs.api.secret_key` | Corresponding lakeFS secret key                              |

## Examples

!!! example "Get a DataFrame for listing all objects in a commit"

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
