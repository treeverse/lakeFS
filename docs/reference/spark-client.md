---
title: Spark Client
description: The lakeFS Spark client performs operations on lakeFS committed metadata stored in the object store. 
parent: Reference
---


# lakeFS Spark Metadata Client

Utilize the power of Spark to interact with the metadata on lakeFS. Possible use cases include:

* Creating a DataFrame for listing the objects in a specific commit or branch.
* Computing changes between two commits.
* Exporting your data for consumption outside lakeFS.
* Bulk operations on the underlying storage.

## Getting Started

Please note that Spark 2 is no longer supported with the lakeFS metadata client.
{: .note }

The Spark metadata client is compiled for Spark 3.1.2 with Hadoop 3.2.1, but
can work for other Spark versions and higher Hadoop versions.

<div class="tabs">
  <ul>
    <li><a href="#spark-shell">PySpark, spark-shell, spark-submit, spark-sql</a></li>
	<li><a href="#databricks">DataBricks</a></li>
  </ul>
  <div markdown="1" id="spark-shell">
Start Spark Shell / PySpark with the `--packages` flag, for instance:

```bash
spark-shell --packages io.lakefs:lakefs-spark-client_2.12:0.13.0
```

Alternatively use the assembled jar (an "Überjar") on S3, from
`s3://treeverse-clients-us-east/lakefs-spark-client/0.13.0/lakefs-spark-client-assembly-0.13.0.jar`
by passing its path to `--jars`.
The assembled jar is larger but shades several common libraries.  Use it if Spark
complains about bad classes or missing methods.
</div>
<div markdown="1" id="databricks">
Include this assembled jar (an "Überjar") from S3, from
`s3://treeverse-clients-us-east/lakefs-spark-client/0.13.0/lakefs-spark-client-assembly-0.13.0.jar`.
</div>
</div>

## Configuration

1. To read metadata from lakeFS, the client should be configured with your lakeFS endpoint and credentials, using the following Hadoop configurations:

   | Configuration                        | Description                                                  |
   |--------------------------------------|--------------------------------------------------------------|
   | `spark.hadoop.lakefs.api.url`        | lakeFS API endpoint, e.g: `http://lakefs.example.com/api/v1` |
   | `spark.hadoop.lakefs.api.access_key` | The access key to use for fetching metadata from lakeFS      |
   | `spark.hadoop.lakefs.api.secret_key` | Corresponding lakeFS secret key                              |

1. The client will also directly interact with your storage using Hadoop FileSystem.
   Therefore, your Spark session must be able to access the underlying storage of your lakeFS repository.
   There are [various ways to do this](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"},
   but for a non-production environment you can use the following Hadoop configurations:

   | Configuration                    | Description                                              |
   |----------------------------------|----------------------------------------------------------|
   | `spark.hadoop.fs.s3a.access.key` | Access key to use for accessing underlying storage on S3 |
   | `spark.hadoop.fs.s3a.secret.key` | Corresponding secret key to use with S3 access key       |

   ### Assuming role on S3 (Hadoop 3 only)

   The client includes support for assuming a separate role on S3 when
   running on Hadoop 3. It uses the same configuration used by
   `S3AFileSystem` to assume the role on S3A. Apache Hadoop AWS
   documentation has details under "[Working with IAM Assumed
   Roles][s3a-assumed-role]". You will need to use the following Hadoop
   configurations:
   
   | Configuration                     | Description                                                          |
   |-----------------------------------|----------------------------------------------------------------------|
   | `fs.s3a.aws.credentials.provider` | Set to `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider` |
   | `fs.s3a.assumed.role.arn`         | Set to the ARN of the role to assume                                 |

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


[s3a-assumed-role]:  https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/assumed_roles.html#Configuring_Assumed_Roles
