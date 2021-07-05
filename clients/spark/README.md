# lakeFS Spark Metadata Client

Read metadata from lakeFS into Spark.

## Features

1. Read Graveler meta-ranges, ranges and entries.
1. Export data from lakeFS to any object storage (see [docs](https://docs.lakefs.io/reference/export.html)).

## Scala/Spark compatibility

Two versions are available for the client, compatible with the following Spark/Scala versions:
1. Spark 2 / Scala 2.11
1. Spark 3 / Scala 2.12

## Publishing a new version

We publish the client to Sonatype, and an Uber-Jar of the client to S3.
The Uber-Jar should be used when running into conflicting dependencies on environments like EMR, Databricks, etc.
Otherwise, the client can be included using Maven coordinates.

1. Have the following files ready:

   1. `~/.sbt/sonatype_credentials`, with the content:
      ```
       realm=Sonatype Nexus Repository Manager
       host=s01.oss.sonatype.org
       user=<your sonatype username>
       password=<your sonatype password>
      ```

   1. `~/.sbt/credentials`, with the content:
      ```
      realm=Amazon S3
      host=treeverse-clients-us-east.s3.amazonaws.com
      user=<AWS access key>
      password=<AWS secret key>
      ```
1. Increment the version in the build.sbt file.

1. From the lakeFS project root, run:
   ```bash
   make publish-scala
   ```
   
## Usage Examples
### Export using spark-submit
```
CLIENT_VERSION=0.1.0
SPARK_VERSION=301 # or 247
SCALA_VERSION=2.12 # or 2.11
spark-submit --conf spark.hadoop.lakefs.api.url=https://lakefs.example.com/api/v1/ \
--conf spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
--conf spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
--conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
--conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
--packages io.lakefs:lakefs-spark-client-${SPARK_VERSION}_${SCALA_VERSION}:${CLIENT_VERSION} \
--class io.treeverse.clients.Main export-app example-repo s3://example-bucket/exported-data/ \
--branch=main
```

### Export using spark-submit (uber-jar)
```
CLIENT_VERSION=0.1.4-SNAPSHOT
SPARK_VERSION=301 # or 247

spark-submit --conf spark.hadoop.lakefs.api.url=https://lakefs.example.com/api/v1/ \
--conf spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
--conf spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
--conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
--conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
--jars http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-${SPARK_VERSION}/${CLIENT_VERSION}/lakefs-spark-client-${SPARK_VERSION}-assembly-${CLIENT_VERSION}.jar \
--class io.treeverse.clients.Main export-app example-repo s3://example-bucket/exported-data/ \
--branch=main
```
