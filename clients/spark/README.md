# lakeFS Spark Metadata Client

Read metadata from lakeFS into Spark.

## Features

1. Read Graveler meta-ranges, ranges and entries.
1. Export data from lakeFS to any object storage (see [docs](https://docs.lakefs.io/reference/export.html)).

_Please note that starting version 0.9.0, Spark 2 is not supported with the lakeFS metadata client._

## Installation

### Uber-jar
The Uber-Jar can be found on a public S3 location:

It should be used when running into conflicting dependencies on environments like EMR, Databricks, etc.

For Spark 3.1.2+:
http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-312-hadoop3/${CLIENT_VERSION}/lakefs-spark-client-312-hadoop3-assembly-${CLIENT_VERSION}.jar

For Spark 3.0.1:
http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-301/${CLIENT_VERSION}/lakefs-spark-client-301-assembly-${CLIENT_VERSION}.jar


### Maven
Otherwise, the client can be included using Maven coordinates:

For Spark 3.1.2+:
```
io.lakefs:lakefs-spark-client-312-hadoop3_2.12:<version>
```
[See available versions](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-312-hadoop3_2.12).

For Spark 3.0.1:
```
io.lakefs:lakefs-spark-client-301_2.12:<version>
```
[See available versions](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-301_2.12).

## Usage Examples
### Export using spark-submit

Replace `<version>` below with the latest version available. See available versions for [Spark 3.1.2+](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-312-hadoop3_2.12) or [Spark 3.0.1](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-301_2.12).

```
CLIENT_VERSION=0.8.1
SPARK_VERSION=301 # or 312-hadoop3
SCALA_VERSION=2.12
spark-submit --conf spark.hadoop.lakefs.api.url=https://lakefs.example.com/api/v1 \
    --conf spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
    --conf spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
    --conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
    --conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
    --packages org.apache.hadoop:hadoop-aws:2.7.7,\
           io.lakefs:lakefs-spark-client-${SPARK_VERSION}_${SCALA_VERSION}:${CLIENT_VERSION} \
    --class io.treeverse.clients.Main export-app example-repo s3://example-bucket/exported-data/ \
    --branch=main
```

### Export using spark-submit (uber-jar)

Replace `<version>` below with the latest version available. See available versions for [Spark 3.1.2+](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-312-hadoop3_2.12) or [Spark 3.0.1](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client-301_2.12).
```
CLIENT_VERSION=0.8.1
SPARK_VERSION=301 # or 312-hadoop3

spark-submit --conf spark.hadoop.lakefs.api.url=https://lakefs.example.com/api/v1 \
    --conf spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
	--conf spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
	--conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
	--conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
	--packages org.apache.hadoop:hadoop-aws:2.7.7 \
	--jars http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client-${SPARK_VERSION}/${CLIENT_VERSION}/lakefs-spark-client-${SPARK_VERSION}-assembly-${CLIENT_VERSION}.jar \
	--class io.treeverse.clients.Main export-app example-repo s3://example-bucket/exported-data/ \
	--branch=main
```

## Publishing a new version

Follow the [Spark client release checklist](https://github.com/treeverse/dev/blob/main/pages/lakefs-clients-release.md#spark-metadata-client)

## Debugging

To debug the Exporter or the Garbage Collector using your IDE you can use a remote JVM debugger. You can follow [these](https://sparkbyexamples.com/spark/how-to-debug-spark-application-locally-or-remote/) instructions to connect one. 

