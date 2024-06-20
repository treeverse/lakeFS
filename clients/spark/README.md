# lakeFS Spark Metadata Client

Read metadata from lakeFS into Spark.

## Features

1. Read Graveler meta-ranges, ranges and entries.
1. Export data from lakeFS to any object storage (see [docs](https://docs.lakefs.io/reference/export.html)).

_Please note that starting version 0.9.0, Spark 2 is not supported with the lakeFS metadata client._

## Installation

### Uber-jar
The Uber-Jar can be found on a public S3 location:
http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client/${CLIENT_VERSION}/lakefs-spark-client-assembly-${CLIENT_VERSION}.jar

### Maven
```
io.lakefs:lakefs-spark-client_2.12:${CLIENT_VERSION}
```

## Usage Examples
### Export using spark-submit

Replace `<version>` below with the latest version available. See [available versions](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client_2.12).

```
CLIENT_VERSION=0.14.0
spark-submit --conf spark.hadoop.lakefs.api.url=https://lakefs.example.com/api/v1 \
    --conf spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
    --conf spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
    --conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
    --conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
    --packages org.apache.hadoop:hadoop-aws:2.7.7,io.lakefs:lakefs-spark-client_2.12:${CLIENT_VERSION} \
    --class io.treeverse.clients.Main export-app example-repo s3://example-bucket/exported-data/ \
    --branch=main
```

### Export using spark-submit (uber-jar)

Replace `<version>` below with the latest version available. See [available versions](https://mvnrepository.com/artifact/io.lakefs/lakefs-spark-client_2.12).

```
CLIENT_VERSION=0.14.0
spark-submit --conf spark.hadoop.lakefs.api.url=https://lakefs.example.com/api/v1 \
    --conf spark.hadoop.fs.s3a.access.key=<S3_ACCESS_KEY> \
	--conf spark.hadoop.fs.s3a.secret.key=<S3_SECRET_KEY> \
	--conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY> \
	--conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_KEY> \
	--packages org.apache.hadoop:hadoop-aws:2.7.7 \
	--jars http://treeverse-clients-us-east.s3-website-us-east-1.amazonaws.com/lakefs-spark-client/${CLIENT_VERSION}/lakefs-spark-client-assembly-${CLIENT_VERSION}.jar \
	--class io.treeverse.clients.Main export-app example-repo s3://example-bucket/exported-data/ \
	--branch=main
```

## Publishing a new version

Follow the [Spark client release checklist](https://github.com/treeverse/dev/blob/main/pages/lakefs-clients-release.md#spark-metadata-client)

## Debugging

To debug the Exporter or the Garbage Collector using your IDE you can use a remote JVM debugger. You can follow [these](https://sparkbyexamples.com/spark/how-to-debug-spark-application-locally-or-remote/) instructions to connect one. 

