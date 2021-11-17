---
layout: default
title: Spark
description: Accessing data in lakeFS from Apache Spark is the same as accessing S3 data from Apache Spark
parent: Integrations
nav_order: 20
has_children: false
redirect_from: ../using/spark.html
---

# Using lakeFS with Spark
{: .no_toc }
[Apache Spark](https://spark.apache.org/) is a unified analytics engine for big data processing, with built-in modules for streaming, SQL, machine learning and graph processing.

{: .pb-5 }

{% include toc.html %}

**Note** In all following examples we set AWS and lakeFS credentials at runtime, for
clarity. In production, properties defining AWS credentials should be set using one of
Hadoop's standard ways of [authenticating with
S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}.
Similarly, properties defining lakeFS credentials should be configured in secure site files,
not on the command line or inlined in code where they might be exposed.
{: .note}

## Two-tiered Spark support

lakeFS support in Spark has two tiers:

* Access lakeFS using the [S3A gateway](#access-lakefs-using-the-s3a-gateway).
* Access lakeFS using the [lakeFS-specific Hadoop
  FileSystem](#access-lakefs-using-the-lakefs-specific-hadoop-filesystem).

Using the S3A gateway is easier to configure and may be more suitable for legacy or
small-scale applications.  Using the lakeFS FileSystem requires somewhat more complex
configuration, but offers greatly increased performance.



## Access lakeFS using the S3A gateway

To use this mode you configure the Spark application to use S3A using the S3-compatible
endpoint which the lakeFS server provides.  Accordingly all data flows through the lakeFS
server.

Accessing data in lakeFS from Spark is the same as accessing S3 data from Spark.  The only
changes we need to consider are:
1. Setting the configurations to access lakeFS.
1. Accessing objects using the lakeFS S3 path convention.

### Configuration

In order to configure Spark to work with lakeFS, we set S3 Hadoop configuration to the lakeFS endpoint and credentials:

| Hadoop Configuration          | Value                                        |
|-------------------------------|----------------------------------------------|
| `fs.s3a.access.key`           | Set to the lakeFS access key                 |
| `fs.s3a.secret.key`           | Set to the lakeFS secret key                 |
| `fs.s3a.endpoint`             | Set to the lakeFS S3-compatible API endpoint |
| `fs.s3a.path.style.access`    | Set to `true`                                |

Here is how to do it:
<div class="tabs">
  <ul>
    <li><a href="#s3-config-tabs-cli">CLI</a></li>
    <li><a href="#s3-config-tabs-code">Scala</a></li>
    <li><a href="#s3-config-tabs-xml">XML Configuration</a></li>
  </ul>
  <div markdown="1" id="s3-config-tabs-cli">
```shell
spark-shell --conf spark.hadoop.fs.s3a.access.key='AKIAlakefs12345EXAMPLE' \
              --conf spark.hadoop.fs.s3a.secret.key='abc/lakefs/1234567bPxRfiCYEXAMPLEKEY' \
              --conf spark.hadoop.fs.s3a.path.style.access=true \
              --conf spark.hadoop.fs.s3a.endpoint='https://lakefs.example.com' ...
```
  </div>
  <div markdown="1" id="s3-config-tabs-code">
```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAlakefs12345EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "https://lakefs.example.com")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
```
  </div>
  <div markdown="1" id="s3-config-tabs-xml">
Add these into a configuration file, e.g. `$SPARK_HOME/conf/hdfs-site.xml`:
```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3a.access.key</name>
        <value>AKIAlakefs12345EXAMPLE</value>
    </property>
    <property>
            <name>fs.s3a.secret.key</name>
            <value>abc/lakefs/1234567bPxRfiCYEXAMPLEKEY</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>https://lakefs.example.com</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
</configuration>
```
  </div>
</div>

#### Per-bucket configuration

The above configuration will use lakeFS as the sole S3 endpoint. To use lakeFS in parallel with S3, you can configure Spark to use lakeFS only for specific bucket names.
For example, to configure only `example-repo` to use lakeFS, set the following configurations:

<div class="tabs">
  <ul>
    <li><a href="#s3-bucket-config-tabs-cli">CLI</a></li>
    <li><a href="#s3-bucket-config-tabs-code">Scala</a></li>
    <li><a href="#s3-bucket-config-tabs-xml">XML Configuration</a></li>
  </ul>
  <div markdown="1" id="s3-bucket-config-tabs-cli">
```sh
spark-shell --conf spark.hadoop.fs.s3a.bucket.example-repo.access.key='AKIAlakefs12345EXAMPLE' \
              --conf spark.hadoop.fs.s3a.bucket.example-repo.secret.key='abc/lakefs/1234567bPxRfiCYEXAMPLEKEY' \
              --conf spark.hadoop.fs.s3a.bucket.example-repo.endpoint='https://lakefs.example.com' \
              --conf spark.hadoop.fs.s3a.path.style.access=true
```
  </div>
  <div markdown="1" id="s3-bucket-config-tabs-code">
```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.example-repo.access.key", "AKIAlakefs12345EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.example-repo.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.example-repo.endpoint", "https://lakefs.example.com")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
```
  </div>
  <div markdown="1" id="s3-bucket-config-tabs-xml">
Add these into a configuration file, e.g. `$SPARK_HOME/conf/hdfs-site.xml`:
```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3a.bucket.example-repo.access.key</name>
        <value>AKIAlakefs12345EXAMPLE</value>
    </property>
    <property>
        <name>fs.s3a.bucket.example-repo.secret.key</name>
        <value>abc/lakefs/1234567bPxRfiCYEXAMPLEKEY</value>
    </property>
    <property>
        <name>fs.s3a.bucket.example-repo.endpoint</name>
        <value>https://lakefs.example.com</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
</configuration>
```
  </div>
</div>

With this configuration set , reading s3a paths with `example-repo` as the bucket will use lakeFS, while all other buckets will use AWS S3.

### Reading Data
In order for us to access objects in lakeFS we will need to use the lakeFS S3 gateway path
conventions:

```
s3a://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT
```

Here is an example for reading a parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "main"
val dataPath = s"s3a://${repo}/${branch}/example-path/example-file.parquet"

val df = spark.read.parquet(dataPath)
```

You can now use this DataFrame like you would normally do.

### Writing Data

Now simply write your results back to a lakeFS path:
```scala
df.write.partitionBy("example-column").parquet(s"s3a://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes, or revert them.


## Access lakeFS using the lakeFS-specific Hadoop FileSystem

To use this mode you configure the Spark application to perform metadata operations on the
lakeFS server, and all data operations directly through the same underlying object store that
lakeFS uses.  The lakeFS FileSystem currently supports Spark with Hadoop Apache 2.7 using only the S3A Hadoop FileSystem
for data access.  In this mode the Spark application will directly read and write from the
underlying object store, significantly increasing application scalability and performance by
reducing the load on the lakeFS server.

Accessing data in lakeFS from Spark is the same as accessing S3 data from Spark.  The only
changes we need to perform are:

1. Configure Spark to access lakeFS for metadata and S3 or a compatible underlying object
   store to access data.
1. Use `lakefs://repo/ref/path/to/data` URIs to read and write data on lakeFS, rather than
   `s3a://...` URIs.

### Configuration

In order to configure Spark to work using the lakeFS Hadoop FileSystem, you will need to load
the filesystem JARs and then configure both that FileSystem and the underlying data access
FileSystem.

#### Load the FileSystem JARs

Add the package `io.lakefs:hadoop-lakefs-assembly:<VERSION>` to your Spark job.  Right now
this is version 0.1.3, so add:

```
--packages io.lakefs:hadoop-lakefs-assembly:0.1.3
```

to your Spark commandlines.

#### Configure the lakeFS FileSystem and the underlying S3A FileSystem

Add Hadoop configuration to the underlying storage and additionally to lakeFS credentials.
When using this mode, do **not** set the S3A endpoint URL to point at lakeFS -- it should
point at the underlying storage.

| Hadoop Configuration   | Value                                 |
|------------------------|---------------------------------------|
| `fs.s3a.access.key`    | Set to the AWS S3 access key          |
| `fs.s3a.secret.key`    | Set to the AWS S3 secret key          |
| `fs.s3a.endpoint`      | Set to the AWS S3-compatible endpoint |
| `fs.lakefs.impl`       | `io.lakefs.LakeFSFileSystem`          |
| `fs.lakefs.access.key` | Set to the lakeFS access key          |
| `fs.lakefs.secret.key` | Set to the lakeFS secret key          |
| `fs.lakefs.endpoint`   | Set to the lakeFS API URL             |

When using AWS S3 itself, the default configuration works with us-east-1, so you may still
need to configure `fs.s3a.endpoint`.  Amazon provides these [S3
endpoints](https://docs.aws.amazon.com/general/latest/gr/s3.html) you can use.

**Note:** If not running on AWS, all s3a configuration properties are required!  Unlike when
using the S3 gateway, when using the lakeFS-specific Hadoop FileSystem you configure `s3a` to
access the S3 underlying object storage, and `lakefs` to access the lakeFS server.  When
running on AWS you do not need to configure credentials if the instance profile has sufficient
permissions.

Here is how to do it:
<div class="tabs">
  <ul>
    <li><a href="#lakefs-config-tabs-cli">CLI</a></li>
    <li><a href="#lakefs-config-tabs-code">Scala</a></li>
    <li><a href="#lakefs-config-tabs-xml">XML Configuration</a></li>
  </ul> 
  <div markdown="1" id="lakefs-config-tabs-cli">
```shell
spark-shell --conf spark.hadoop.fs.s3a.access.key='AKIAIOSFODNN7EXAMPLE' \
              --conf spark.hadoop.fs.s3a.secret.key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' \
              --conf spark.hadoop.fs.s3a.endpoint='https://s3.eu-central-1.amazonaws.com' \
              --conf spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem \
              --conf spark.hadoop.fs.lakefs.access.key=AKIAlakefs12345EXAMPLE \
              --conf spark.hadoop.fs.lakefs.secret.key=abc/lakefs/1234567bPxRfiCYEXAMPLEKEY \
              --conf spark.hadoop.fs.lakefs.endpoint=https://lakefs.example.com/api/v1 \
              --packages io.lakefs:hadoop-lakefs-assembly:0.1.3
              ...
```
  </div>
  <div markdown="1" id="lakefs-config-tabs-code">

Ensure you load the lakeFS FileSystem into Spark by running it with `--packages` or `--jars`,
and then run:

```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "https://s3.eu-central-1.amazonaws.com")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.access.key", "AKIAlakefs12345EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.endpoint", "https://lakefs.example.com/api/v1")
```
  </div>
  <div markdown="1" id="lakefs-config-tabs-xml">

Ensure you load the lakeFS FileSystem into Spark by running it with `--packages` or `--jars`,
and then add these into a configuration file, e.g. `$SPARK_HOME/conf/hdfs-site.xml`:

```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3a.access.key</name>
        <value>AKIAIOSFODNN7EXAMPLE</value>
    </property>
    <property>
            <name>fs.s3a.secret.key</name>
            <value>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>https://s3.eu-central-1.amazonaws.com</value>
    </property>
    <property>
        <name>fs.lakefs.impl</name>
        <value>io.lakefs.LakeFSFileSystem</value>
    </property>
    <property>
        <name>fs.lakefs.access.key</name>
        <value>AKIAlakefs12345EXAMPLE</value>
    </property>
    <property>
        <name>fs.lakefs.secret.key</name>
        <value>abc/lakefs/1234567bPxRfiCYEXAMPLEKEY</value>
    </property>
    <property>
        <name>fs.lakefs.endpoint</name>
        <value>https://lakefs.example.com/api/v1</value>
    </property>
</configuration>
```
  </div>
</div>

#### Per-bucket and per-repo configuration

As above, S3 allows for per-bucket configuration.  You can use this if:

1. You need to use S3A directly to access data in an S3 outside of lakeFS, _and_
1. different credentials are required to access data inside that bucket.

Refer to the Hadoop AWS guide on [Configuring different S3 buckets with Per-Bucket
Configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets_with_Per-Bucket_Configuration).

There is no need for per-repo configurations in lakeFS when all repositories are on the same
lakeFS server.  If you need to access repositories that are on *multiple* lakeFS servers,
configure multiple prefixes.  For instance, you might configure both `fs.lakefs.impl` and
`fs.lakefs2.impl` to be `io.lakefs.LakeFSFileSystem`, place separate endpoints and credentials
under `fs.lakefs.*` and `fs.lakefs2.*`, and access the two servers using `lakefs://...` and
`lakefs2://...` URLs.

### Reading Data
In order for us to access objects in lakeFS we will need to use the lakeFS path conventions:

```
lakefs://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT
```

Here is an example for reading a parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "main"
val dataPath = s"lakefs://${repo}/${branch}/example-path/example-file.parquet"

val df = spark.read.parquet(dataPath)
```

You can now use this DataFrame like you would normally do.

### Writing Data

Now simply write your results back to a lakeFS path:
```scala
df.write.partitionBy("example-column").parquet(s"lakefs://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes, or revert them.

## Case Study: SimilarWeb

See how SimilarWeb is using lakeFS with Spark to [manage algorithm changes in data pipelines](https://grdoron.medium.com/a-smarter-way-to-manage-algorithm-changes-in-data-pipelines-with-lakefs-a4e284f8c756).
