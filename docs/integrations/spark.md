---
layout: default
title: Spark
description: Accessing data in lakeFS from Apache Spark works the same as accessing S3 data from Apache Spark.
parent: Integrations
nav_order: 20
has_children: false
redirect_from: ../using/spark.html
---

# Using lakeFS with Spark
{: .no_toc }
[Apache Spark](https://spark.apache.org/) is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters

{: .pb-5 }

{% include toc.html %}

**Note** In all of the following examples, we set AWS and lakeFS credentials at runtime for
clarity. In production, properties defining AWS credentials should be set using one of
Hadoop's standard ways of [authenticating with
S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}.
Similarly, properties defining lakeFS credentials should be configured in secure site files,
not on the command line or inlined in code where they might be exposed.
{: .note}

## Two-tiered Spark support
{: .no_toc }

There are two ways you can use lakeFS with Spark:

* Using the [S3 gateway](#use-the-s3-gateway): get started quickly!
* Using the [lakeFS Hadoop FileSystem](#use-the-lakefs-hadoop-filesystem): fully unlock the performance of lakeFS.

## Use the S3 gateway

lakeFS has an S3-compatible endpoint. Simply point Spark to this endpoint to get started quickly.
You will access your data using S3-style URIs, e.g. `s3a://example-repo/example-branch/example-table`.

### Configuration
{: .no_toc }

To configure Spark to work with lakeFS, we set S3A Hadoop configuration to the lakeFS endpoint and credentials:

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

With this configuration set, you read S3A paths with `example-repo` as the bucket will use lakeFS, while all other buckets will use AWS S3.

### Reading Data
{: .no_toc }

To access objects in lakeFS, you need to use the lakeFS S3 gateway path
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

You can now use this DataFrame like you'd normally do.

### Writing Data
{: .no_toc }

Now simply write your results back to a lakeFS path:
```scala
df.write.partitionBy("example-column").parquet(s"s3a://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes or revert them.

## Use the lakeFS Hadoop FileSystem

If you're using lakeFS on top of S3, this mode will enhance your application's performance.
In this mode, Spark will read and write objects directly from S3, reducing the load on the lakeFS server.
It will still access the lakeFS server for metadata operations.

After configuring the lakeFS Hadoop FileSystem below, use URIs of the form `lakefs://example-repo/ref/path/to/data` to
interact with your data on lakeFS.

### Configuration
{: .no_toc }

1. Install the lakeFS Hadoop FileSystem:

   Add the package to your _spark-submit_ command:

   ```
   --packages io.lakefs:hadoop-lakefs-assembly:0.1.8
   ```

   The jar is also available on a public S3 location: `s3://treeverse-clients-us-east/hadoop/hadoop-lakefs-assembly-0.1.8.jar`

2. Configure the S3A filesystem with your S3 credentials (**not** the lakeFS credentials).
   Additionally, supply the `fs.lakefs.*` configurations to allow Spark to access metadata on lakeFS: 

   | Hadoop Configuration   | Value                                 |
   |------------------------|---------------------------------------|
   | `fs.s3a.access.key`    | Set to the AWS S3 access key          |
   | `fs.s3a.secret.key`    | Set to the AWS S3 secret key          |
   | `fs.lakefs.impl`       | `io.lakefs.LakeFSFileSystem`          |
   | `fs.lakefs.access.key` | Set to the lakeFS access key          |
   | `fs.lakefs.secret.key` | Set to the lakeFS secret key          |
   | `fs.lakefs.endpoint`   | Set to the lakeFS API URL             |

   The lakeFS Hadoop FileSystem uses the `fs.s3a.*` properties to directly
   access S3. If your cluster already has access to your buckets (for example, if you're using an AWS instance profile), then you don't need to configure these properties.
   permissions.
   {: .note }

   Here are some configuration examples:
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
                 --packages io.lakefs:hadoop-lakefs-assembly:0.1.8
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
   
   Make sure that you load the lakeFS FileSystem into Spark by running it with `--packages` or `--jars`,
   and then add these into a configuration file, e.g., `$SPARK_HOME/conf/hdfs-site.xml`:
   
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
   If your bucket is on a region other than us-east-1, you may also need to configure `fs.s3a.endpoint` with the correct region.
   Amazon provides [S3 endpoints](https://docs.aws.amazon.com/general/latest/gr/s3.html) you can use.
   {: .note }

### Reading Data
{: .no_toc }

To access objects in lakeFS, you need to use the lakeFS path conventions:

```
lakefs://[REPOSITORY]/[BRANCH]/PATH/TO/OBJECT
```

Here's an example for reading a parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "main"
val dataPath = s"lakefs://${repo}/${branch}/example-path/example-file.parquet"

val df = spark.read.parquet(dataPath)
```

You can now use this DataFrame like you would normally do.

### Writing Data
{: .no_toc }

Now simply write your results back to a lakeFS path:
```scala
df.write.partitionBy("example-column").parquet(s"lakefs://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes or revert them.

### Notes for the lakeFS Hadoop FileSystem

* Since data will not be sent to the lakeFS server, using this mode maximizes data security.
* The FileSystem implementation is tested with the latest Spark 2.X (Hadoop 2) and Spark 3.X (Hadoop 3) Bitnami images.

## Case Study: SimilarWeb

See how SimilarWeb is using lakeFS with Spark to [manage algorithm changes in data pipelines](https://grdoron.medium.com/a-smarter-way-to-manage-algorithm-changes-in-data-pipelines-with-lakefs-a4e284f8c756).
