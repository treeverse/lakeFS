---
layout: default
title: Spark
description: Accessing data in lakeFS from Apache Spark works the same as accessing S3 data from Apache Spark.
parent: Integrations
nav_order: 10
has_children: false
redirect_from: 
  - ../using/spark.html
  - ../using/databricks.html
  - ./databricks.html
  - ./emr.html
  - ../using/emr.html
  - ./glue_etl.html
  - ../using/glue_etl.html
---

# Using lakeFS with Spark
{: .no_toc }

There are two ways to use lakeFS with Spark:

* Using the [lakeFS Hadoop FileSystem](#use-the-lakefs-hadoop-filesystem): Highly scalable, data flows directly from client to storage.
* Using the [S3 gateway](#use-the-s3-gateway): Scalable and simpler to set up.

{: .pb-5 }

⚠️ The following examples include inline credentials for the sake of simplicity.
In production, credentials should be protected using security best practices.
{: .note }

## Use the lakeFS Hadoop FileSystem

If you're using lakeFS on top of S3, this mode will enhance your application's performance.
In this mode, Spark will read and write objects directly from S3, reducing the load on the lakeFS server.
It will still access the lakeFS server for metadata operations.

After configuring the lakeFS Hadoop FileSystem below, use URIs of the form `lakefs://example-repo/ref/path/to/data` to
interact with your data on lakeFS.

### Installation

<div class="tabs">
  <ul>
    <li><a href="#install-standalone">Spark Standalone</a></li>
    <li><a href="#install-databricks">Databricks</a></li>
  </ul> 
  <div markdown="1" id="install-standalone">

  Add the package to your `spark-submit` command:

  ```
  --packages io.lakefs:hadoop-lakefs-assembly:0.1.12
  ```

  </div>
  <div markdown="2" id="install-databricks">

**Note** For your convenience, follow this [step by step Databricks integration tutorial, including lakeFS Hadoop File System, Python client and lakeFS SPARK client](https://lakefs.io/blog/databricks-lakefs-integration-tutorial/).
{: .note }


  Add the `hadoop-lakefs-assembly` jar to your Databricks cluster. Under `compute ➡️ Libraries` Add a Maven package as follows:

  Install a Maven package with the `io.lakefs:hadoop-lakefs-assembly:0.1.12` 
  coordinates.

  Once installed, it should look something like this:

  ![Databricks - Adding the lakeFS client Jar](../assets/img/databricks-install-package.png)

  
  </div>
</div>


### Configuration

Configure the S3A filesystem with your S3 credentials (**not** the lakeFS credentials).
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
{: .note }

Here are some configuration examples:
<div class="tabs">
  <ul>
    <li><a href="#config-cli">CLI</a></li>
    <li><a href="#config-scala">Scala</a></li>
    <li><a href="#config-pyspark">PySpark</a></li>
    <li><a href="#config-xml">XML Configuration</a></li>
    <li><a href="#config-databricks">Databricks</a></li>
  </ul> 
  <div markdown="1" id="config-cli">
```shell
spark-shell --conf spark.hadoop.fs.s3a.access.key='AKIAIOSFODNN7EXAMPLE' \
              --conf spark.hadoop.fs.s3a.secret.key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' \
              --conf spark.hadoop.fs.s3a.endpoint='https://s3.eu-central-1.amazonaws.com' \
              --conf spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem \
              --conf spark.hadoop.fs.lakefs.access.key=AKIAlakefs12345EXAMPLE \
              --conf spark.hadoop.fs.lakefs.secret.key=abc/lakefs/1234567bPxRfiCYEXAMPLEKEY \
              --conf spark.hadoop.fs.lakefs.endpoint=https://lakefs.example.com/api/v1 \
              --packages io.lakefs:hadoop-lakefs-assembly:0.1.12
              ...
```
  </div>
  <div markdown="1" id="config-scala">

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
  <div markdown="1" id="config-pyspark">

```python
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://s3.eu-central-1.amazonaws.com")
sc._jsc.hadoopConfiguration().set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem")
sc._jsc.hadoopConfiguration().set("fs.lakefs.access.key", "AKIAlakefs12345EXAMPLE")
sc._jsc.hadoopConfiguration().set("fs.lakefs.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
sc._jsc.hadoopConfiguration().set("fs.lakefs.endpoint", "https://lakefs.example.com/api/v1")
```
  </div>
  <div markdown="1" id="config-xml">
   
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
  <div markdown="1" id="config-databricks">

Add the following the cluster's configuration under `Configuration ➡️ Advanced options`:

```
spark.hadoop.fs.lakefs.impl io.lakefs.LakeFSFileSystem
spark.hadoop.fs.lakefs.access.key AKIAlakefs12345EXAMPLE
spark.hadoop.fs.lakefs.secret.key abc/lakefs/1234567bPxRfiCYEXAMPLEKEY
spark.hadoop.fs.s3a.access.key AKIAIOSFODNN7EXAMPLE
spark.hadoop.fs.s3a.secret.key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
spark.hadoop.fs.s3a.impl shaded.databricks.org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.lakefs.endpoint https://lakefs.example.com/api/v1
```

  Alternatively, follow this [step by step Databricks integration tutorial, including lakeFS Hadoop File System, Python client and lakeFS SPARK client](https://lakefs.io/blog/databricks-lakefs-integration-tutorial/).
  </div>
</div>
   
⚠️ If your bucket is on a region other than us-east-1, you may also need to configure `fs.s3a.endpoint` with the correct region.
Amazon provides [S3 endpoints](https://docs.aws.amazon.com/general/latest/gr/s3.html) you can use.
{: .note }

### Usage

Hadoop FileSystem paths use the `lakefs://` protocol, with paths taking the form `lakefs://<repository>/<ref>/path/to/object`.
`<ref>` can be a branch, tag, or commit ID in lakeFS.
Here's an example for reading a Parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "main"
val df = spark.read.parquet(s"lakefs://${repo}/${branch}/example-path/example-file.parquet")
```

Here's how to write some results back to a lakeFS path:

```scala
df.write.partitionBy("example-column").parquet(s"lakefs://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes or revert them.

### Notes for the lakeFS Hadoop FileSystem

* Since data will not be sent to the lakeFS server, using this mode maximizes data security.
* The FileSystem implementation is tested with the latest Spark 2.X (Hadoop 2) and Spark 3.X (Hadoop 3) Bitnami images.

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
    <li><a href="#s3-config-tabs-emr">EMR</a></li>
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
  <div markdown="1" id="s3-config-tabs-emr">
  Use the below configuration when creating the cluster. You may delete any app configuration that is not suitable for your use case:

```json
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.sql.catalogImplementation": "hive"
    }
  },
  {
    "Classification": "core-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://lakefs.example.com",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://lakefs.example.com",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://lakefs.example.com",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://lakefs.example.com",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "presto-connector-hive",
    "Properties": {
        "hive.s3.aws-access-key": "AKIAIOSFODNN7EXAMPLE",
        "hive.s3.aws-secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "hive.s3.endpoint": "https://lakefs.example.com",
        "hive.s3.path-style-access": "true",
        "hive.s3-file-system-type": "PRESTO"
    }
  },
  {
    "Classification": "hive-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://lakefs.example.com",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://lakefs.example.com",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "hdfs-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://lakefs.example.com",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://lakefs.example.com",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "mapred-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://lakefs.example.com",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://lakefs.example.com",
        "fs.s3a.path.style.access": "true"
    }
  }
]
```

Alternatively, you can pass these configuration values when adding a step.

For example:

```bash
aws emr add-steps --cluster-id j-197B3AEGQ9XE4 \
  --steps="Type=Spark,Name=SparkApplication,ActionOnFailure=CONTINUE, \
  Args=[--conf,spark.hadoop.fs.s3a.access.key=AKIAIOSFODNN7EXAMPLE, \
  --conf,spark.hadoop.fs.s3a.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY, \
  --conf,spark.hadoop.fs.s3a.endpoint=https://lakefs.example.com, \
  --conf,spark.hadoop.fs.s3a.path.style.access=true, \
  s3a://<lakefs-repo>/<lakefs-branch>/path/to/jar]"
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
    <li><a href="#s3-bucket-config-tabs-emr">EMR</a></li>
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
  <div markdown="1" id="s3-bucket-config-tabs-emr">
  Use the below configuration when creating the cluster. You may delete any app configuration that is not suitable for your use case:

```json
[
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.sql.catalogImplementation": "hive"
    }
  },
  {
    "Classification": "core-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "presto-connector-hive",
    "Properties": {
        "hive.s3.aws-access-key": "AKIAIOSFODNN7EXAMPLE",
        "hive.s3.aws-secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "hive.s3.endpoint": "https://lakefs.example.com",
        "hive.s3.path-style-access": "true",
        "hive.s3-file-system-type": "PRESTO"
    }
  },
  {
    "Classification": "hive-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "hdfs-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "mapred-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://lakefs.example.com",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  }
]
```

Alternatively, you can pass these configuration values when adding a step.

For example:

```bash
aws emr add-steps --cluster-id j-197B3AEGQ9XE4 \
  --steps="Type=Spark,Name=SparkApplication,ActionOnFailure=CONTINUE, \
  Args=[--conf,spark.hadoop.fs.s3a.bucket.example-repo.access.key=AKIAIOSFODNN7EXAMPLE, \
  --conf,spark.hadoop.fs.s3a.bucket.example-repo.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY, \
  --conf,spark.hadoop.fs.s3a.bucket.example-repo.endpoint=https://lakefs.example.com, \
  --conf,spark.hadoop.fs.s3a.path.style.access=true, \
  s3a://<lakefs-repo>/<lakefs-branch>/path/to/jar]"
```

  </div>
</div>

With this configuration set, you read S3A paths with `example-repo` as the bucket will use lakeFS, while all other buckets will use AWS S3.

### Usage

Here's an example for reading a Parquet file from lakeFS to a Spark DataFrame:

```scala
val repo = "example-repo"
val branch = "main"
val df = spark.read.parquet(s"s3a://${repo}/${branch}/example-path/example-file.parquet")
```

Here's how to write some results back to a lakeFS path:
```scala
df.write.partitionBy("example-column").parquet(s"s3a://${repo}/${branch}/output-path/")
```

The data is now created in lakeFS as new changes in your branch. You can now commit these changes or revert them.

### Configuring Azure Databricks with the S3 Gateway
{: .no_toc }

If you use Azure Databricks, you would want to take advantage of the lakeFS S3 gateway with your Azure account, using the S3A File system. 
Meaning, you’ll need to add the `hadoop-aws` jar (with the same version of your `hadoop-azure` jar) to your Databricks cluster.
The S3 gateway makes lakeFS compatible with S3, meaning that the S3A file system can “talk” to it natively
Define your File system configurations in the following way:

```
spark.hadoop.fs.lakefs.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.lakefs.access.key=‘AKIAlakefs12345EXAMPLE’                   // The access key to your lakeFS server
spark.hadoop.fs.lakefs.secret.key=‘abc/lakefs/1234567bPxRfiCYEXAMPLEKEY’     // The secret key to your lakeFS server
spark.hadoop.fs.lakefs.path.style.access=true
spark.hadoop.fs.lakefs.endpoint=‘https://lakefs.example.com’                 // The endpoint of your lakeFS server
```

For more details about [Mounting cloud object storage on Databricks](https://docs.databricks.com/dbfs/mounts.html).

## Case Study: SimilarWeb

See how SimilarWeb is using lakeFS with Spark to [manage algorithm changes in data pipelines](https://grdoron.medium.com/a-smarter-way-to-manage-algorithm-changes-in-data-pipelines-with-lakefs-a4e284f8c756).
