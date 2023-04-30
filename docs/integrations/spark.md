---
layout: default
title: Spark
description: Accessing data in lakeFS from Apache Spark works the same as accessing S3 data from Apache Spark.
parent: Integrations
nav_order: 10
has_children: false
redirect_from: 
  - /integrations/databricks.html
  - /integrations/emr.html
  - /integrations/glue_etl.html
  - /using/databricks.html
  - /using/emr.html
  - /using/glue_etl.html
  - /using/spark.html
---

# Using lakeFS with Spark
{: .no_toc }

## Ways to use lakeFS with Spark

* [The S3-compatible API](#use-the-s3-compatible-api): Scalable and best to get started. <span class="badge">All Storage Vendors</span>
* [The lakeFS FileSystem](#use-the-lakefs-hadoop-filesystem): Direct data flow from client to storage, highly scalable. <span class="badge">AWS S3</span>
   * [lakeFS FileSystem in Presigned mode](#hadoop-filesystem-in-presigned-mode-beta): Best of both worlds, but still in beta. <span class="badge mr-1">AWS S3</span><span class="badge">Azure Blob</span>

{: .pb-5 }

## Use the S3-compatible API

lakeFS has an S3-compatible endpoint. Simply point Spark to this endpoint to get started quickly.
You will access your data using S3-style URIs, e.g. `s3a://example-repo/example-branch/example-table`.
You can use the S3-compatible API regardless of where your data is hosted.

### Configuration
{: .no_toc }

To configure Spark to work with lakeFS, we set S3A Hadoop configuration to the lakeFS endpoint and credentials:

* `fs.s3a.access.key`: lakeFS access key
* `fs.s3a.secret.key`: lakeFS secret key
* `fs.s3a.endpoint`: lakeFS S3-compatible API endpoint (e.g. https://example-org.us-east-1.lakefscloud.io)
* `fs.s3a.path.style.access`: `true`

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
              --conf spark.hadoop.fs.s3a.endpoint='https://example-org.us-east-1.lakefscloud.io' ...
```
  </div>
  <div markdown="1" id="s3-config-tabs-code">
```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAlakefs12345EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "https://example-org.us-east-1.lakefscloud.io")
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
        <value>https://example-org.us-east-1.lakefscloud.io</value>
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
        "fs.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "presto-connector-hive",
    "Properties": {
        "hive.s3.aws-access-key": "AKIAIOSFODNN7EXAMPLE",
        "hive.s3.aws-secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "hive.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "hive.s3.path-style-access": "true",
        "hive.s3-file-system-type": "PRESTO"
    }
  },
  {
    "Classification": "hive-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "hdfs-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.path.style.access": "true"
    }
  },
  {
    "Classification": "mapred-site",
    "Properties": {
        "fs.s3.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.path.style.access": "true",
        "fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint": "https://example-org.us-east-1.lakefscloud.io",
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
  --conf,spark.hadoop.fs.s3a.endpoint=https://example-org.us-east-1.lakefscloud.io, \
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
              --conf spark.hadoop.fs.s3a.bucket.example-repo.endpoint='https://example-org.us-east-1.lakefscloud.io' \
              --conf spark.hadoop.fs.s3a.path.style.access=true
```
  </div>
  <div markdown="1" id="s3-bucket-config-tabs-code">
```scala
spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.example-repo.access.key", "AKIAlakefs12345EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.example-repo.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.bucket.example-repo.endpoint", "https://example-org.us-east-1.lakefscloud.io")
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
        <value>https://example-org.us-east-1.lakefscloud.io</value>
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
        "fs.s3.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "presto-connector-hive",
    "Properties": {
        "hive.s3.aws-access-key": "AKIAIOSFODNN7EXAMPLE",
        "hive.s3.aws-secret-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "hive.s3.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "hive.s3.path-style-access": "true",
        "hive.s3-file-system-type": "PRESTO"
    }
  },
  {
    "Classification": "hive-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "hdfs-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3a.bucket.example-repo.path.style.access": "true"
    }
  },
  {
    "Classification": "mapred-site",
    "Properties": {
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
        "fs.s3.bucket.example-repo.path.style.access": "true",
        "fs.s3a.bucket.example-repo.access.key": "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.bucket.example-repo.secret.key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.bucket.example-repo.endpoint": "https://example-org.us-east-1.lakefscloud.io",
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
  --conf,spark.hadoop.fs.s3a.bucket.example-repo.endpoint=https://example-org.us-east-1.lakefscloud.io, \
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

### Configuring Azure Databricks with the S3-compatible API
{: .no_toc }

If you use Azure Databricks, you can take advantage of the lakeFS S3-compatible API with your Azure account and the S3A FileSystem. 
This will require installing the `hadoop-aws` package (with the same version as your `hadoop-azure` package) to your Databricks cluster.

Define your FileSystem configurations in the following way:

```
spark.hadoop.fs.lakefs.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.lakefs.access.key=‘AKIAlakefs12345EXAMPLE’                   // The access key to your lakeFS server
spark.hadoop.fs.lakefs.secret.key=‘abc/lakefs/1234567bPxRfiCYEXAMPLEKEY’     // The secret key to your lakeFS server
spark.hadoop.fs.lakefs.path.style.access=true
spark.hadoop.fs.lakefs.endpoint=‘https://example-org.us-east-1.lakefscloud.io’                 // The endpoint of your lakeFS server
```

For more details about [Mounting cloud object storage on Databricks](https://docs.databricks.com/dbfs/mounts.html).

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
    <li><a href="#install-cloudera-spark">Cloudera Spark</a></li>
  </ul> 
  <div markdown="1" id="install-standalone">

Add the package to your `spark-submit` command:

  ```
  --packages io.lakefs:hadoop-lakefs-assembly:0.1.14
  ```

  </div>
  <div markdown="2" id="install-databricks">
In  your cluster settings, under the _Libraries_ tab, add the following Maven package:

```
io.lakefs:hadoop-lakefs-assembly:0.1.14
```

Once installed, it should look something like this:

![Databricks - Adding the lakeFS client Jar](/assets/img/databricks-install-package.png)

  </div>
  <div markdown="3" id="install-cloudera-spark">

Add the package to your `pyspark` or `spark-submit` command:

  ```
  --packages io.lakefs:hadoop-lakefs-assembly:0.1.14
  ```

Add the configuration to access the S3 bucket used by lakeFS to your `pyspark` or `spark-submit` command or add this configuration at the Cloudera cluster level (see below):

  ```
  --conf spark.yarn.access.hadoopFileSystems=s3a://bucket-name
  ```

Add the configuration to access the S3 bucket used by lakeFS at the Cloudera cluster level:
1. Log in to the CDP (Cloudera Data Platform) web interface.
1. From the CDP home screen, click the `Management Console` icon.
1. In the Management Console, select `Data Hub Clusters` from the navigation pane.
1. Select the cluster you want to configure. Click on `CM-UI` link under Services:
  ![Cloudera - Management Console](/assets/img/cloudera/ManagementConsole.png)
1. In Cloudera Manager web interface, click on `Clusters` from the navigation pane and click on `spark_on_yarn` option:
  ![Cloudera - Cloudera Manager](/assets/img/cloudera/ClouderaManager.png)
1. Click on `Configuration` tab and search for `spark.yarn.access.hadoopFileSystems` in the search box:
  ![Cloudera - spark_on_yarn](/assets/img/cloudera/spark_on_yarn.png)
1. Add S3 bucket used by lakeFS `s3a://bucket-name` in the `spark.yarn.access.hadoopFileSystems` list:
  ![Cloudera - hadoopFileSystems](/assets/img/cloudera/hadoopFileSystems.png)
  </div>
</div>


### Configuration

Set the `fs.lakefs.*` Hadoop configurations to point to your lakeFS installation:
* `fs.lakefs.impl`: `io.lakefs.LakeFSFileSystem`
* `fs.lakefs.access.key`: lakeFS access key
* `fs.lakefs.secret.key`: lakeFS secret key
* `fs.lakefs.endpoint`: lakeFS API URL (e.g. `https://example-org.us-east-1.lakefscloud.io/api/v1`)

Configure the S3A FileSystem to access your S3 storage, for example using the `fs.s3a.*` configurations (these are **not** your lakeFS credentials):
* `fs.s3a.access.key`: AWS S3 access key
* `fs.s3a.secret.key`: AWS S3 secret key

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
              --conf spark.hadoop.fs.lakefs.endpoint=https://example-org.us-east-1.lakefscloud.io/api/v1 \
              --packages io.lakefs:hadoop-lakefs-assembly:0.1.14 \
              io.example.ExampleClass
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
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.endpoint", "https://example-org.us-east-1.lakefscloud.io/api/v1")
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
sc._jsc.hadoopConfiguration().set("fs.lakefs.endpoint", "https://example-org.us-east-1.lakefscloud.io/api/v1")
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
        <value>https://example-org.us-east-1.lakefscloud.io/api/v1</value>
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
spark.hadoop.fs.lakefs.endpoint https://example-org.us-east-1.lakefscloud.io/api/v1
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

### Hadoop FileSystem in Presigned mode <sup>BETA</sup>
_Available starting version 0.1.13 of the FileSystem_

In this mode, the lakeFS server is responsible for authenticating with your storage.
The client will still perform data operations directly on the storage.
To do so, it will use pre-signed storage URLs provided by the lakeFS server.

When using this mode, you don't need to configure the client with access to your storage:

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
spark-shell --conf spark.hadoop.fs.access.mode=presigned \
              --conf spark.hadoop.fs.lakefs.impl=io.lakefs.LakeFSFileSystem \
              --conf spark.hadoop.fs.lakefs.access.key=AKIAlakefs12345EXAMPLE \
              --conf spark.hadoop.fs.lakefs.secret.key=abc/lakefs/1234567bPxRfiCYEXAMPLEKEY \
              --conf spark.hadoop.fs.lakefs.endpoint=https://example-org.us-east-1.lakefscloud.io/api/v1 \
              --packages io.lakefs:hadoop-lakefs-assembly:0.1.14 \
              io.example.ExampleClass
```
  </div>
  <div markdown="1" id="config-scala">

```scala
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.access.mode", "presigned")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.access.key", "AKIAlakefs12345EXAMPLE")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
spark.sparkContext.hadoopConfiguration.set("fs.lakefs.endpoint", "https://example-org.us-east-1.lakefscloud.io/api/v1")
```
  </div>
  <div markdown="1" id="config-pyspark">

```python
sc._jsc.hadoopConfiguration().set("fs.lakefs.access.mode", "presigned")
sc._jsc.hadoopConfiguration().set("fs.lakefs.impl", "io.lakefs.LakeFSFileSystem")
sc._jsc.hadoopConfiguration().set("fs.lakefs.access.key", "AKIAlakefs12345EXAMPLE")
sc._jsc.hadoopConfiguration().set("fs.lakefs.secret.key", "abc/lakefs/1234567bPxRfiCYEXAMPLEKEY")
sc._jsc.hadoopConfiguration().set("fs.lakefs.endpoint", "https://example-org.us-east-1.lakefscloud.io/api/v1")
```
  </div>
  <div markdown="1" id="config-xml">

Make sure that you load the lakeFS FileSystem into Spark by running it with `--packages` or `--jars`,
and then add these into a configuration file, e.g., `$SPARK_HOME/conf/hdfs-site.xml`:

```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.lakefs.access.mode</name>
        <value>presigned</value>
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
        <value>https://example-org.us-east-1.lakefscloud.io/api/v1</value>
    </property>
</configuration>
```
  </div>
  <div markdown="1" id="config-databricks">

Add the following the cluster's configuration under `Configuration ➡️ Advanced options`:

```
spark.hadoop.fs.access.mode presigned
spark.hadoop.fs.lakefs.impl io.lakefs.LakeFSFileSystem
spark.hadoop.fs.lakefs.access.key AKIAlakefs12345EXAMPLE
spark.hadoop.fs.lakefs.secret.key abc/lakefs/1234567bPxRfiCYEXAMPLEKEY
spark.hadoop.fs.lakefs.endpoint https://example-org.us-east-1.lakefscloud.io/api/v1
```

## Case Study: SimilarWeb

See how SimilarWeb is using lakeFS with Spark to [manage algorithm changes in data pipelines](https://grdoron.medium.com/a-smarter-way-to-manage-algorithm-changes-in-data-pipelines-with-lakefs-a4e284f8c756).
