---
layout: default
title: EMR
description: This section covers how you can start using lakeFS with Amazon EMR, an AWS managed service that simplifies running open-source big data frameworks.
parent: Integrations
nav_order: 35
has_children: false
redirect_from: ../using/emr.html
---

# Using lakeFS with EMR

[Amazon EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html){: .button-clickable} is a managed cluster platform that simplifies running big data frameworks, such as Apache Hadoop and Apache Spark.

## Configuration
In order to configure Spark on EMR to work with lakeFS we will set the lakeFS credentials and endpoint in the appropriate fields.
The exact configuration keys depends on the application running in EMR, but their format is of the form:

lakeFS endpoint: `*.fs.s3a.endpoint`

lakeFS access key: `*.fs.s3a.access.key`

lakeFS secret key: `*.fs.s3a.secret.key`

EMR will encourage users to use s3:// with Spark as it will use EMR's proprietary driver. Users need to use s3a:// for this guide to work.
{: .note}

The Spark job reads and writes will be directed to the lakeFS instance, using the [s3 gateway](../understand/architecture.md#s3-gateway){: .button-clickable}.

There are 2 options for configuring an EMR cluster to work with lakeFS:
1. When you create a cluster - All steps will use the cluster configuration.
   No specific configuration needed when adding a step.
1. Configuring on each step - cluster is created with the default s3 configuration.
   Each step using lakeFS should pass the appropriate config params.

## Configuration on cluster creation

Use the below configuration when creating the cluster. You may delete any app configuration which is not suitable for your use-case.
```json
[{
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
   },
   {
      "Classification": "spark-defaults",
      "Properties": {
         "spark.sql.catalogImplementation": "hive"
      }
   }
]
```

## Configuration on adding a step

When a cluster was created without the above configuration, you can still use lakeFS when adding a step.

For example, when creating a Spark job:

```shell
aws emr add-steps --cluster-id j-197B3AEGQ9XE4 \
  --steps="Type=Spark,Name=SparkApplication,ActionOnFailure=CONTINUE, \
  Args=[--conf,spark.hadoop.fs.s3a.access.key=AKIAIOSFODNN7EXAMPLE, \
  --conf,spark.hadoop.fs.s3a.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY, \
  --conf,spark.hadoop.fs.s3a.endpoint=https://lakefs.example.com, \
  --conf,spark.hadoop.fs.s3a.path.style.access=true, \
  s3a://<lakefs-repo>/<lakefs-branch>/path/to/jar]"
```

The Spark context in the running job will already be initialized to use the provided lakeFS configuration.
There's no need to repeat the configuration steps mentioned in [Using lakeFS with Spark](spark.md#Configuration){: .button-clickable}
{: .note}
