---
layout: default
title: EMR
description: This section covers how you can start using lakeFS with EMR, an AWS service that uses open-source frameworks to process vast amounts of data.
parent: Using lakeFS with...
nav_order: 6
has_children: false
---

# Using lakeFS with EMR

EMR is AWS big data platform for processing data using open source tools such as Spark, Hive and more.

Once you have your EMR cluster up and running, you can configure it to use your lakeFS installation as the s3 gateway.


## Configuration
In order to configure EMR to work with lakeFS we will set the lakeFS credentials and endpoint in the appropriate fields.
    
lakeFS endpoint: ```spark.hadoop.fs.s3a.endpoint``` 

lakeFS access key: ```spark.hadoop.fs.s3a.access.key```

lakeFS secret key: ```spark.hadoop.fs.s3a.secret.key```

## aws cli Spark Example 

```shell
aws emr add-steps --cluster-id j-197B3AEGQ9XE4 \
--steps="Type=Spark,Name=SparkApplication,ActionOnFailure=CONTINUE,\
Args=[--conf,spark.hadoop.fs.s3a.access.key=AKIAIOSFODNN7EXAMPLE,\
--conf,spark.hadoop.fs.s3a.secret.key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY,\
--conf,spark.hadoop.fs.s3a.endpoint=https://s3.lakefs.example.com,\
s3a://<lakefs-repo>/<lakefs-branch>/path/to/jar]"
```

The Spark job reads and writes will be directed to the lakeFS instance, using the [s3 gateway](../architecture.md#S3 Gateway).

The Spark context in the running job will already be initialized to use the provided lakeFS configuration.
There's no need to repeat the configuration steps mentioned in [Using lakeFS with Spark](spark.md#Configuration)    
{: .note}

