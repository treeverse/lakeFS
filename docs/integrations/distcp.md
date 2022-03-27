---
layout: default
title: Copying Data with DistCp
description: Apache Hadoop DistCp is a tool used for large inter/intra-cluster copying. You can easily use it with your lakeFS repositories.
parent: Integrations
nav_order: 5
has_children: false
redirect_from: ../using/distcp.html
---

# Copying Data to/from lakeFS with DistCp
{: .no_toc }

Apache Hadoop [DistCp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){:target="_blank" .button-clickable} (distributed copy) is a tool used for large inter/intra-cluster copying. You can easily use it with your lakeFS repositories.
{% include toc.html %}

**Note** 
In the following examples we set AWS credentials on the command line, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank" .button-clickable}. 
{: .note}


## Copying from lakeFS to lakeFS

You can use DistCP to copy between two different lakeFS repositories. Replace the access key pair with your lakeFS access key pair:

```bash
hadoop distcp \
  -Dfs.s3a.path.style.access=true \
  -Dfs.s3a.access.key="AKIAIOSFODNN7EXAMPLE" \
  -Dfs.s3a.secret.key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -Dfs.s3a.endpoint="https://lakefs.example.com" \
  "s3a://example-repo-1/main/example-file.parquet" \
  "s3a://example-repo-2/main/example-file.parquet"
```

val workDir = s"s3a://${repo}/${branch}/collection/shows"
val dataPath = s"$workDir/title.basics.parquet"

## Copying between S3 and lakeFS
In order to copy between an S3 bucket and lakeFS repository, use Hadoop's [per-bucket configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets_with_Per-Bucket_Configuration){:target="_blank" .button-clickable}.
In the following examples, replace the first access key pair with your lakeFS key pair, and the second one with your AWS IAM key pair:

### From S3 to lakeFs
```bash
hadoop distcp \
  -Dfs.s3a.path.style.access=true \
  -Dfs.s3a.bucket.example-repo.access.key="AKIAIOSFODNN7EXAMPLE" \
  -Dfs.s3a.bucket.example-repo.secret.key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -Dfs.s3a.bucket.example-repo.endpoint="https://lakefs.example.com" \
  -Dfs.s3a.bucket.example-bucket.access.key="AKIAIOSFODNN3EXAMPLE" \
  -Dfs.s3a.bucket.example-bucket.secret.key="wJalrXUtnFEMI/K3MDENG/bPxRfiCYEXAMPLEKEY" \
  "s3a://example-bucket/example-file.parquet" \
  "s3a://example-repo/main/example-file.parquet"
```

### From lakeFS to S3
```bash
hadoop distcp \
  -Dfs.s3a.path.style.access=true \
  -Dfs.s3a.bucket.example-repo.access.key="AKIAIOSFODNN7EXAMPLE" \
  -Dfs.s3a.bucket.example-repo.secret.key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -Dfs.s3a.bucket.example-repo.endpoint="https://lakefs.example.com" \
  -Dfs.s3a.bucket.example-bucket.access.key="AKIAIOSFODNN3EXAMPLE" \
  -Dfs.s3a.bucket.example-bucket.secret.key="wJalrXUtnFEMI/K3MDENG/bPxRfiCYEXAMPLEKEY" \
  "s3a://example-repo/main/myfile" \
  "s3a://example-bucket/myfile"
```
