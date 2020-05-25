---
layout: default
title: Copying Data with DistCp
parent: Using lakeFS with...
nav_order: 0
has_children: false
---

# Copying Data to/from lakeFS with DistCp
{: .no_toc }

Apache Hadoop [DistCp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){:target="_blank"} (distributed copy) is a tool used for large inter/intra-cluster copying. You can easily use it with your lakeFS repositories.
## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

**Note** 
In the following examples we set AWS credentials on the command line, for clarity. In production, these properties should be set using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
{: .note}


## Copying from lakeFS to lakeFS

You can use DistCP to copy between two different lakeFS repositories. Replace the access key pair with your lakeFS access key pair:

```bash
hadoop distcp \
  -Dfs.s3a.access.key="AKIAJF2VSETNW3RTP3ZQ" \
  -Dfs.s3a.secret.key="pQRw1MEPspmZeng5XEXMSvKiPxxQBdXbziXtVjq2" \
  -Dfs.s3a.endpoint="http://s3.local.lakefs.io:8000" \
  "s3a://my-first-repo/branch1/myfile" \
  "s3a://my-second-repo/branch2/myfile"
```


## Copying between S3 and lakeFS
In order to copy between an S3 bucket and lakeFS repository, use Hadoop's [per-bucket configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets_with_Per-Bucket_Configuration){:target="_blank"}.
In the following examples, replace the first access key pair with your lakeFS key pair, and the second one with your AWS IAM key pair:

### From S3 to lakeFs
```bash
hadoop distcp \
  -Dfs.s3a.my-lakefs-repo.access.key="AKIAJF2VSETNW3RTP3ZQ" \
  -Dfs.s3a.my-lakefs-repo.secret.key="pQRw1MEPspmZeng5XEXMSvKiPxxQBdXbziXtVjq2" \
  -Dfs.s3a.my-lakefs-repo.endpoint="http://s3.local.lakefs.io:8000" \
  -Dfs.s3a.my-s3-bucket.access.key="AKIAJXQPFV4FZH7UMMAQ" \
  -Dfs.s3a.my-s3-bucket.secret.key="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" \
  "s3a://my-s3-bucket/myfile" \
  "s3a://my-lakefs-repo/master/myfile"
```

### From lakeFS to S3
```bash
hadoop distcp \
  -Dfs.s3a.my-lakefs-repo.access.key="AKIAJF2VSETNW3RTP3ZQ" \
  -Dfs.s3a.my-lakefs-repo.secret.key="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" \
  -Dfs.s3a.my-lakefs-repo.endpoint="http://s3.local.lakefs.io:8000" \
  -Dfs.s3a.my-s3-bucket.access.key="AKIAJXQPFV4FZH7UMMAQ" \
  -Dfs.s3a.my-s3-bucket.secret.key="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" \
  "s3a://my-lakefs-repo/master/myfile" \
  "s3a://my-s3-bucket/myfile"
```
