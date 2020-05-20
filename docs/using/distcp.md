---
layout: default
title: Copying Data with DistCp
parent: Using lakeFS with...
nav_order: 0
has_children: false
---

# Copying Data to/from lakeFS with DistCp
Apache Hadoop [DistCp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){:target="_blank"} (distributed copy) is a tool used for large inter/intra-cluster copying. You can easily use it with your lakeFS repositories.

## Copying from lakeFS to lakeFS
In the following examples, replace the access key pair with your lakeFS access key pair.

Copy between branches in the same lakeFS repository:
```bash
hadoop distcp \
  -Dfs.s3a.access.key="AKIAJF2VSETNW3RTP3ZQ" \
  -Dfs.s3a.secret.key="pQRw1MEPspmZeng5XEXMSvKiPxxQBdXbziXtVjq2" \
  -Dfs.s3a.endpoint="http://s3.local.lakefs.io:8000" \
  "s3a://my-lakefs-repo/master/myfile" \
  "s3a://my-lakefs-repo/my-branch/myfile"
```

Or, copy between two different lakeFS repositories:
```bash
hadoop distcp \
  -Dfs.s3a.access.key="AKIAJF2VSETNW3RTP3ZQ" \
  -Dfs.s3a.secret.key="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" \
  -Dfs.s3a.endpoint="http://s3.local.lakefs.io:8000" \
  "s3a://my-first-repo/branch1/myfile" \
  "s3a://my-second-repo/branch2/myfile"
```
## Copying from S3 to lakeFS
In order to copy from S3 to a lakeFS repository, use Hadoop's [per-bucket configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets_with_Per-Bucket_Configuration){:target="_blank"}.
Replace the first access key pair with your lakeFS key pair, and the second one with your AWS IAM key pair:

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
