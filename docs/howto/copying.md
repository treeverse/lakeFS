---
layout: default
title: Copying data to/from lakeFS
description: 
parent: How-To
nav_order: 50
has_children: false
redirect_from: 
  - /integrations/distcp.html
  - /integrations/rclone.html
---

{% include toc.html %}

## Using DistCp

Apache Hadoop [DistCp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){:target="_blank"} (distributed copy) is a tool used for large inter/intra-cluster copying. You can easily use it with your lakeFS repositories.

**Note** 

In the following examples, we set AWS credentials on the command line for clarity. In production, you should set these properties using one of Hadoop's standard ways of [Authenticating with S3](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3){:target="_blank"}. 
{: .note}

### Between lakeFS repositories

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

### Between S3 buckets and lakeFS

To copy data from an S3 bucket to a lakeFS repository, use Hadoop's [per-bucket configuration](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets_with_Per-Bucket_Configuration){:target="_blank"}.
In the following examples, replace the first access key pair with your lakeFS key pair, and the second one with your AWS IAM key pair:

#### From S3 to lakeFs

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

#### From lakeFS to S3

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

## Using Rclone

[Rclone](https://rclone.org/){:target="_blank"} is a command line program to sync files and directories between cloud providers.
To use it with lakeFS, create an Rclone remote as describe below and then use it as you would any other Rclone remote.

### Creating a remote for lakeFS in Rclone

To add the remote to Rclone, choose one of the following options:

#### Option 1: Add an entry in your Rclone configuration file
*   Find the path to your Rclone configuration file and copy it for the next step.

    ```shell
    rclone config file
    # output:
    # Configuration file is stored at:
    # /home/myuser/.config/rclone/rclone.conf
    ```

*   If your lakeFS access key is already set in an AWS profile or environment variables, run the following command, replacing the endpoint property with your lakeFS endpoint:

    ```shell
    cat <<EOT >> /home/myuser/.config/rclone/rclone.conf
    [lakefs]
    type = s3
    provider = AWS
    endpoint = https://lakefs.example.com
	no_check_bucket = true
    EOT
    ```

*   Otherwise, also include your lakeFS access key pair in the Rclone configuration file:

    ```shell
    cat <<EOT >> /home/myuser/.config/rclone/rclone.conf
    [lakefs]
    type = s3
    provider = AWS
    env_auth = false
    access_key_id = AKIAIOSFODNN7EXAMPLE
    secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    endpoint = https://lakefs.example.com
	no_check_bucket = true
    EOT
    ```
	
#### Option 2: Use the Rclone interactive config command

Run this command and follow the instructions:
```shell
rclone config
```
Choose AWS S3 as your type of storage, and enter your lakeFS endpoint as your S3 endpoint.
You will have to choose whether you use your environment for authentication (recommended),
or enter the lakeFS access key pair into the Rclone configuration. Select "Edit advanced
config" and accept defaults for all values except `no_check_bucket`:
```
If set, don't attempt to check the bucket exists or create it.

This can be useful when trying to minimize the number of transactions
Rclone carries out, if you know the bucket exists already.

This might also be needed if the user you're using doesn't have bucket
creation permissions. Before v1.52.0, this would have passed silently
due to a bug.

Enter a boolean value (true or false). Press Enter for the default ("false").
no_check_bucket> yes
```

### Syncing S3 and lakeFS

```shell
rclone sync mys3remote://mybucket/path/ lakefs:example-repo/main/path
```

### Syncing a local directory and lakeFS

```shell
rclone sync /home/myuser/path/ lakefs:example-repo/main/path
```
