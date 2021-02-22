---
layout: default
title: Migrating away from lakeFS
description: The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
parent: AWS Deployment
nav_order: 40
has_children: false
---

# Migrating away from lakeFS
{: .no_toc }

{:.pb-3 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Copying data from a lakeFS repository to an S3 bucket

The simplest way to migrate away from lakeFS is to copy data from a lakeFS repository to an S3 bucket
(or any other object store).

For smaller repositories, this could be done using the [AWS cli](../using/aws_cli.md) or [rclone](../using/rclone.md).
For larger repositories, running [distcp](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html){: target="_blank"} with lakeFS as the source is also an option.


## Using treeverse-distcp

If for some reason, lakeFS is not accessible, we can still migrate data to S3 using [treeverse-distcp](https://github.com/treeverse/treeverse-distcp){: target="_blank"} -
assuming the underlying S3 bucket is intact. Here's how to do it:

1. Create a Copy Manifest - this file describes the source and destination for every object we want to copy. It is a mapping between lakeFS' internal storage addressing and the paths of the objects as we'd expect to see them in S3.
   
   To generate a manifest, connect to the PostgreSQL instance used by lakeFS and run the following command:
   
   ```shell
   psql \
     --var "repository_name=repo1" \
     --var "branch_name=master" \
     --var "dst_bucket_name=bucket1" \
     postgres < create-extraction-manifest.sql > manifest.csv
   ```
   
   You can download the `create-extraction-manifest.sql` script from the [lakeFS GitHub repository](https://github.com/treeverse/lakeFS/blob/master/scripts/create-extraction-manifest.sql){: target="_blank" }.
   
   **Note** This manifest is useful for recovery - it will allow you to restore service in case the PostgreSQL database is for some reason not accessible.
   For safety, you can automate the creation of this manifest to happen daily. 
   {: .note .note-info }
1. Copy the manifest to S3. Once copied, keep note of its etag - we'll need this to run the copy batch job:
   
   ```shell
   cp /path/to/manifest.csv s3://my-bucket/path/to/manifest.csv
   aws s3api head-object --bucket my-bucket --key path/to-manifest/csv | jq -r .ETag # Or look for ETag in the output
   ```
1. Once we have a manifest, let's define a S3 batch job that will copy all files for us.
To do this, let's start by creating an IAM role called `lakeFSExportJobRole`, and grant it permissions as described in ["Granting permissions for Batch Operations"](https://docs.aws.amazon.com/AmazonS3/latest/dev/batch-ops-iam-role-policies.html#batch-ops-iam-role-policies-create){: target="_blank" }
1. Once we have an IAM role, let's install the [`treeverse-distcp` lambda function](https://github.com/treeverse/treeverse-distcp/blob/master/lambda_handler.py){: target="_blank" }

   Make a note of the Lambda function ARN -- this is required for running an S3 Batch Job.
1. Take note of your account ID - this is required for running an S3 Batch Job:
   
   ```shell
   aws sts get-caller-identity | jq -r .Account
   ```
1. Dispatch a copy job using the [`run_copy.py`](https://github.com/treeverse/treeverse-distcp/blob/master/run_copy.py){: target="_blank" } script:
   
   ```shell
   run_copy.py \
     --account-id "123456789" \
     --csv-path "s3://s3://my-bucket/path/to/manifest" \
     --csv-etag "..." \
     --report-path "s3://another-bucket/prefix/for/reports" \
     --lambda-handler-arn "arn:lambda:..."
   ```
1. You will get a job number. Now go to the [AWS S3 Batch Operations Console](https://s3.console.aws.amazon.com/s3/jobs){: target="_blank" }, switch to the region of your bucket, and confirm execution of that job.
