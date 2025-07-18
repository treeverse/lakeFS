---
title: Amazon SageMaker
description: This section explains how to integrate your Amazon SageMaker installation to work with lakeFS.
---

# Using lakeFS with Amazon SageMaker 

[Amazon SageMaker](https://aws.amazon.com/sagemaker/) helps to prepare, build, train and deploy ML models quickly by bringing together a broad set of capabilities purpose-built for ML.

## Initializing session and client

Initialize a Sagemaker session and an S3 client with lakeFS as the endpoint:

```python
import sagemaker
import boto3

endpoint_url = '<LAKEFS_ENDPOINT>'
aws_access_key_id = '<LAKEFS_ACCESS_KEY_ID>'
aws_secret_access_key = '<LAKEFS_SECRET_ACCESS_KEY>'
repo = 'example-repo'

sm = boto3.client('sagemaker',
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

s3_resource = boto3.resource('s3',
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

session = sagemaker.Session(boto3.Session(), sagemaker_client=sm, default_bucket=repo)
session.s3_resource = s3_resource
```

## Usage Examples

### Upload train and test data

Let's use the created session for uploading data to the 'main' branch:

```python
prefix = "/prefix-within-branch"
branch = 'main'

train_file = 'train_data.csv';
train_data.to_csv(train_file, index=False, header=True)
train_data_s3_path = session.upload_data(path=train_file, key_prefix=branch + prefix + "/train")

test_file = 'test_data.csv';
test_data_no_target.to_csv(test_file, index=False, header=False)
test_data_s3_path = session.upload_data(path=test_file, key_prefix=branch + prefix + "/test")
```

### Download objects

You can use the integration with lakeFS to download a portion of the data you see fit:
 
```python
repo = 'example-repo'
prefix = "/prefix-to-download"
branch = 'main'
localpath = './' + branch

session.download_data(path=localpath, bucket=repo, key_prefix = branch + prefix)
```

!!! note
    Advanced AWS SageMaker features, like Autopilot jobs, are encapsulated and don't have the option to override the S3 endpoint.
    However, it is possible to [export](../howto/export.md) the required inputs from lakeFS to S3.
    
    **If you're using SageMaker features that aren't supported by lakeFS, we'd love to [hear from you](https://lakefs.io/slack).**
