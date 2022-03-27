---
layout: default
title: SageMaker
description: This section covers how you can integrate your SageMaker installation to work with lakeFS.
parent: Integrations
nav_order: 80
has_children: false
redirect_from: ../using/sagemaker.html
---

# Using lakeFS with SageMaker 
{: .no_toc }
[Amazon SageMaker](https://aws.amazon.com/sagemaker/){: .button-clickable} helps prepare, build, train and deploy ML models quickly by bringing together a broad set of capabilities purpose-built for ML.

{% include toc.html %}

## Initializing session and client

Initialize a Sagemaker session and an s3 client with lakeFS as the endpoint:
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

We can use the integration with lakeFS to download a portion of the data we see fit:
 
```python
repo = 'example-repo'
prefix = "/prefix-to-download"
branch = 'main'
localpath = './' + branch

session.download_data(path=localpath, bucket=repo, key_prefix = branch + prefix)
```

**Note:**
Advanced AWS SageMaker features, like Autopilot jobs, are encapsulated and don't have the option to override the S3 endpoint.
However, it is possible to [export](../reference/export.md){: .button-clickable} the required inputs from lakeFS to S3.
<br/>If you're using SageMaker features that aren't supported by lakeFS, we'd love to [hear](https://join.slack.com/t/lakefs/shared_invite/zt-ks1fwp0w-bgD9PIekW86WF25nE_8_tw){: .button-clickable} from you.
{: .note}