---
title: Auditing
parent: Reference
description: Auditing is a solution for lakeFS Cloud which enables tracking of events and activities performed within the solution. These logs capture information such as who accessed the solution, what actions were taken, and when they occurred.
redirect_from: 
  - /audit.html
  - /reference/audit.html
  - /cloud/auditing.html
---

# Auditing
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

{: .note}
> Auditing is only available for [lakeFS Cloud]({% link understand/lakefs-cloud.md %}).

{: .warning }
> Please note, as of Jan 2024, the queryable interface within the lakeFS Cloud UI has been removed in favor of direct access to lakeFS audit logs. This document now describes how to set up and query this information using [AWS Glue](https://aws.amazon.com/glue/) as a reference.

The lakeFS audit log allows you to view all relevant user action information in a clear and organized table, including when the action was performed, by whom, and what it was they did. 

This can be useful for several purposes, including: 

1. Compliance - Audit logs can be used to show what data users accessed, as well as any changes they made to user management.

2. Troubleshooting - If something changes on your underlying object store that you weren't expecting, such as a big file suddenly breaking into thousands of smaller files, you can use the audit log to find out what action led to this change. 

## Setting up access to Audit Logs on AWS S3

The access to the Audit Logs is done via [AWS S3 Access Point](https://aws.amazon.com/s3/features/access-points/).

There are different ways to interact with an access point (see [Using access points in AWS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html)).

The initial setup:

1. Take note of the IAM Role ARN that will be used to access the data. This should be the user or role used by e.g. Athena.
1. [Reach out to customer success](mailto:support@treeverse.io?subject=ARN to use for audit logs) and provide this ARN. Once receiving the ARN role, an access point will be created and you should get in response the following details:
   1. S3 Bucket (e.g. `arn:aws:s3:::lakefs-audit-logs-us-east-1-production`)
   2. S3 URI to an access point (e.g. `s3://arn:aws:s3:us-east-1:<treeverse-id>:accesspoint/lakefs-logs-<organization>`)
   3. Access Point alias. You can use this alias instead of the bucket name or Access Point ARN to access data through the Access Point. (e.g. `lakefs-logs-<generated>-s3alias`)
3. Update your IAM Role policy and trust policy if required

A minimal example for IAM policy with 2 lakeFS installations in 2 regions (`us-east-1`, `us-west-2`):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::lakefs-audit-logs-us-east-1-production",
                "arn:aws:s3:::lakefs-audit-logs-us-east-1-production/*",
                "arn:aws:s3:::lakefs-logs-<generated>-s3alias/*",
                "arn:aws:s3:us-east-1:<treeverse-id>:accesspoint/lakefs-logs-<organization>",
                "arn:aws:s3:us-east-1:<treeverse-id>:accesspoint/lakefs-logs-<organization>/*"
            ],
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "etl/v1/data/region=<region_a>/organization=org-<organization>/*",
                        "etl/v1/data/region=<region_b>/organization=org-<organization>/*"
                    ]
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::lakefs-audit-logs-us-east-1-production",
                "arn:aws:s3:::lakefs-audit-logs-us-east-1-production/etl/v1/data/region=<region_a>/organization=org-<organization>/*",
                "arn:aws:s3:::lakefs-audit-logs-us-east-1-production/etl/v1/data/region=<region_b>/organization=org-<organization>/*",
                "arn:aws:s3:::lakefs-logs-<generated>-s3alias/*",
                "arn:aws:s3:us-east-1:<treeverse-id>:accesspoint/lakefs-logs-<organization>/object/etl/v1/data/region=<region_a>/organization=org-<organization>/*",
                "arn:aws:s3:us-east-1:<treeverse-id>:accesspoint/lakefs-logs-<organization>/object/etl/v1/data/region=<region_b>/organization=org-<organization>/*"
            ]
        },
        {
            "Action": [
                "kms:Decrypt"
            ],
            "Resource": [
                "arn:aws:kms:us-east-1:<treeverse-id>:key/<encryption-key-id>"
            ],
            "Effect": "Allow"
        }
    ]
}
```

Trust Policy example that allows anyone in your account to assume the role above:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<YOUR_ACCOUNT_ID>:root"
            },
            "Action": "sts:AssumeRole",
            "Condition": {}
        }
    ]
}
```


Authentication is done by assuming an IAM Role:

```bash
# Assume role use AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN:
aws sts assume-role --role-arn arn:aws:iam::<your-aws-account>:role/<reader-role> --role-session-name <name> 

# verify role assumed
aws sts get-caller-identity 

# list objects (can be used with --recursive) with access point ARN
aws s3 ls arn:aws:s3:us-east-1:<treeverse-id>:accesspoint/lakefs-logs-<organization>/etl/v1/data/region=<region>/organization=org-<organization>/

# get object locally via s3 access point alias 
aws s3api get-object --bucket lakefs-logs-<generated>-s3alias --key lakefs-audit-logs-us-east-1-production/etl/v1/data/region=<region>/organization=org-<organization>/year=<YY>/month=<MM>/day=<DD>/hour=<HH>/<file>-snappy.parquet sample.parquet 
```

## Data layout

{: .note }
> The bucket name is important when creating the IAM policy but, the Access Point ARN and Alias will be the ones that are used to access the data (i.e AWS CLI, Spark etc).

**Bucket Name:** `lakefs-audit-logs-us-east-1-production`

**Root prefix:** `etl/v1/data/region=<region>/organization=org-<organization-name>/`

**Files Path pattern:** All the audit logs files are in parquet format and their pattern is: `etl/v1/data/region=<region>/organization=org-<organization-name>/year=<YY>/month=<MM>/day=<DD>/hour=<HH>/*-snappy.parquet`

### Path Values

**region:** lakeFS installation region (e.g the region in lakeFS URL: https://<organization-name>.<region>.lakefscloud.io/)

**organization:** Found in the lakeFS URL `https://<organization-name>.<region>.lakefscloud.io/`. The value in the S3 path must be prefixed with `org-<organization-name>`

### Partitions

- year
- month
- day
- hour

### Example

As an example paths for "Acme" organization with 2 lakeFS installations:

```text
# ACME in us-east-1 
etl/v1/data/region=us-east-1/organization=org-acme/year=2024/month=02/day=12/hour=13/log_abc-snappy.parquet

# ACME in us-west-2 
etl/v1/data/region=us-west-2/organization=org-acme/year=2024/month=02/day=12/hour=13/log_xyz-snappy.parquet
```

## Schema

The files are in parquet format and can be accessed directly from Spark or any client that can read parquet files.
Using Spark's [`printSchema()`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.printSchema.html) we can inspect the values, that’s the latest schema with comments on important columns:

| column              | type   | description                                                                                                                                                                                                                         |
|---------------------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `data_user`         | string | the internal user ID for the user making the request. if using an external IdP (i.e SSO, Microsoft Entra, etc) it will be the UID represented by the IdP. (see below an  example how to extract the info of external IDs in python) |
| `data_repository`   | string | the repository ID relevant for this request. Currently only returned for s3_gateway requests                                                                                                                                        |
| `data_ref`          | string | the reference ID (tag, branch, ...) relevant for this request. Currently only returned for s3_gateway requests                                                                                                                      |
| `data_status_code`  | int    | HTTP status code returned for this request                                                                                                                                                                                          |
| `data_service_name` | string | Service name for the request. Could be either "rest_api" or "s3_gateway"                                                                                                                                                            |
| `data_request_id`   | string | Unique ID representing this request                                                                                                                                                                                                 |
| `data_path`         | string | HTTP path used for this request                                                                                                                                                                                                     |
| `data_operation_id` | string | Logical operation ID for this request. E.g. `list_objects`, `delete_repository`, ...                                                                                                                                                |
| `data_method`       | string | HTTP method for the request                                                                                                                                                                                                         |
| `data_time`         | string | datetime representing the start time of this request, in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format                                                                                                                                                        |


## IdP users: map user IDs from audit logs to an email in lakeFS

The `data_user` column in each log represents the user id that performed it.

* It might be empty in cases where authentication is not required (e.g login attempt).
* If the user is an API user created internally in lakeFS that id is also the name it was given.
* `data_user` might contain an ID to an external IdP (i.e. SSO system), usually it is not human friendly, we can correlate the ID to a lakeFS email used, see an example using the [Python lakefs-sdk](../integrations/python.md#using-the-lakefs-sdk).


```python
import lakefs_sdk

# Configure HTTP basic authorization: basic_auth
configuration = lakefs_sdk.Configuration(
    host = "https://<org>.<region>.lakefscloud.io/api/v1",
    username = 'AKIA...',
    password = '...'
)

# Print all user email and uid in lakeFS 
# the uid is equal to the user id in the audit logs.
with lakefs_sdk.ApiClient(configuration) as api_client:
    auth_api = lakefs_sdk.AuthApi(api_client)
    has_more = True
    next_offset = ''
    page_size = 100 
    while has_more: 
        resp = auth_api.list_users(prefix='', after=next_offset, amount=page_size)
        for u in resp.results:
            email = u.email
            uid = u.id
            print(f'Email: {email}, UID: {uid}')

        has_more = resp.pagination.has_more 
        next_offset = resp.pagination.next_offset
```

## Example: Glue Notebook with Spark

```python
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# connect to s3 access point 
alias = 's3://<bucket-alias-name>'
s3_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [alias + "/etl/v1/data/region=<region>/organization=org-<org>/year=<YY>/month=<MM>/day=<DD>/hour=<HH>/"],
        "recurse": True,
    },
    transformation_ctx="sample-ctx",
)

s3_dyf.show()
s3_dyf.printSchema()
```
