---
title: Dremio
description: This section shows how you can start using lakeFS with Dremio, a next-generation data lake engine.
---

# Using lakeFS with Dremio

[Dremio](https://www.dremio.com/) is a next-generation data lake engine that liberates your data with live, 
interactive queries directly on cloud data lake storage, including S3 and lakeFS.


## Iceberg REST Catalog

lakeFS Iceberg REST Catalog allow you to use lakeFS as a [spec-compliant](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) Apache [Iceberg REST catalog](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml),
allowing Dremio to manage and access tables using a standard REST API.

![lakeFS Iceberg REST Catalog](../assets/img/lakefs_iceberg_rest_catalog.png)

This is the recommended way to use lakeFS with Dremio, as it allows lakeFS to stay completely outside the data path: data itself is read and written by Dremio executors, directly to the underlying object store. Metadata is managed by Iceberg at the table level, while lakeFS keeps track of new snapshots to provide versioning and isolation.

[Read more about using the Iceberg REST Catalog](./iceberg.md#iceberg-rest-catalog).

### Configuration

To configure Dremio to work with the Iceberg REST Catalog, you need to configure the [Iceberg REST Catalog in Dremio](https://docs.dremio.com/current/data-sources/lakehouse-catalogs/iceberg-rest-catalog/).

1. On the Datasets page, to the right of **Sources** in the left panel, click `+`
1. In the **Add Data Source** dialog, under Lakehouse Catalogs, select **Iceberg REST Catalog** Source. The New Iceberg REST Catalog Source dialog box appears, which contains the following tabs:
       1. In **General** →
           - Enter a name for your Iceberg REST Catalog source, specify the endpoint URI (i.e. `https://lakefs.example.com/iceberg/api`)
           - Uncheck "Use vended credentials"
       1. In **Advanced Options** → Catalog Properties, add the following key-value pairs (left = key, right = value):
        
          | Key                               | Value                                                    | Notes                                                   |
          | --------------------------------- | -------------------------------------------------------- |---------------------------------------------------------|
          | `oauth2-server-uri`               | `https://lakefs.example.com/iceberg/api/v1/oauth/tokens` | Your lakeFS OAuth2 token endpoint (not the catalog URL). |
          | `credential`                      | `<lakefs_access_key>:<lakefs_secret_key>`                | Your lakeFS credentials.      |
          | `fs.s3a.aws.credentials.provider` | `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`  | Use static AWS credentials.                             |
          | `fs.s3a.access.key`               | `<aws_access_key_id>`                                    | AWS key with read/write access to your data bucket.     |
          | `fs.s3a.secret.key`               | `<aws_secret_access_key>`                                | AWS secret key.                                         |
          | `dremio.s3.list.all.buckets`      | `false`                                                  | Avoid listing all buckets during initialization.        |

1. Click **Save** to create the Iceberg REST Catalog source.

#### Data Bucket Permissions 

The lakeFS Iceberg Catalog manages table metadata, while Dremio reads and writes data files directly from your underlying 
storage (for example, Amazon S3).

You must ensure that the IAM role or user Dremio uses has read/write access to your data bucket.
The following AWS IAM policy provides the required permissions for direct access: 

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DremioIcebergAccess",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::<lakefs_repo_storage_namespace>/_managed/", 
                "arn:aws:s3:::<lakefs_repo_storage_namespace>/_managed/*"
            ]
        },
        {
            "Sid": "BucketLevelRequiredForDremio",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::<lakefs_repo_storage_namespace_bucket_name>"
        }
    ]
}
```

!!! tip
    To learn more about the Iceberg REST Catalog, see the [Iceberg REST Catalog](./iceberg.md#iceberg-rest-catalog) documentation.

## Using Dremio with the S3 Gateway

Alternatively, you can use the S3 Gateway to read and write data to lakeFS from Dremio.

While flexible, this approach requires lakeFS to be involved in the data path, which can be less efficient than the Iceberg 
REST Catalog approach, since lakeFS has to proxy all data operations through the lakeFS server. This is particularly true 
for large data sets where network bandwidth might incur some overhead.

### Configuration

Starting from version 3.2.3, Dremio supports Minio as an [experimental S3-compatible plugin](https://docs.dremio.com/current/sonar/data-sources/object/s3/#configuring-s3-for-minio).
Similarly, you can connect lakeFS with Dremio.

Suppose you already have both lakeFS and Dremio deployed, and want to use Dremio to query your data in the lakeFS repositories.
You can follow the steps listed below to configure on Dremio UI:

1. click **Add Data Lake**.
1. Under **File Stores**, choose **Amazon S3**.
1. Under **Advanced Options**, check **Enable compatibility mode (experimental)**.
1. Under **Advanced Options** > **Connection Properties**, add `fs.s3a.path.style.access` and set the value to `true`.
1. Under **Advanced Options** > **Connection Properties**, add `fs.s3a.endpoint` and set lakeFS S3 endpoint to the value. 
1. Under the **General** tab, specify the **access_key_id** and **secret_access_key** provided by lakeFS server.
1. Click **Save**, and now you should be able to browse lakeFS repositories on Dremio.
