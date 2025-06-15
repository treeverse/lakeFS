---
title: AWS
description: How to deploy and set up a production-suitable lakeFS environment on AWS
next:  ["Import data into your installation", "../import/index.md"]
---

# Deploy lakeFS on AWS

!!! tip
    The instructions given here are for a self-managed deployment of lakeFS on AWS. <br/>
    For a hosted lakeFS service with guaranteed SLAs, try [lakeFS Cloud](https://lakefs.cloud)

When you deploy lakeFS on AWS these are the options available to use:

![](../../assets/img/deploy/deploy-on-aws.excalidraw.png)

This guide walks you through the options available and how to configure them, finishing with configuring and running lakeFS itself and creating your first repository.

!!! info "⏰ Expected deployment time: 25 min"

## Grant lakeFS permissions to DynamoDB

By default, lakeFS will create the required DynamoDB table if it does not already exist. You'll have to give the IAM role used by lakeFS the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListAndDescribe",
            "Effect": "Allow",
            "Action": [
                "dynamodb:List*",
                "dynamodb:DescribeReservedCapacity*",
                "dynamodb:DescribeLimits",
                "dynamodb:DescribeTimeToLive"
            ],
            "Resource": "*"
        },
        {
            "Sid": "kvstore",
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchGet*",
                "dynamodb:DescribeTable",
                "dynamodb:Get*",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:BatchWrite*",
                "dynamodb:CreateTable",
                "dynamodb:Delete*",
                "dynamodb:Update*",
                "dynamodb:PutItem"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/kvstore"
        }
    ]
}
```

!!! tip
    You can also use lakeFS with PostgreSQL instead of DynamoDB!<br/>
    See the [configuration reference](../../reference/configuration.md) for more information.

## Run the lakeFS server

=== "EC2"
    Connect to your EC2 instance using SSH:

    1. Create a `config.yaml` on your EC2 instance, with the following parameters:
        ```yaml
        ---
        database:
            type: "dynamodb"

        auth:
            encrypt:
            # replace this with a randomly-generated string. Make sure to keep it safe!
            secret_key: "[ENCRYPTION_SECRET_KEY]"

        blockstore:
            type: s3
        ```
    1. [Download the binary][downloads] to run on the EC2 instance.
    1. Run the `lakefs` binary on the EC2 instance:
        ```sh
        lakefs --config config.yaml run
        ```

        !!! note
            It's preferable to run the binary as a service using systemd or your operating system's facilities.

    !!! info "Advanced: Deploying lakeFS behind an AWS Application Load Balancer"
        1. Your security groups should allow the load balancer to access the lakeFS server.
        1. Create a target group with a listener for port 8000.
        1. Setup TLS termination using the domain names you wish to use (e.g., `lakefs.example.com` and potentially `s3.lakefs.example.com`, `*.s3.lakefs.example.com` if using [virtual-host addressing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)).
        1. Configure the health-check to use the exposed `/_health` URL

=== "EKS"
    You can install lakeFS on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).

    To install lakeFS with Helm:

    1. Copy the Helm values file relevant for S3:
        ```yaml
        secrets:
            # replace this with a randomly-generated string
            authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
        lakefsConfig: |
            database:
                type: dynamodb
            blockstore:
                type: s3
        ```
    1. Fill in the missing values and save the file as `conf-values.yaml`. For more configuration options, see our Helm chart [README](https://github.com/treeverse/charts/blob/master/charts/lakefs/README.md#custom-configuration).
        !!! note
            The `lakefsConfig` parameter is the lakeFS configuration documented [here](../../reference/configuration.md) but without sensitive information.
            Sensitive information like `databaseConnectionString` is given through separate parameters, and the chart will inject it into Kubernetes secrets.
    1. In the directory where you created `conf-values.yaml`, run the following commands:
        ```bash
        # Add the lakeFS repository
        helm repo add lakefs https://charts.lakefs.io
        # Deploy lakeFS
        helm install my-lakefs lakefs/lakefs -f conf-values.yaml
        ```

        *my-lakefs* is the [Helm Release](https://helm.sh/docs/intro/using_helm/#three-big-concepts) name.

        !!! warning
            Make sure the Kubernetes nodes have access to all buckets/containers with which you intend to use with lakeFS.
            If you can't provide such access, configure lakeFS with an AWS key-pair.

### Load balancing

To configure a load balancer to direct requests to the lakeFS servers you can use the `LoadBalancer` Service type or a Kubernetes Ingress.
By default, lakeFS operates on port 8000 and exposes a `/_health` endpoint that you can use for health checks.

!!! tip
    The NGINX Ingress Controller by default limits the client body size to 1 MiB.

    Some clients use bigger chunks to upload objects - for example, multipart upload to lakeFS using the [S3 Gateway][s3-gateway] or
    a simple PUT request using the [OpenAPI Server][openapi].

    Checkout the Nginx [documentation](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/annotations/#custom-max-body-size) for increasing the limit, or an example of Nginx configuration with [MinIO](https://docs.min.io/docs/setup-nginx-proxy-with-minio.html).

## Prepare your S3 bucket

1. Take note of the bucket name you want to use with lakeFS
2. Use the following as your bucket policy, filling in the placeholders:

=== "Standard Permissions"
    ```json
    {
        "Id": "lakeFSPolicy",
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "lakeFSObjects",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::[BUCKET_NAME_AND_PREFIX]/*"],
                "Principal": {
                    "AWS": ["arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"]
                }
            },
            {
                "Sid": "lakeFSBucket",
                "Action": [
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:ListBucketMultipartUploads"
                ],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::[BUCKET]"],
                "Principal": {
                    "AWS": ["arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"]
                }
            }
        ]
    }
    ```

    * Replace `[BUCKET_NAME]`, `[ACCOUNT_ID]` and `[IAM_ROLE]` with values relevant to your environment.
    * `[BUCKET_NAME_AND_PREFIX]` can be the bucket name. If you want to minimize the bucket policy permissions, use the bucket name together with a prefix (e.g. `example-bucket/a/b/c`).
    This way, lakeFS will be able to create repositories only under this specific path (see: [Storage Namespace][understand-repository]).
    * lakeFS will try to assume the role `[IAM_ROLE]`.

=== "Standard Permissions (with s3express)"

    To use an S3 Express One Zone _directory bucket_, use the following policy. Note the `lakeFSDirectoryBucket` statement which is specifically required for using a directory bucket.

    ```json
    {
        "Id": "lakeFSPolicy",
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "lakeFSObjects",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts"
                ],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::[BUCKET_NAME_AND_PREFIX]/*"],
                "Principal": {
                    "AWS": ["arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"]
                }
            },
            {
                "Sid": "lakeFSBucket",
                "Action": [
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:ListBucketMultipartUploads"
                ],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::[BUCKET]"],
                "Principal": {
                    "AWS": ["arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"]
                }
            },
            {
                "Sid": "lakeFSDirectoryBucket",
                "Action": [
                    "s3express:CreateSession"
                ],
                "Effect": "Allow",
                "Resource": "arn:aws:s3express:[REGION]:[ACCOUNT_ID]:bucket/[BUCKET_NAME]"
            }
        ]
    }
    ```

    * Replace `[BUCKET_NAME]`, `[ACCOUNT_ID]` and `[IAM_ROLE]` with values relevant to your environment.
    * `[BUCKET_NAME_AND_PREFIX]` can be the bucket name. If you want to minimize the bucket policy permissions, use the bucket name together with a prefix (e.g. `example-bucket/a/b/c`).
    This way, lakeFS will be able to create repositories only under this specific path (see: [Storage Namespace][understand-repository]).
    * lakeFS will try to assume the role `[IAM_ROLE]`.

=== "Minimal Permissions (Advanced)"
    If required lakeFS can operate without accessing the data itself, this permission section is useful if you are using [presigned URLs mode][presigned-url] or the [lakeFS Hadoop FileSystem Spark integration][integration-hadoopfs].
    Since this FileSystem performs many operations directly on the storage, lakeFS requires less permissive permissions, resulting in increased security.

    lakeFS always requires permissions to access the `_lakefs` prefix under your storage namespace, in which metadata
    is stored ([learn more][understand-commits]).

    By setting this policy **without** presign mode you'll be able to perform only metadata operations through lakeFS, meaning that you'll **not** be able
    to use lakeFS to upload or download objects. Specifically you won't be able to:

    * Upload objects using the lakeFS GUI (**Works with presign mode**)
    * Upload objects through Spark using the S3 gateway
    * Run `lakectl fs` commands (unless using **presign mode** with `--pre-sign` flag)
    * Use [Actions and Hooks](../hooks/index.md)

    ```json
    {
        "Id": "[POLICY_ID]",
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "lakeFSObjects",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:s3:::[STORAGE_NAMESPACE]/_lakefs/*"
                ],
                "Principal": {
                    "AWS": ["arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"]
                }
            },
            {
                "Sid": "lakeFSBucket",
                "Action": [
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::[BUCKET]"],
                "Principal": {
                    "AWS": ["arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"]
                }
            }
        ]
    }
    ```

    We can use [presigned URLs mode][presigned-url] without allowing access to the data from the lakeFS server directly.
    We can achieve this by using [condition keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html) such as [aws:referer](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-referer), [aws:SourceVpc](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-sourcevpc) and [aws:SourceIp](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html#condition-keys-sourceip).

    !!! example "For example, assume the following scenario"
        - lakeFS is deployed outside the company (i.e lakeFS cloud or other VPC **not** `vpc-123`)
        - We don't want lakeFS to be able to access the data, so we use presign URL, we still need lakeFS role to be able to sign the URL.
        - We want to allow access from the internal company VPC: `vpc-123`.

        ```json
        {
            "Sid": "allowLakeFSRoleFromCompanyOnly",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::[ACCOUNT_ID]:role/[IAM_ROLE]"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
            ],
            "Resource": [
                "arn:aws:s3:::[BUCKET]/*",
            ],
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "vpc-123"
                }
            }
        }
        ```

#### S3 Storage Tier Classes

lakeFS currently supports the following S3 Storage Classes:

1. [S3 Standard](https://aws.amazon.com/s3/storage-classes/#General_purpose) - The default AWS S3 storage tier. Fully supported.
2. [S3 Express One-Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) - Fully supported.
3. [S3 Glacier Instant Retrival](https://aws.amazon.com/s3/storage-classes/glacier/instant-retrieval/) - Supported with limitations: currently, pre-signed URLs are not supported when using Instant Retrival. The outstanding feature request [could be tracked here](https://github.com/treeverse/lakeFS/issues/7784).

Other storage classes are currently unsupported - either because they have not been tested with lakeFS or because they cannot be supported.

If you need lakeFS to support a storage tier that isn't currently on the supported list, please [open an issue on GitHub](https://github.com/treeverse/lakeFS/issues).

### Alternative: use an AWS user

lakeFS can authenticate with your AWS account using an AWS user, using an access key and secret. To allow this, change the policy's Principal accordingly:

```json
 "Principal": {
   "AWS": ["arn:aws:iam::<ACCOUNT_ID>:user/<IAM_USER>"]
 }
```

## Create the admin user

When you first open the lakeFS UI, you will be asked to create an initial admin user.

1. Open `http://<lakefs-host>/` in your browser. If you haven't set up a load balancer, this will likely be `http://<instance ip address>:8000/`
1. On first use, you'll be redirected to the setup page:
   <img src="../../../assets/img/setup.png" alt="Create user">
1. Follow the steps to create an initial administrator user. Save the credentials you’ve received somewhere safe, you won’t be able to see them again!
   <img src="../../../assets/img/setup_done.png" alt="Setup Done">
1. Follow the link and go to the login screen. Use the credentials from the previous step to log in.

## Create your first repository

1. Use the credentials from the previous step to log in
1. Click *Create Repository* and choose *Blank Repository*.
   <img src="../../../assets/img/create-repo-no-sn.png" alt="Create Repo"/>
1. Under Storage Namespace, enter a path to your desired location on the object store. This is where data written to this repository will be stored.
1. Click *Create Repository*
1. You should now have a configured repository, ready to use!
   <img src="../../../assets/img/repo-created.png" alt="Repo Created" style="border: 1px solid #DDDDDD;"/>

!!! success "Congratulations"
    Your environment is now ready 🤩

[integration-hadoopfs]:  ../../integrations/spark.md#lakefs-hadoop-filesystem
[presigned-url]:  ../../security/presigned-url.md
