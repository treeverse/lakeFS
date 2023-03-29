---
layout: default
title: AWS
parent: Deploy and Setup lakeFS
description: This section will guide you through deploying and setting up a production-suitable lakeFS environment on AWS
nav_order: 10
redirect_from:
   - ../deploying-aws/index.html
   - ../deploying-aws/install.html
   - ../deploying-aws/db.html
   - ../deploying-aws/lb_dns.html
   - ../setup/storage/s3.html 
next:  ["Import data into your installation", "../howto/import.html"]
---

# Deploy lakeFS on AWS
{: .no_toc }


⏰ Expected deployment time: 25 min
{: .note }

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

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

💡 You can also use lakeFS with PostgreSQL instead of DynamoDB! See the [configuration reference](../reference/configuration.md) for more information.
{: .note }

## Run the lakeFS server

<div class="tabs">
  <ul>
    <li><a href="#ec2">EC2</a></li>
    <li><a href="#eks">EKS</a></li>
  </ul>
  <div markdown="1" id="ec2">
  
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
1. [Download the binary](../index.md#downloads) to the EC2 instance.
1. Run the `lakefs` binary on the EC2 instance:
  
   ```sh
   lakefs --config config.yaml run
   ```

**Note:** It's preferable to run the binary as a service using systemd or your operating system's facilities.
{: .note }

### Advanced: Deploying lakeFS behind an AWS Application Load Balancer
{: .no_toc }

1. Your security groups should allow the load balancer to access the lakeFS server.
1. Create a target group with a listener for port 8000.
1. Setup TLS termination using the domain names you wish to use (e.g., `lakefs.example.com` and potentially `s3.lakefs.example.com`, `*.s3.lakefs.example.com` if using [virtual-host addressing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)).
1. Configure the health-check to use the exposed `/_health` URL

  </div>
  <div markdown="1" id="eks">
  
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
1. Fill in the missing values and save the file as `conf-values.yaml`. For more configuration options, see our Helm chart [README](https://github.com/treeverse/charts/blob/master/charts/lakefs/README.md#custom-configuration){:target="_blank"}.

   The `lakefsConfig` parameter is the lakeFS configuration documented [here](https://docs.lakefs.io/reference/configuration.html) but without sensitive information.
   Sensitive information like `databaseConnectionString` is given through separate parameters, and the chart will inject it into Kubernetes secrets.
   {: .note }

1. In the directory where you created `conf-values.yaml`, run the following commands:

   ```bash
   # Add the lakeFS repository
   helm repo add lakefs https://charts.lakefs.io
   # Deploy lakeFS
   helm install my-lakefs lakefs/lakefs -f conf-values.yaml
   ```

   *my-lakefs* is the [Helm Release](https://helm.sh/docs/intro/using_helm/#three-big-concepts) name.

⚠️ Make sure the Kubernetes nodes have access to all buckets/containers with which you intend to use with lakeFS.
If you can't provide such access, configure lakeFS with an AWS key-pair.
{: .note .note-warning }

### Load balancing
{: .no_toc }

To configure a load balancer to direct requests to the lakeFS servers you can use the `LoadBalancer` Service type or a Kubernetes Ingress.
By default, lakeFS operates on port 8000 and exposes a `/_health` endpoint that you can use for health checks.

💡 The NGINX Ingress Controller by default limits the client body size to 1 MiB.
Some clients use bigger chunks to upload objects - for example, multipart upload to lakeFS using the [S3 Gateway](../understand/architecture.md#s3-gateway) or 
a simple PUT request using the [OpenAPI Server](../understand/architecture.md#openapi-server).
Checkout Nginx [documentation](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/annotations/#custom-max-body-size) for increasing the limit, or an example of Nginx configuration with [MinIO](https://docs.min.io/docs/setup-nginx-proxy-with-minio.html).
{: .note }


  </div>
</div>

## Prepare your S3 bucket

1. From the S3 Administration console, choose _Create Bucket_.
2. Use the following as your bucket policy, filling in the placeholders:
   <div class="tabs">
      <ul>
         <li><a href="#bucket-policy-standard">Standard Permissions</a></li>
         <li><a href="#bucket-policy-minimal">Minimal Permissions (Advanced)</a></li>
      </ul>
      <div markdown="1" id="bucket-policy-standard">
         
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
        This way, lakeFS will be able to create repositories only under this specific path (see: [Storage Namespace](../understand/model.md#repository)).
      * lakeFS will try to assume the role `[IAM_ROLE]`.
   </div>
   <div markdown="1" id="bucket-policy-minimal">
   This permission is useful if you are using the [lakeFS Hadoop FileSystem Spark integration](../integrations/spark.md#use-the-lakefs-hadoop-filesystem).
   Since this FileSystem performs many operations directly on the storage, lakeFS requires less permissive permissions, resulting in increased security.
   
   lakeFS always requires permissions to access the `_lakefs` prefix under your storage namespace, in which metadata
   is stored ([learn more](../understand/how/versioning-internals.md#constructing-a-consistent-view-of-the-keyspace-ie-a-commit)).  
   By setting this policy you'll be able to perform only metadata operations through lakeFS, meaning that you'll **not** be able
   to use lakeFS to upload or download objects. Specifically you won't be able to:
      * Upload objects using the lakeFS GUI
      * Upload objects through Spark using the S3 gateway
      * Run `lakectl fs` commands (unless using the `--direct` flag)
   
   
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

   </div>
   </div>

### Alternative: use an AWS user

lakeFS can authenticate with your AWS account using an AWS user, using an access key and secret. To allow this, change the policy's Principal accordingly:
```json
 "Principal": {
   "AWS": ["arn:aws:iam::<ACCOUNT_ID>:user/<IAM_USER>"]
 }
```

{% include_relative includes/setup.md %}
