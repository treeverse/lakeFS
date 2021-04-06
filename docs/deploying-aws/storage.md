---
layout: default
title: Configuring the Storage
description: Providing the data storage layer for our installation. 
parent: Production Deployment
nav_order: 15
has_children: false
---

# Configuring the Storage
{: .no_toc }

A production installation of lakeFS will usually use your cloud provider's object storage as the underlying storage layer.
You can choose to create a new bucket/container (recommended), or use an existing one with a path prefix. The path under the existing bucket/container should be empty.

After you have a bucket/container configured, proceed to [Installing lakeFS](./install.md).

Choose your cloud provider to configure your storage.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## AWS S3

1. From the S3 Administration console, choose `Create Bucket`.
2. Make sure you:
    1. Block public access
    2. Disable Object Locking
3. Go to the `Permissions` tab, and create a Bucket Policy. Use the following structure:

   ```json
    {
     "Id": "Policy1590051531320",
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "Stmt1590051522178",
         "Action": [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:PutObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
            "s3:GetBucketVersioning",
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "s3:ListBucketMultipartUploads",
            "s3:ListBucketVersions"
         ],
         "Effect": "Allow",
         "Resource": ["arn:aws:s3:::<BUCKET_NAME>", "arn:aws:s3:::<BUCKET_NAME_WITH_PATH_PREFIX>/*"],
         "Principal": {
           "AWS": ["arn:aws:iam::<ACCOUNT_ID>:role/<IAM_ROLE>"]
         }
       }
     ]
    }
   ```
   
   Replace `<ACCOUNT_ID>`, `<BUCKET_NAME>` and `<IAM_ROLE>` with values relevant to your environment.
   `IAM_ROLE` should be the role assumed by your lakeFS installation.
   
   Alternatively, if you use an AWS user's key-pair to authenticate lakeFS to AWS, change the policy's Principal to be the user: 
   
   ```json
    "Principal": {
      "AWS": ["arn:aws:iam::<ACCOUNT_ID>:user/<IAM_USER>"]
    }
   ```
You can now proceed to [Installing lakeFS](./install.md).   

## Microsoft Azure Blob Storage

[Create a container in Azure portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container)
1. From the Azure portal, Storage Accounts, choose your account, then in the container tab click `+ Container`.
2. Make sure you block public access

### Authenticate with Secret Key
{: .no_toc }

In case you want to use the secret key for authentication you will need to use the account key in the configuration
Go to the `Access Keys` tab and click on `Show Keys` save the values under `Storage account name` and `Key` we will need them in the [installing lakeFS](install.md) step
### Authenticate with Active Directory
{: .no_toc }

In case you want your lakeFS Installation (we will install in the next step) to access this Container using Active Directory authentication,
First go to the container you created in step 1.
* Go to `Access Control (IAM)`
* Go to the `Role assignments` tab
* Add the `Storage Blob Data Contributor` role to the Installation running lakeFS. 
  
You can now proceed to [Installing lakeFS](./install.md).

## Google Cloud Storage
1. On the Google Cloud Storage console, click *Create Bucket*. Follow the instructions.
   
1. On the *Permissions* tab, add the service account you intend to use lakeFS with. Give it a role that allows reading and writing to the bucket, e.g. *Storage Object Creator*.

You can now proceed to [Installing lakeFS](./install.md).