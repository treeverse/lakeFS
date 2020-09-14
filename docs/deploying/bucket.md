---
layout: default
title: Configuring the S3 bucket
parent: AWS Deployment
nav_order: 15
has_children: false
---

# Configuring the S3 bucket

The S3 bucket will provide the data storage layer for our installation.
You can choose to create a new S3 bucket (recommended) or use an existing bucket with a path prefix.
The path under the existing bucket should be empty.

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
