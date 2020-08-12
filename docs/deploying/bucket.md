---
layout: default
title: Creating the S3 bucket
parent: AWS Deployment
nav_order: 15
has_children: false
---

# Creating the S3 bucket

We now create the S3 bucket, which will provide the data storage layer for our installation:

1. From the S3 Administration console, choose `Create Bucket` (you can use an existing bucket with a path prefix, but creating a dedicated bucket is recommended).
2. Make sure you:
    1. Block public access
    2. Disable Object Locking
3. Once created, go to the `Permissions` tab, and create a Bucket Policy. Use the following structure:

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
           "s3:DeleteObject",
           "s3:DeleteObjectVersion",
           "s3:AbortMultipartUpload",
           "s3:ListMultipartUploadParts"
         ],
         "Effect": "Allow",
         "Resource": [
           "arn:aws:s3:::<BUCKET_NAME>/*"
         ],
         "Principal": {
           "AWS": [
             "arn:aws:iam::<ACCOUNT_ID>:role/<IAM_ROLE>"
           ]
         }
       }, {
         "Sid": "Stmt1590051522178",
         "Action": [
           "s3:GetBucketVersioning",
           "s3:ListBucket",
           "s3:GetBucketLocation",
           "s3:ListBucketMultipartUploads",
           "s3:ListBucketVersions"
         ],
         "Effect": "Allow",
         "Resource": [
           "arn:aws:s3:::<BUCKET_NAME>"
         ],
         "Principal": {
           "AWS": [
             "arn:aws:iam::<ACCOUNT_ID>:role/<IAM_ROLE>"
           ]
         }
       }
     ]
   }
   ```
   
   Replace `<ACCOUNT_ID>`, `<IAM_ROLE>` and `<BUCKET_NAME>` with values relevant to your environment.
