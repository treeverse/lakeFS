---
layout: default
title: Deploying on AWS
parent: Deployment
nav_order: 0
has_children: false
---

# Deploying on AWS
{: .no_toc }

**Warning:** lakeFS is currently in POC - this is a pre-alpha version that *should not be used in production*. The API and data model are likely to change.
{: .note .pb-3 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}


### Running PostgreSQL
##### On RDS

1. Follow the official AWS documentation on [Creating a PostgreSQL DB Instance and Connecting to a Database on a PostgreSQL DB Instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_GettingStarted.CreatingConnecting.PostgreSQL.html){: target="_blank" }.  
You may use the default PostgreSQL engine, or [Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraPostgreSQL.html){: target="_blank" }. Make sure you're using PostgreSQL version >= 11.
2. Once your RDS is set up and the server is in `Available` state, and take note of the endpoint and port.

   ![RDS Connection String](../assets/img/rds_conn.png)

3. Make sure your security group rules allow you to connect to the database instance. 


##### Creating an initial Database

1. Use psql or any other [PostgreSQL client](https://wiki.postgresql.org/wiki/PostgreSQL_Clients){: target="_blank" } available for your platform.
2. Once connected, create a database to be used by your lakeFS installation:
   
      ```sql
      CREATE DATABASE lakefsdb LC_COLLATE='C' TEMPLATE template0;
      ```

### Setting up an S3 bucket for data storage

1. From the S3 Administration console, choose `Create Bucket`.
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

### Creating a configuration file

See the full [configuration reference](../reference/configuration.md){: target="_blank" } for all configurable settings.
{: .note .note-info }

A minimal example of a conifugration file for the setup we've created above would look like this:

```yaml
---
metadata:
  db:
    uri: "postgres://user:pass@<RDS_ENDPOINT>:5432/lakefsdb?search_path=lakefs_index"

auth:
  db:
    uri: "postgres://user:pass@<RDS_ENDPOINT>:5432/lakefsdb?search_path=lakefs_auth"
  encrypt:
    secret_key: "<RANDOM_GENERATED_STRING>"

blockstore:
  type: s3

gateways:
  s3:
    domain_name: s3.lakefs.example.com
    listen_address: 0.0.0.0:8000
```

Make sure to:

1. Change `<RDS_ENDPOINT>` to the endpoint we've created in the [Creating an initial Database](#creating-an-initial-database) step above.
2. Change `<RANDOM_GENERATED_STRING>` to a cryptographically safe, randomly generated string. Example how:
    
   ```bash
   # On *NIX systems
   $ LC_ALL=C tr -dc '[:alnum:]' < /dev/urandom | head -c128
   ```

   **Note:** It is best to keep this somewhere safe such as KMS or Hashicorp Vault, and provide it to the system at run time
   {: .note } 
   
3. Change `s3.lakefs.example.com` to a domain we'll configure for lakeFS' [S3 Gateway](../architecture.md#s3-gateway).  
See below on how to configure a load balancer to forward requests to our lakeFS instance on the port listed under `listen_port`. 

### Running lakeFS
##### Option #1: Using Docker (Fargate, ECS or EC2)

Depending on your runtime enviroment, running lakeFS using docker would look like this:

```sh
$ docker run \
    --name lakefs \
    -p 8000:8000 \
    -p 8001:8001 \
    -v <PATH_TO_CONFIG_FILE>:/home
```

##### Option #2: On a Linux EC2 server

###### Downloading and running the lakefs binary

###### Load balancing with Amazon Application Load Balancer

###### Setting up DNS names for the OpenAPI Server and the S3 Gateway

##### Option #3: On Kubernetes



