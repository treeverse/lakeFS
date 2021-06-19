---
layout: default
title: On GCP
parent: Deploy lakeFS
description: This guide will help you deploy your production lakeFS environment on GCP
nav_order: 30
---

# On GCP

{: .no\_toc } Expected deployment time: 25min

## Table of contents

{: .no\_toc .text-delta }

1. TOC 

   {:toc}

## Creating the Database on GCP SQL

lakeFS requires a PostgreSQL database to synchronize actions on your repositories. We will show you how to create a database on Google Cloud SQL, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](gcp.md#install-lakefs-on-ec2)

1. Follow the official [Google documentation](https://cloud.google.com/sql/docs/postgres/quickstart#create-instance) on how to create a PostgreSQL instance.

   Make sure you're using PostgreSQL version &gt;= 11.

2. On the _Users_ tab in the console, create a user to be used by the lakeFS installation.
3. Choose the method by which lakeFS [will connect to your database](https://cloud.google.com/sql/docs/postgres/connect-overview). Google recommends using

   the [SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy).

Depending on the chosen lakeFS installation method, you will need to make sure lakeFS can access your database. For example, if you install lakeFS on GKE, you need to deploy the SQL Auth Proxy from [this Helm chart](https://github.com/rimusz/charts/blob/master/stable/gcloud-sqlproxy/README.md), or as [a sidecar container in your lakeFS pod](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine).

## Installation Options

### On Google Compute Engine

1. Save the following configuration file as `config.yaml`:

   ```yaml
   ---
   database:
     connection_string: "[DATABASE_CONNECTION_STRING]"
   auth:
     encrypt:
       # replace this with a randomly-generated string:
       secret_key: "[ENCRYPTION_SECRET_KEY]"
   blockstore:
     type: gs
      # Uncomment the following lines to give lakeFS access to your buckets using a service account:
      # gs:
      #   credentials_json: [YOUR SERVICE ACCOUNT JSON STRING]
   gateways:
     s3:
         # replace this with the host you will use for the lakeFS S3-compatible endpoint:
        domain_name: [S3_GATEWAY_DOMAIN]
   ```

2. [Download the binary](../#downloads) to the GCE instance.
3. Run the `lakefs` binary on the GCE machine:

   ```bash
   lakefs --config config.yaml run
   ```

   **Note:** it is preferable to run the binary as a service using systemd or your operating system's facilities.

### On Google Cloud Run

To support container-based environments like Google Cloud Run, lakeFS can be configured using environment variables. Here is a `docker run` command to demonstrate starting lakeFS using Docker:

```bash
docker run \
  --name lakefs \
  -p 8000:8000 \
  -e LAKEFS_DATABASE_CONNECTION_STRING="[DATABASE_CONNECTION_STRING]" \
  -e LAKEFS_AUTH_ENCRYPT_SECRET_KEY="[ENCRYPTION_SECRET_KEY]" \
  -e LAKEFS_BLOCKSTORE_TYPE="gs" \
  -e LAKEFS_GATEWAYS_S3_DOMAIN_NAME="[S3_GATEWAY_DOMAIN]" \
  treeverse/lakefs:latest run
```

See the [reference](https://github.com/treeverse/lakeFS/tree/b7c8b3f4ad69e73a5dc68d3168ee38f65fa57f15/docs/reference/configuration.md#using-environment-variables) for a complete list of environment variables.

### On GKE

See [Kubernetes Deployment](k8s.md).

## Load balancing

Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

## DNS

As mentioned above, you should create 3 DNS records for lakeFS: 1. One record for the lakeFS API: `lakefs.example.com` 1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

Depending on your DNS provider, refer to the documentation on how to add CNAME records.

## Next Steps

Your next step is to [prepare your storage](../index-3/index/). If you already have a storage bucket/container, you are ready to [create your first lakeFS repository](../index-3/create-repo.md).

