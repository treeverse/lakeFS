---
layout: default
title: On GCP
parent: Deploy lakeFS
description:
nav_order: 30
---

# Deploy lakeFS on GCP
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC 
{:toc}

{% include_relative installation-methods/prerequisites.md %}

## Creating the Database on GCP SQL
lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
We will show you how to create a database on Google Cloud SQL, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#install-lakefs-on-ec2)

1. Follow the official [Google documentation](https://cloud.google.com/sql/docs/postgres/quickstart#create-instance) on how to create a PostgreSQL instance.
   Make sure you're using PostgreSQL version >= 11.
1. On the *Users* tab in the console, create a user to be used by the lakeFS installation.
1. Choose the method by which lakeFS [will connect to your database](https://cloud.google.com/sql/docs/postgres/connect-overview). Google recommends using
   the [SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy).

Depending on the chosen lakeFS installation method, you will need to make sure lakeFS can access your database.
For example, if you install lakeFS on GKE, you need to deploy the SQL Auth Proxy from [this Helm chart](https://github.com/rimusz/charts/blob/master/stable/gcloud-sqlproxy/README.md), or as [a sidecar container in your lakeFS pod](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine).

You can now proceed to [Configuring the Storage](bucket.md).

## Install lakeFS on GCE
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
1. [Download the binary](../index.md#downloads) to the GCE instance.
1. Run the `lakefs` binary on the GCE machine:
   ```bash
   lakefs --config config.yaml run
   ```
   **Note:** it is preferable to run the binary as a service using systemd or your operating system's facilities.

## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

## DNS
As mentioned above, you should create 3 DNS records for lakeFS:
1. One record for the lakeFS API: `lakefs.example.com`
1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

Depending on your DNS provider, refer to the documentation on how to add CNAME records.

You can now move on to the [Setup](setup.md) page.
