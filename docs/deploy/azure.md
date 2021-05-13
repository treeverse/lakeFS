---
layout: default
title: On Azure
parent: Deploy lakeFS
description:  This guide will help you deploy your production lakeFS environment on Azure 
nav_order: 20
---

# Deploy lakeFS on Azure
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

{% include_relative includes/prerequisites.md %}

## Creating the Database on Azure Database
lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
We will show you how to create a database on Azure Database, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#install-lakefs-on-azure-vm)

1. Follow the official [Azure documentation](https://docs.microsoft.com/en-us/azure/postgresql/quickstart-create-server-database-portal){: target="_blank" } on how to create a PostgreSQL instance and connect to it.
   Make sure you're using PostgreSQL version >= 11.
1. Once your Azure Database for PostgreSQL server is set up and the server is in `Available` state, take note of the endpoint and username.
   ![Azure postgres Connection String](../assets/img/azure_postgres_conn.png)
1. Make sure your Access control roles allow you to connect to the database instance.

## Install lakeFS on Azure VM
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
     type: azure
     azure:
       auth_method: msi # msi for active directory, access-key for access key 
         # In case you chose to authenticate via access key unmark the following rows and insert the values from the previous step 
         # storage_account: [your storage account]
         # storage_access_key: [your access key]
   gateways:
     s3:
         # replace this with the host you will use for the lakeFS S3-compatible endpoint:
        domain_name: [S3_GATEWAY_DOMAIN]
   ```
   
1. [Download the binary](../index.md#downloads) to the Azure Virtual Machine.
1. Run the `lakefs` binary on the machine:
   ```bash
   lakefs --config config.yaml run
   ```
   **Note:** it is preferable to run the binary as a service using systemd or your operating system's facilities.
1. To support Azure AD authentication go to `Identity` tab and switch `Status` toggle to on, then add the `Storage Blob Data Contributor' role on the container you created.


## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

## DNS
As mentioned above, you should create 3 DNS records for lakeFS:
1. One record for the lakeFS API: `lakefs.example.com`
1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

Depending on your DNS provider, refer to the documentation on how to add CNAME records.

## Next Steps
You can now move on to the [Setup](../guides/setup.md) page.

{% include_relative includes/why-dns.md %}