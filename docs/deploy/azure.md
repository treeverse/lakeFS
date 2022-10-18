---
layout: default
title: On Azure
parent: Deploy lakeFS
description:  This guide will help you deploy your production lakeFS environment on Microsoft Azure.
nav_order: 20
---

# Deploy lakeFS on Azure
{: .no_toc }
Expected deployment time: 25 min

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Creating the Database on Azure Database
lakeFS requires a PostgreSQL database to synchronize actions in your repositories.
We will show you how to create a database on Azure Database, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#install-lakefs-on-azure-vm)

1. Follow the official [Azure documentation](https://docs.microsoft.com/en-us/azure/postgresql/quickstart-create-server-database-portal){: target="_blank" } on how to create a PostgreSQL instance and connect to it.
   Make sure that you're using PostgreSQL version >= 11.
1. Once your Azure Database for PostgreSQL server is set up and the server is in the `Available` state, take note of the endpoint and username.
   ![Azure postgres Connection String]({{ site.baseurl }}/assets/img/azure_postgres_conn.png)
1. Make sure your Access control roles allow you to connect to the database instance.

## Installation Options

### On Azure VM
1. Save the following configuration file as `config.yaml`:

   ```yaml
   ---
   database:
     type: "postgres"
     postgres:
       connection_string: "[DATABASE_CONNECTION_STRING]"
   auth:
     encrypt:
       # replace this with a randomly-generated string:
       secret_key: "[ENCRYPTION_SECRET_KEY]"
   blockstore:
     type: azure
     azure:
       auth_method: msi # msi for active directory, access-key for access key 
         # In case you chose to authenticate via access key, unmark the following rows and insert the values from the previous step 
         # storage_account: [your storage account]
         # storage_access_key: [your access key]
   ```
   
1. [Download the binary](../index.md#downloads) to the Azure Virtual Machine.
1. Run the `lakefs` binary on the machine:
   ```bash
   lakefs --config config.yaml run
   ```
   **Note:** It is preferable to run the binary as a service using systemd or your operating system's facilities.
1. To support Azure AD authentication go to `Identity` tab and switch `Status` toggle to on, then add the `Storage Blob Data Contributor' role on the container you created.

### On Azure Container instances
To support container-based environments like Azure Container Instances, you can configure lakeFS using environment variables. Here is a `docker run`
command to demonstrate starting lakeFS using Docker:

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -e LAKEFS_DATABASE_TYPE="postgres" \
  -e LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING="[DATABASE_CONNECTION_STRING]" \
  -e LAKEFS_AUTH_ENCRYPT_SECRET_KEY="[ENCRYPTION_SECRET_KEY]" \
  -e LAKEFS_BLOCKSTORE_TYPE="azure" \
  -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCOUNT="[YOUR_STORAGE_ACCOUNT]" \
  -e LAKEFS_BLOCKSTORE_AZURE_STORAGE_ACCESS_KEY="[YOUR_ACCESS_KEY]" \
  treeverse/lakefs:latest run
```

See the [reference](../reference/configuration.md#using-environment-variables) for a complete list of environment variables.

### On AKS
See [Kubernetes Deployment](./k8s.md).

## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

## Next Steps
Your next step is to [prepare your storage](../setup/storage/index.md). If you already have a storage bucket/container, you are ready to [create your first lakeFS repository](../setup/create-repo.md).
