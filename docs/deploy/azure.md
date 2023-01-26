---
layout: default
title: Azure
parent: Deploy and Setup lakeFS
description: This section will guide you through deploying and setting up a production-suitable lakeFS environment on Microsoft Azure
nav_order: 20
redirect_from:
   - ../setup/storage/blob.html 
next:  ["Import data into your installation", "../howto/import.html"]
---

# Deploy lakeFS on Azure
{: .no_toc }

⏰ Expected deployment time: 25 min
{: .note }

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Create a database

lakeFS requires a PostgreSQL database to synchronize actions in your repositories.
We will show you how to create a database on Azure Database, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#run-the-lakefs-server)

1. Follow the official [Azure documentation](https://docs.microsoft.com/en-us/azure/postgresql/quickstart-create-server-database-portal){: target="_blank" } on how to create a PostgreSQL instance and connect to it.
   Make sure that you're using PostgreSQL version >= 11.
1. Once your Azure Database for PostgreSQL server is set up and the server is in the `Available` state, take note of the endpoint and username.
   ![Azure postgres Connection String]({{ site.baseurl }}/assets/img/azure_postgres_conn.png)
1. Make sure your Access control roles allow you to connect to the database instance.

## Run the lakeFS server

### Storage account access

lakeFS uses [Azure SDK for go](https://github.com/Azure/azure-sdk-for-go/) 
under the hood. The environment variables listed in options 1&2 in the Azure 
[docs](https://learn.microsoft.
com/en-us/azure/developer/go/azure-sdk-authentication#2-authenticate-with
-azure) are supported by lakeFS. You may still pass storage accounts 
credentials directly to lakeFS by setting the `blockstore.azure.
storage_account` & `blockstore.azure.storage_access_key`.  


<div class="tabs">
  <ul>
    <li><a href="#vm">Azure VM</a></li>
    <li><a href="#docker">Docker</a></li>
    <li><a href="#aks">AKS</a></li>
  </ul>
  <div markdown="1" id="vm">

Connect to your VM instance using SSH:

1. Create a `config.yaml` on your VM, with the following parameters:
  
   ```yaml
   ---
   database:
     type: "postgres"
     postgres:
       connection_string: "[DATABASE_CONNECTION_STRING]"
  
   auth:
     encrypt:
       # replace this with a randomly-generated string. Make sure to keep it safe!
       secret_key: "[ENCRYPTION_SECRET_KEY]"
   
   blockstore:
     type: azure
     azure:
       auth_method: msi # msi for active directory, access-key for access key 
         # In case you chose to authenticate via access key, unmark the following rows and insert the values from the previous step 
         # storage_account: [your storage account]
         # storage_access_key: [your access key]
   ```
1. [Download the binary](../index.md#downloads) to the VM.
1. Run the `lakefs` binary:
  
   ```sh
   lakefs --config config.yaml run
   ```

**Note:** It's preferable to run the binary as a service using systemd or your operating system's facilities.
{: .note }

</div>
<div markdown="2" id="docker">

To support container-based environments, you can configure lakeFS using environment variables. Here is a `docker run`
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


</div>
<div markdown="2" id="aks">

You can install lakeFS on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).

To install lakeFS with Helm:

1. Copy the Helm values file relevant for Azure Blob:
   
   ```yaml
   secrets:
       # replace this with the connection string of the database you created in a previous step:
       databaseConnectionString: [DATABASE_CONNECTION_STRING]
       # replace this with a randomly-generated string
       authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
   lakefsConfig: |
       blockstore:
         type: azure
         azure:
           auth_method: msi # msi for active directory, access-key for access key 
       #  If you chose to authenticate via access key, unmark the following rows and insert the values from the previous step 
       #  storage_account: [your storage account]
       #  storage_access_key: [your access key]
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


## Load balancing
{: .no_toc }

To configure a load balancer to direct requests to the lakeFS servers you can use the `LoadBalancer` Service type or a Kubernetes Ingress.
By default, lakeFS operates on port 8000 and exposes a `/_health` endpoint that you can use for health checks.

💡 The NGINX Ingress Controller by default limits the client body size to 1 MiB.
Some clients use bigger chunks to upload objects - for example, multipart upload to lakeFS using the [S3-compatible Gateway](../understand/architecture.md#s3-gateway) or 
a simple PUT request using the [OpenAPI Server](../understand/architecture.md#openapi-server).
Checkout Nginx [documentation](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/annotations/#custom-max-body-size) for increasing the limit, or an example of Nginx configuration with [MinIO](https://docs.min.io/docs/setup-nginx-proxy-with-minio.html).
{: .note }

</div>
</div>



{% include_relative includes/setup.md %}