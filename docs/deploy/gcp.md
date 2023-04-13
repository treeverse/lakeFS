---
layout: default
title: GCP
parent: Deploy and Setup lakeFS
description: This section will guide you through deploying and setting up a production-suitable lakeFS environment on Google Cloud Platform (GCP).
nav_order: 40
redirect_from:
   - /setup/storage/gcs.html 
next:  ["Import data into your installation", "../howto/import.html"]
---

# Deploy lakeFS on GCP
{: .no_toc }

⏰ Expected deployment time: 25 min
{: .note }

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Create a Database

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
We will show you how to create a database on Google Cloud SQL, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#run-the-lakefs-server)

1. Follow the official [Google documentation](https://cloud.google.com/sql/docs/postgres/quickstart#create-instance) on how to create a PostgreSQL instance.
   Make sure you're using PostgreSQL version >= 11.
1. On the *Users* tab in the console, create a user. The lakeFS installation will use it to connect to your database.
1. Choose the method by which lakeFS [will connect to your database](https://cloud.google.com/sql/docs/postgres/connect-overview). Google recommends using
   the [SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy).


## Run the lakeFS Server

<div class="tabs">
  <ul>
    <li><a href="#gce">GCE Instance</a></li>
    <li><a href="#docker">Docker</a></li>
    <li><a href="#gke">GKE</a></li>
  </ul>
  <div markdown="1" id="gce">

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
     type: gs
      # Uncomment the following lines to give lakeFS access to your buckets using a service account:
      # gs:
      #   credentials_json: [YOUR SERVICE ACCOUNT JSON STRING]
   ```
   
1. [Download the binary](../index.md#downloads) to the GCE instance.
1. Run the `lakefs` binary on the GCE machine:
   ```bash
   lakefs --config config.yaml run
   ```
   **Note:** it is preferable to run the binary as a service using systemd or your operating system's facilities.

</div>
<div markdown="2" id="docker">

To support container-based environments like Google Cloud Run, lakeFS can be configured using environment variables. Here is a `docker run`
command to demonstrate starting lakeFS using Docker:

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -e LAKEFS_DATABASE_TYPE="postgres" \
  -e LAKEFS_DATABASE_POSTGRES_CONNECTION_STRING="[DATABASE_CONNECTION_STRING]" \
  -e LAKEFS_AUTH_ENCRYPT_SECRET_KEY="[ENCRYPTION_SECRET_KEY]" \
  -e LAKEFS_BLOCKSTORE_TYPE="gs" \
  treeverse/lakefs:latest run
```

See the [reference](../reference/configuration.md#using-environment-variables) for a complete list of environment variables.

</div>
<div markdown="3" id="gke">

You can install lakeFS on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).

To install lakeFS with Helm:

1. Copy the Helm values file relevant for Google Storage:
   
   ```yaml
   secrets:
       # replace DATABASE_CONNECTION_STRING with the connection string of the database you created in a previous step.
       # e.g.: postgres://postgres:myPassword@localhost/postgres:5432
       databaseConnectionString: [DATABASE_CONNECTION_STRING]
       # replace this with a randomly-generated string
       authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
   lakefsConfig: |
       blockstore:
         type: gs
         # Uncomment the following lines to give lakeFS access to your buckets using a service account:
         # gs:
         #   credentials_json: [YOUR SERVICE ACCOUNT JSON STRING]
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