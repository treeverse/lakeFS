---
layout: default
title: On-Premises Deployment of lakeFS
parent: Deploy and Setup lakeFS
description: This section will guide you through deploying and setting up a production-suitable lakeFS environment on-premises (or on other cloud providers)
nav_order: 50
redirect_from:
   - /deploy/k8s.html
   - /deploy/docker.html 
   - /integrations/minio.html
   - /using/minio.html
next:  ["Import data into your installation", "../howto/import.html"]
---

# On-Premises deployment
{: .no_toc }

⏰ Expected deployment time: 25 min
{: .note }

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Prerequisites

To use lakeFS, you'll need to have access to an S3-compatible object store such as [MinIO](https://min.io)

For more information on how to set up MinIO, see the [official deployment guide](https://min.io/docs/minio/container/operations/installation.html){: target="_blank" }

## Setting up a database

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
This section assumes that you already have a PostgreSQL >= 11.0 database accessible.


## Setting up a lakeFS Server

<div class="tabs">
  <ul>
    <li><a href="#linux">Linux</a></li>
    <li><a href="#docker">Docker</a></li>
    <li><a href="#k8s">Kubernetes</a></li>
  </ul>
  <div markdown="1" id="linux">

Connect to your host using SSH:

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
     type: s3
     s3:
        force_path_style: true
        endpoint: http://<minio_endpoint>
        discover_bucket_region: false
        credentials:
           access_key_id: <minio_access_key>
           secret_access_key: <minio_secret_key>
   ```

   ⚠️ Notice that the lakeFS Blockstore type is set to `s3` - This configuration works with S3-compatible storage engines such as [MinIO](https://min.io/){: target="blank" }.
   {: .note }

1. [Download the binary](../index.md#downloads) to the server.

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
  -e LAKEFS_BLOCKSTORE_TYPE="s3" \
  -e LAKEFS_BLOCKSTORE_S3_FORCE_PATH_STYLE="true" \
  -e LAKEFS_BLOCKSTORE_S3_ENDPOINT="http://<minio_endpoint>" \
  -e LAKEFS_BLOCKSTORE_S3_DISCOVER_BUCKET_REGION="false" \
  -e LAKEFS_BLOCKSTORE_S3_CREDENTIALS_ACCESS_KEY_ID="<minio_access_key>" \
  -e LAKEFS_BLOCKSTORE_S3_CREDENTIALS_SECRET_ACCESS_KEY="<minio_secret_key>" \
  treeverse/lakefs:latest run
```

⚠️ Notice that the lakeFS Blockstore type is set to `s3` - This configuration works with S3-compatible storage engines such as [MinIO](https://min.io/){: target="blank" }.
{: .note }

See the [reference](../reference/configuration.md#using-environment-variables) for a complete list of environment variables.


</div>
<div markdown="3" id="k8s">

You can install lakeFS on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).

To install lakeFS with Helm:

1. Copy the Helm values file relevant for S3-Compatible storage (MinIO in this example):

   ```yaml
   secrets:
       # replace this with the connection string of the database you created in a previous step:
       databaseConnectionString: [DATABASE_CONNECTION_STRING]
       # replace this with a randomly-generated string
       authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
   lakefsConfig: |
       blockstore:
         type: s3
         s3:
           force_path_style: true
           endpoint: http://<minio_endpoint>
           discover_bucket_region: false
           credentials:
             access_key_id: <minio_access_key>
             secret_access_key: <minio_secret_key>
   ```

   ⚠️ Notice that the lakeFS Blockstore type is set to `s3` - This configuration works with S3-compatible storage engines such as [MinIO](https://min.io/){: target="blank" }.
   {: .note }
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

## Secure connection

It is recommended that TLS/SSL termination by your infrastructure's load balancer or cluster manager.

To configure your local lakeFS to listen and serve with HTTPS, update the lakeFS config yaml with the additional `tls` section:

```yaml
tls:
  enabled: true
  cert_file: server.crt   # provide path to your certificate file
  key_file: server.key    # provide path to your server private key
```


## Local Blockstore

You can configure a block adapter to a POSIX compatible storage location shared by all lakeFS instances. 
Using the shared storage location, both data and metadata will be stored there.

Using the local blockstore import and allowing lakeFS access to a specific prefix, it is possible to import files from a shared location.
Import is not enabled by default, as it doesn't assume the local path is shared and there is a security concern about accessing a path outside the specified in the blockstore configuration.
Enabling is done by `blockstore.local.import_enabled` and `blockstore.local.allowed_external_prefixes` as described in the [configuration reference](../reference/configuration.md).

### Sample configuration using local blockstore

```yaml
database:
  type: "postgres"
  postgres:
    connection_string: "[DATABASE_CONNECTION_STRING]"
  
auth:
  encrypt:
    # replace this with a randomly-generated string. Make sure to keep it safe!
    secret_key: "[ENCRYPTION_SECRET_KEY]"

blockstore:
  type: local
  local:
    path: /shared/location/lakefs_data    # location where data and metadata kept by lakeFS
    import_enabled: true                  # required to be true to enable import files
                                          # from `allowed_external_prefixes` locations
    allowed_external_prefixes:
      - /shared/location/files_to_import  # location with files we can import into lakeFS, require access from lakeFS
```

### Limitations

- Using a local adapter on a shared location is relativly new and not battle-tested yet
- lakeFS doesn't control the way a shared location is managed across machines
- Import works only for folders
- Garbage collector (for committed and uncommitted) and lakeFS Hadoop FileSystem currently unsupported

{% include_relative includes/setup.md %}
