---
layout: default
title: With Kubernetes
parent: Deploy lakeFS
description: This guide will help you deploy your production lakeFS environment on Kubernetes using a helm chart
nav_order: 40
---


# Deploy lakeFS on Kubernetes
{: .no_toc }

## Database
{: .no_toc }

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
This section assumes you already have a PostgreSQL database accessible from your Kubernetes cluster.
Instructions for creating the database can be found on the deployment instructions for [AWS](./aws.md#creating-the-database-on-aws-rds), [Azure](./azure.md#creating-the-database-on-azure-database) and [GCP](./gcp.md#creating-the-database-on-gcp-sql).

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Installing on Kubernetes

lakeFS can be easily installed on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).
To install lakeFS with Helm:
1. Copy the Helm values file relevant to your storage provider:

<div class="tabs">
   <ul>
     <li><a href="#helm-tabs-1">S3</a></li>
     <li><a href="#helm-tabs-2">GCS</a></li>
     <li><a href="#helm-tabs-3">Azure Blob</a></li>
   </ul>
   <div markdown="1" id="helm-tabs-1">
```yaml
secrets:
    # replace DATABASE_CONNECTION_STRING with the connection string of the database you created in a previous step.
    # e.g. postgres://postgres:myPassword@my-lakefs-db.rds.amazonaws.com:5432/lakefs
    databaseConnectionString: [DATABASE_CONNECTION_STRING]
    # replace this with a randomly-generated string
    authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
lakefsConfig: |
    blockstore:
      type: s3
      s3:
        region: us-east-1 # optional, fallback in case discover from bucket is not supported
```
   </div>
   <div markdown="1" id="helm-tabs-2">
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
   **Notes for running lakeFS on GKE**
   * To connect to your database, you need to use one of the ways of [connecting GKE to Cloud SQL](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine#cloud-sql-auth-proxy-with-workload-identity).
   * To give lakeFS access to your bucket, you can start the cluster in [storage-rw](https://cloud.google.com/container-registry/docs/access-control#gke) mode. Alternatively, you can use a service account JSON string by uncommenting the `gs.credentials_json` property in the following yaml.

   </div>
   <div markdown="1" id="helm-tabs-3">
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
     #  In case you chose to authenticate via access key unmark the following rows and insert the values from the previous step 
     #  storage_account: [your storage account]
     #  storage_access_key: [your access key]
```
   </div>
</div>

1. Fill in the missing values and save the file as `conf-values.yaml`. For more configuration options, see our Helm chart [README](https://github.com/treeverse/charts/blob/master/charts/lakefs/README.md#custom-configuration){:target="_blank"}.

   The `lakefsConfig` parameter is the lakeFS configuration documented [here](https://docs.lakefs.io/reference/configuration.html), but without sensitive information.
   Sensitive information like `databaseConnectionString` is given through separate parameters, and the chart will inject them into Kubernetes secrets.

1. In the directory where you created `conf-values.yaml`, run the following commands:

    ```bash
    # Add the lakeFS repository
    helm repo add lakefs https://charts.lakefs.io
    # Deploy lakeFS
    helm install example-lakefs lakefs/lakefs -f conf-values.yaml
    ```

   *example-lakefs* is the [Helm Release](https://helm.sh/docs/intro/using_helm/#three-big-concepts) name.

You should give your Kubernetes nodes access to all buckets/containers you intend to use lakeFS with.
If you can't provide such access, lakeFS can be configured to use an AWS key-pair, an Azure access key, or a Google Cloud credentials file to authenticate (part of the `lakefsConfig` YAML below).
{: .note .note-info }

## Load balancing
You should have a load balancer direct requests to the lakeFS server.
Options to do so include a Kubernetes Service of type `LoadBalancer`, or a Kubernetes Ingress.
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.


## Next Steps
Your next step is to [prepare your storage](../setup/storage/index.md). If you already have a storage bucket/container, you are ready to [create your first lakeFS repository](../setup/create-repo.md).
