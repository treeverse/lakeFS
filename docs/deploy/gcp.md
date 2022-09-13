---
layout: default
title: On GCP
parent: Deploy lakeFS
description: This guide will help you deploy your production lakeFS environment on Google Cloud Platform (GCP).
nav_order: 30
---

# Deploy lakeFS on GCP
{: .no_toc }
Expected deployment time: 25 min

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Creating the Database on GCP SQL
lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
We will show you how to create a database on Google Cloud SQL, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

If you already have a database, take note of the connection string and skip to the [next step](#install-lakefs-on-ec2)

1. Follow the official [Google documentation](https://cloud.google.com/sql/docs/postgres/quickstart#create-instance) on how to create a PostgreSQL instance.
   Make sure you're using PostgreSQL version >= 11.
1. On the *Users* tab in the console, create a user. The lakeFS installation will use it to connect to your database.
1. Choose the method by which lakeFS [will connect to your database](https://cloud.google.com/sql/docs/postgres/connect-overview). Google recommends using
   the [SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy).

Depending on the chosen lakeFS installation method, you will need to make sure lakeFS can access your database.
For example, if you install lakeFS on GKE, you need to deploy the SQL Auth Proxy from [this Helm chart](https://github.com/rimusz/charts/blob/master/stable/gcloud-sqlproxy/README.md), or as [a sidecar container in your lakeFS pod](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine).

## Installation Options

### On Google Compute Engine
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

### On Google Cloud Run
To support container-based environments like Google Cloud Run, lakeFS can be configured using environment variables.

1. Save the following configuration file as `config.yaml`:

   ```yaml
   apiVersion: serving.knative.dev/v1
   kind: Service
   metadata:
     name: [LAKEFS_SERVICE_NAME]
   spec:
     template:
       metadata:
         annotations:
           autoscaling.knative.dev/maxScale: '5'
           run.googleapis.com/client-name: cloud-console
           run.googleapis.com/cloudsql-instances: [POSTGRES_INSTANCE_NAME]
       spec:
         containers:
         - image: gcr.io/lakefs/treeverse/lakefs@sha256:18be2f7e86f7572160909b7dcae2e50be5704cb174eeaf4fb61274def3536009
           ports:
           - name: http1
             containerPort: 8000
           env:
           - name: LAKEFS_BLOCKSTORE_TYPE
             value: gs
           - name: LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON
             value: '[YOUR SERVICE ACCOUNT JSON STRING]'
           - name: LAKEFS_DATABASE_CONNECTION_STRING
             value: [DATABASE_CONNECTION_STRING]
           - name: LAKEFS_STATS_ENABLED
             value: 'false'
           - name: INSTANCE_CONNECTION_NAME
             value: [POSTGRES_INSTANCE_NAME]
           - name: CLOUD_SQL_CONNECTION_NAME
             value: [POSTGRES_INSTANCE_NAME]
           - name: LAKEFS_AUTH_ENCRYPT_SECRET_KEY
             valueFrom:
               secretKeyRef:
                 key: '2'
                 name: [ENCRYPTION_SECRET_KEY]
           resources:
             limits:
               cpu: 2000m
               memory: 1Gi
   ```
   
   See the [reference](../reference/configuration.md#using-environment-variables) for a complete list of environment variables.

1. Create the service using Google Cloud CLI:

   ```sh
   gcloud run services replace config.yaml
   ```

1. Go to Google Cloud Run UI, select the checkbox for the lakeFS service created in the previous step and click on "ADD PRINCIPAL" button:

   <img src="{{ site.baseurl }}/assets/img/gcp_1.png" alt="Google Cloud Run UI" />

1. Enter "allUsers" in "New principals" column, select "Cloud Run Invoker" role and save:

   <img src="{{ site.baseurl }}/assets/img/gcp_2.png" alt="Add principals and roles" />

1. Select service name to go to service details and click on the URL to go to lakeFS UI:

   <img src="{{ site.baseurl }}/assets/img/gcp_3.png" alt="lakeFS URL" />

### On GKE
See [Kubernetes Deployment](./k8s.md).

## Load balancing
Depending on how you chose to install lakeFS, you should have a load balancer direct requests to the lakeFS server.  
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint that you can use for health checks.

## Next Steps
Your next step is to [prepare your storage](../setup/storage/index.md). If you already have a storage bucket/container, you are ready to [create your first lakeFS repository](../setup/create-repo.md).
