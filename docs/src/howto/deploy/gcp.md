---
title: GCP
description: How to deploy and set up a production-suitable lakeFS environment on Google Cloud Platform (GCP).
next:  ["Import data into your installation", "../import/index.md"]
---

# Deploy lakeFS on GCP

!!! tip
    The instructions given here are for a self-managed deployment of lakeFS on GCP. <br/>
    For a hosted lakeFS service with guaranteed SLAs, please [contact us](https://lakefs.io/contact-us/) for details of lakeFS Cloud on GCP.

When you deploy lakeFS on GCP these are the options available to use:

![](../../assets/img/deploy/deploy-on-gcp.excalidraw.png)

!!! info "⏰ Expected deployment time: 25 min"

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

=== "GCE Instance"
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
    
    1. [Download the binary][downloads] to run on the GCE instance.
    1. Run the `lakefs` binary on the GCE machine:
    ```bash
    lakefs --config config.yaml run
    ```
    **Note:** it is preferable to run the binary as a service using systemd or your operating system's facilities.

=== "Docker"
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

    See the [reference][config-envariables] for a complete list of environment variables.

=== "GKE"
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

    !!! note
        The `lakefsConfig` parameter is the lakeFS configuration documented [here](https://docs.lakefs.io/reference/configuration.html) but without sensitive information.
        Sensitive information like `databaseConnectionString` is given through separate parameters, and the chart will inject it into Kubernetes secrets.
    
    1. In the directory where you created `conf-values.yaml`, run the following commands:

    ```bash
    # Add the lakeFS repository
    helm repo add lakefs https://charts.lakefs.io
    # Deploy lakeFS
    helm install my-lakefs lakefs/lakefs -f conf-values.yaml
    ```

    *my-lakefs* is the [Helm Release](https://helm.sh/docs/intro/using_helm/#three-big-concepts) name.

## Load balancing

To configure a load balancer to direct requests to the lakeFS servers you can use the `LoadBalancer` Service type or a Kubernetes Ingress.
By default, lakeFS operates on port 8000 and exposes a `/_health` endpoint that you can use for health checks.

!!! tip
    The NGINX Ingress Controller by default limits the client body size to 1 MiB.

    Some clients use bigger chunks to upload objects - for example, multipart upload to lakeFS using the [S3-compatible Gateway][s3-gateway] or 
    a simple PUT request using the [OpenAPI Server][openapi].
    
    Checkout the Nginx [documentation](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/annotations/#custom-max-body-size) for increasing the limit, or an example of Nginx configuration with [MinIO](https://docs.min.io/docs/setup-nginx-proxy-with-minio.html).

## Create the admin user

When you first open the lakeFS UI, you will be asked to create an initial admin user.

1. Open `http://<lakefs-host>/` in your browser. If you haven't set up a load balancer, this will likely be `http://<instance ip address>:8000/`
1. On first use, you'll be redirected to the setup page:

   <img src="../../../assets/img/setup.png" alt="Create user">

1. Follow the steps to create an initial administrator user. Save the credentials you’ve received somewhere safe, you won’t be able to see them again!

   <img src="../../../assets/img/setup_done.png" alt="Setup Done">

1. Follow the link and go to the login screen. Use the credentials from the previous step to log in.

## Create your first repository

1. Use the credentials from the previous step to log in
1. Click *Create Repository* and choose *Blank Repository*.

   <img src="../../../assets/img/create-repo-no-sn.png" alt="Create Repo"/>

1. Under Storage Namespace, enter a path to your desired location on the object store. This is where data written to this repository will be stored.
1. Click *Create Repository*
1. You should now have a configured repository, ready to use!

   <img src="../../../assets/img/repo-created.png" alt="Repo Created" style="border: 1px solid #DDDDDD;"/>

!!! success "Congratulations"
    Your environment is now ready 🤩
