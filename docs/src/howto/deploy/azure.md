---
title: Azure
description: How to deploy and set up a production-suitable lakeFS environment on Microsoft Azure
next:  ["Import data into your installation", "../import/index.md"]
---

# Deploy lakeFS on Azure

!!! tip
    The instructions given here are for a self-managed deployment of lakeFS on Azure. <br/>
    For a hosted lakeFS service with guaranteed SLAs, try [lakeFS Cloud](https://lakefs.cloud)

When you deploy lakeFS on Azure these are the options available to use:

![](../../assets/img/deploy/deploy-on-azure.excalidraw.png)

This guide walks you through the options available and how to configure them, finishing with configuring and running lakeFS itself and creating your first repository.

!!! info "⏰ Expected deployment time: 25 min"

## Object Storage

lakeFS supports the following [Azure Storage](https://learn.microsoft.com/en-us/azure/storage/common/storage-introduction) types:

1. [Azure Blob Storage](https://azure.microsoft.com/en-gb/products/storage/blobs)
2. [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) ([HNS](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace))

Data Lake Storage Gen1 is not supported.

## Authentication Method

lakeFS supports two ways to authenticate with Azure.

=== "Identity Based Authentication (recommended)"

    lakeFS uses environment variables to determine credentials to use for authentication. The following authentication methods are supported:

    1. Managed Service Identity (MSI)
    1. Service Principal RBAC
    1. Azure CLI

    For deployments inside the Azure ecosystem it is recommended to use a managed identity.

    More information on authentication methods and environment variables can be found [here](https://learn.microsoft.com/en-us/azure/developer/go/azure-sdk-authentication)

    ### How to Create Service Principal for Resource Group

    It is recommended to create a resource group that consists of all the resources lakeFS should have access to.

    Using a resource group will allow dynamic removal/addition of services from the group, effectively providing/preventing access for lakeFS to these resources without requiring any changes in configuration in lakeFS or providing lakeFS with any additional credentials.

    The minimal role required for the service principal is "Storage Blob Data Contributor"

    The following Azure CLI command creates a service principal for a resource group called "lakeFS" with permission to access (read/write/delete)
    Blob Storage resources in the resource group and with an expiry of 5 years

    ``` shell
    az ad sp create-for-rbac \
    --role "Storage Blob Data Contributor" \
    --scopes /subscriptions/XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX/resourceGroups/lakeFS --years 5

    Creating 'Storage Blob Data Contributor' role assignment under scope '/subscriptions/947382ea-681a-4541-99ab-b718960c6289/resourceGroups/lakeFS'
    The output includes credentials that you must protect. Be sure that you do not include these credentials in your code or check the credentials into your source control. For more information, see https://aka.ms/azadsp-cli
    {
    "appId": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "displayName": "azure-cli-2023-01-30-06-18-30",
    "password": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "tenant": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    }
    ```

    The command output should be used to populate the following environment variables:

    ```
    AZURE_CLIENT_ID      =  $appId
    AZURE_TENANT_ID      =  $tenant
    AZURE_CLIENT_SECRET  =  $password
    ```

    !!! danger
        Service Principal credentials have an expiry date and lakeFS will lose access to resources unless credentials are renewed on time.

    !!! info
        It is possible to provide both account based credentials and environment variables to lakeFS. In that case - lakeFS will use
        the account credentials for any access to data located in the given account, and will try to use the identity credentials for any data located outside the given account.

=== "Storage Account Credentials"

    Storage account credentials can be set directly in the lakeFS configuration using the following parameters:

    * `blockstore.azure.storage_account`
    * `blockstore.azure.storage_access_key`

    !!!warning "Limitations"

        Please note that using this authentication method limits lakeFS to the scope of the given storage account.

        Specifically, **the following operations will not work**:

        1. Import of data from different storage accounts
        1. Copy/Read/Write of data that was imported from a different storage account
        1. Create pre-signed URL for data that was imported from a different storage account

## K/V Store

lakeFS stores metadata in a database for its versioning engine.
This is done via a Key-Value interface that can be implemented on any DB engine and lakeFS comes with several built-in driver implementations (You can read more about it [here](https://docs.lakefs.io/understand/how/kv.html)).

The database used doesn't _have_ to be a dedicated K/V database.

=== "CosmosDB"
    [CosmosDB](https://azure.microsoft.com/en-us/products/cosmos-db/) is a managed database service provided by Azure.

    lakeFS supports [CosmosDB For NoSQL](https://learn.microsoft.com/en-GB/azure/cosmos-db/nosql/) as a database backend.

    1. Follow the official [Azure documentation](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-create-account?tabs=azure-cli)
    on how to create a CosmosDB account for NoSQL and connect to it.
    1. Once your CosmosDB account is set up, you can create a Database for
    lakeFS. For lakeFS ACID guarantees, make sure to select the [Bounded
    staleness consistency](https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels#bounded-staleness-consistency),
    for single region deployments.
    1. Create a new container in the database and select type
    `partitionKey` as the Partition key (case sensitive).
    1. Pass the endpoint, database name and container name to lakeFS as
    described in the [configuration guide][config-reference-azure-block].
    You can either pass the CosmosDB's account read-write key to lakeFS, or
    use a managed identity to authenticate to CosmosDB, as described
    [earlier](#authentication-method).

=== "PostgreSQL"

    Below we show you how to create a database on Azure Database, but you can use any PostgreSQL database as long as it's accessible by your lakeFS installation.

    If you already have a database, take note of the connection string and skip to the [next step](#4-run-the-lakefs-server)

    1. Follow the official [Azure documentation](https://docs.microsoft.com/en-us/azure/postgresql/quickstart-create-server-database-portal){: target="_blank" } on how to create a PostgreSQL instance and connect to it.
    Make sure that you're using PostgreSQL version >= 11.
    1. Once your Azure Database for PostgreSQL server is set up and the server is in the _Available_ state, take note of the endpoint and username.
    ![Azure postgres Connection String](../../assets/img/azure_postgres_conn.png)
    1. Make sure your Access control roles allow you to connect to the database instance.

## 4. Run the lakeFS server

Now that you've chosen and configured object storage, a K/V store, and authentication—you're ready to configure and run lakeFS. There are three different ways you can run lakeFS:

=== "Azure VM"
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
    ```
    1. [Download the binary](https://github.com/treeverse/lakeFS/releases) to run on the VM.
    1. Run the `lakefs` binary:
    ```sh
    lakefs --config config.yaml run
    ```

    !!! note
        It's preferable to run the binary as a service using systemd or your operating system's facilities.

=== "Docker"
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

    See the [reference][config-envariables] for a complete list of environment variables.

=== "Azure Kubernetes Service (AKS)"
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
        #  If you chose to authenticate via access key, unmark the following rows and insert the values from the previous step
        #  storage_account: [your storage account]
        #  storage_access_key: [your access key]
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

### Load balancing

To configure a load balancer to direct requests to the lakeFS servers you can use the `LoadBalancer` Service type or a Kubernetes Ingress.
By default, lakeFS operates on port `8000` and exposes a `/_health` endpoint that you can use for health checks.

!!! info
    The NGINX Ingress Controller by default limits the client body size to 1 MiB.

    Some clients use bigger chunks to upload objects - for example, multipart upload to lakeFS using the [S3-compatible Gateway][s3-gateway] or
    a simple PUT request using the [OpenAPI Server][openapi].

    Check out the Nginx [documentation](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/annotations/#custom-max-body-size) for increasing the limit, or an example of Nginx configuration with [MinIO](https://min.io/docs/minio/linux/integrations/setup-nginx-proxy-with-minio.html).

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
1. Click _Create Repository_ and choose _Blank Repository_.
   <img src="../../../assets/img/create-repo-no-sn.png" alt="Create Repo"/>
1. Under Storage Namespace, enter a path to your desired location on the object store. This is where data written to this repository will be stored.
1. Click _Create Repository_
1. You should now have a configured repository, ready to use!
   <img src="../../../assets/img/repo-created.png" alt="Repo Created" style="border: 1px solid #DDDDDD;"/>

!!! success "Congratulations"
    Your environment is now ready 🤩
