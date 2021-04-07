---
layout: default
title: Installing lakeFS
description: Installing lakeFS is easy. This section covers common deployment options for installing lakeFS.
parent: Production Deployment
nav_order: 20
has_children: false
---

# Installing lakeFS
{: .no_toc }

## Preqrequisites
{: .no_toc }
A production-suitable lakeFS installation will require three DNS records **pointing at your lakeFS server**.
A good convention for those will be, assuming you own the domain `example.com`:
  
  * `lakefs.example.com` 
  * `s3.lakefs.example.com` - **this is the S3 Gateway Domain**
  * `*.s3.lakefs.example.com`


Take note of the second one, which is the *S3 Gateway Domain*. You will have to use it in the lakeFS configuration. [Why do I need these three DNS records?](#why-do-i-need-the-three-dns-records)

You can now move on to your preferred installation method:

1. TOC
{:toc}

## Kubernetes with Helm

lakeFS can be easily installed on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).
To install lakeFS with Helm:
1. Copy the Helm values file relevant to your cloud provider:
   
   <div class="tab">
     <button id="helm_aws_btn" class="tablinks" onclick="openTab(this, 'helm_aws_tab')">AWS</button>
     <button class="tablinks" onclick="openTab(this, 'helm_google_tab')">Google Cloud</button>
     <button class="tablinks" onclick="openTab(this, 'helm_azure_tab')">Microsoft Azure</button>
      <script>
         $(() => {
           $('#helm_aws_btn').click();
         })
      </script>
   </div>
   <div markdown="1" id="helm_aws_tab" class="tabcontent" >
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
         region: us-east-1
     gateways:
       s3:
         # replace this with the host you will use for the lakeFS S3-compatible endpoint:
         domain_name: [S3_GATEWAY_DOMAIN]
   ```
   </div>
   <div markdown="1" id="helm_google_tab" class="tabcontent">
   ### Notes for running lakeFS on GKE
   {: .no_toc }
     * To connect to your database, you need to use one of the ways of [connecting GKE to Cloud SQL](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine#cloud-sql-auth-proxy-with-workload-identity).
     * To give lakeFS access to your bucket, you can start the cluster in [storage-rw](https://cloud.google.com/container-registry/docs/access-control#gke) mode. Alternatively, you can use a service account JSON string by uncommenting the `gs.credentials_json` property in the following yaml.
   
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
     gateways:
       s3:
         # replace this with the host you will use for the lakeFS S3-compatible endpoint:
         domain_name: [S3_GATEWAY_DOMAIN]
   ```
   </div>
   <div markdown="1" id="helm_azure_tab" class="tabcontent">
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
     gateways:
       s3:
         # replace this with the host you will use for the lakeFS S3-compatible endpoint:
         domain_name: s3.lakefs.example.com
   ```
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

Once your installation is running, move on to [Load Balancing and DNS](./lb_dns.md).

## Docker
To deploy using Docker, create a yaml configuration file.
Here is a minimal example, but you can see the [reference](../reference/configuration.md#example-aws-deployment) for the full list of configurations.

```yaml
database:
  connection_string: "[DATABASE_CONNECTION_STRING]"
auth:
  encrypt:
    secret_key: "[ENCRYPTION_SECRET_KEY]"
blockstore:
  type: s3  # or "gs", or "azure"
gateways:
  s3:
    domain_name: "[S3_GATEWAY_DOMAIN]"
```

Save the configuration file locally as `lakefs-config.yaml` and run the following command:

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -v $(pwd)/lakefs-config.yaml:/home/lakefs/.lakefs.yaml \
  treeverse/lakefs:latest run
```

Once your installation is running, move on to [Load Balancing and DNS](./lb_dns.md).

## AWS ECS / Google Cloud Run / Azure Container Instances 

Some environments make it harder to use a configuration file, and are best configured using environment variables.
All lakeFS configurations can be given through environment variables, see the [reference](../reference/configuration.md#using-environment-variables) for the full list of configurations.

These configurations can be used to run lakeFS on container orchestration service providers like AWS ECS, Google Cloud Run , or Azure Container Instances.

```bash
export STORAGE_PROVIDER=s3  # or "gs", or "azure"
docker run \
  --name lakefs \
  -p 8000:8000 \
  -e LAKEFS_DATABASE_CONNECTION_STRING="[DATABASE_CONNECTION_STRING]" \
  -e LAKEFS_AUTH_ENCRYPT_SECRET_KEY="[ENCRYPTION_SECRET_KEY]" \
  -e LAKEFS_BLOCKSTORE_TYPE="${STORAGE_PROVIDER}" \
  -e LAKEFS_GATEWAYS_S3_DOMAIN_NAME="[S3_GATEWAY_DOMAIN]" \
  treeverse/lakefs:latest run
```

Once your installation is running, move on to [Load Balancing and DNS](./lb_dns.md).

## AWS EC2 / Google Compute Engine / Azure Virtual Machine
Run lakeFS directly on a cloud instance:

1. [Download the binary for your operating system](../downloads.md)
2. `lakefs` is a single binary, you can run it directly, but preferably run it as a service using systemd or your operating system's facilities.

   ```bash
   lakefs --config <PATH_TO_CONFIG_FILE> run
   ``` 
3. To support azure AD authentication go to `Identity` tab and switch `Status` toggle to on, then add the `Storage Blob Data Contributer' role on the container you created.

Once your installation is running, move on to [Load Balancing and DNS](./lb_dns.md).

## Why do I need the three DNS records?
{: .no_toc }

They are needed to access the two different lakeFS APIs (these are also covered in the [Architecture](../architecture/overview.md) section):

1. **The lakeFS OpenAPI**: used by the `lakectl` CLI tool. Exposes git-like operations (branching, diffing, merging etc.).
1. **An S3-compatible API**: read and write your data in any tool that can communicate with S3. Examples include: AWS CLI, Boto, Presto and Spark.

lakeFS actually exposes only one API endpoint. For every request, lakeFS checks the `Host` header.
If the header is under the S3 gateway domain, the request is directed to the S3-compatible API.

The third DNS record (`*.s3.lakefs.example.com`) allows for [virtual-host style access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html). This is a way for AWS client to specify the bucket name in the Host subdomain.
