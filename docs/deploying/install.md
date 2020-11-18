---
layout: default
title: Installing lakeFS
parent: AWS Deployment
nav_order: 20
has_children: false
---

# Installing lakeFS
{: .no_toc }
You are now ready to install the lakeFS server. Following are some options for doing that.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Kubernetes with Helm

lakeFS can be easily installed on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).
To install lakeFS with Helm:
1. Create a `conf-values.yaml` file, replacing values as described in the comments:
 
    ```yaml
   secrets:
     # replace this with the connection string of the database you created in a previous step:
     databaseConnectionString: postgres://postgres:myPassword@my-lakefs-db.rds.amazonaws.com:5432/lakefs?search_path=lakefs
     # replace this with a randomly-generated string
     authEncryptSecretKey: <some random string>
   lakefsConfig: |
     blockstore:
       type: s3
       s3:
         region: us-east-1
     gateways:
       s3:
         # replace this with the host you will use for the lakeFS S3-compatible endpoint:
         domain_name: s3.lakefs.example.com
    ```
   
    See [below](#configurations) for more configuration options. The `lakefsConfig` parameter is the lakeFS configuration documented [here](https://docs.lakefs.io/reference/configuration.html), but without sensitive information.
    Sensitive information like `databaseConnectionString` is given through separate parameters, and the chart will inject them into Kubernetes secrets.
        
1. In the directory where you created `conf-values.yaml`, run the following commands:

    ```bash
    # Add the lakeFS repository
    helm repo add lakefs https://charts.lakefs.io
    # Deploy lakeFS
    helm install example-lakefs lakefs/lakefs -f conf-values.yaml
    ```

    `example-lakefs` is the [Helm Release](https://helm.sh/docs/intro/using_helm/#three-big-concepts) name.

You should give your Kubernetes nodes access to all S3 buckets you intend to use lakeFS with.
If you can't provide such access, lakeFS can be configured to use an AWS key-pair to authenticate (part of the `lakefsConfig` YAML below).
{: .note .note-info }

### Configurations

| **Parameter**                               | **Description**                                                                                            | **Default** |
|---------------------------------------------|------------------------------------------------------------------------------------------------------------|-------------|
|`secrets.databaseConnectionString`|PostgreSQL connection string to be used by lakeFS||
|`secrets.authEncryptSecretKey`|A random (cryptographically safe) generated string that is used for encryption and HMAC signing||
| `lakefsConfig`                              | lakeFS config YAML stringified, as shown above. See [reference](../reference/configuration.md) for available configurations.                                                               |             |
| `replicaCount`                              | Number of lakeFS pods                                                                                      | `1`         |
| `resources`                                 | Pod resource requests & limits                                                                             | `{}`        |
| `service.type`                              | Kubernetes service type                                                                                    | ClusterIP   |
| `service.port`                              | Kubernetes service external port                                                                           | 80          |
| `extraEnvVarsSecret`                        | A Kubernetes secret name containing extra env variables                                                    |             |

## Docker
To deploy using Docker, create a yaml configuration file.
Here is a minimal example, but you can see the [reference](../reference/configuration.md) for the full list of configurations.

```yaml
database:
  connection_string: "postgres://user:pass@<RDS_ENDPOINT>:5432/postgres"

auth:
  encrypt:
    secret_key: "<RANDOM_GENERATED_STRING>"

blockstore:
  type: s3

gateways:
  s3:
    domain_name: s3.lakefs.example.com
```

Depending on your runtime environment, running lakeFS using docker would look like this:

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -v <PATH_TO_CONFIG_FILE>:/home/lakefs/.lakefs.yaml \
  treeverse/lakefs:latest run
```

## Fargate and other container-based environments

Some environments make it harder to use a configuration file, and are best configured using environment variables.

Here is an example of running lakeFS using environment variables. See the [reference](../reference/configuration.md#using-environment-variables) for the full list of configurations.

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -e LAKEFS_DATABASE_CONNECTION_STRING="postgres://user:pass@<RDS ENDPOINT>..." \
  -e LAKEFS_AUTH_ENCRYPT_SECRET_KEY="<RANDOM_GENERATED_STRING>" \
  -e LAKEFS_BLOCKSTORE_TYPE="s3" \
  -e LAKEFS_GATEWAYS_S3_DOMAIN_NAME="s3.lakefs.example.com" \
  treeverse/lakefs:latest run
```

## AWS EC2

Alternatively, you can run lakeFS directly on an EC2 instance:

1. [Download the binary for your operating system](../downloads.md)
2. `lakefs` is a single binary, you can run it directly, but preferably run it as a service using systemd or your operating system's facilities.

   ```bash
   lakefs --config <PATH_TO_CONFIG_FILE> run
   ``` 