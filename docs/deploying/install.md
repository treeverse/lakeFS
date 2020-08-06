---
layout: default
title: Installing lakeFS
parent: Deployment
nav_order: 20
has_children: false
---

# Installing lakeFS
{: .no_toc }
You are now ready to install the lakeFS server. Following are some options of how to do that.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Kubernetes with Helm

To install lakeFS with Helm, create a `conf-values.yaml` file, and run the following commands:

```bash
# Add the lakeFS repository
helm repo add lakefs https://charts.lakefs.io
# Deploy lakeFS
helm install lakefs/lakefs -f conf-values.yaml --name example-lakefs
```

`example-lakefs` is the [Helm Release](https://helm.sh/docs/intro/using_helm/#three-big-concepts) name.

Here is an example `conf-values.yaml`. See [below](#configurations) for more configuration options.

```yaml
blockstore:
  type: s3
  s3:
    region: us-east-1
  databaseConnectionString: postgres://lakefs:lakefs@postgres/postgres
  authEncryptSecretKey: <some random secret string>
  gateways:
    s3:
      domain_name: s3.lakefs.example.com
```

You should give your Kubernetes nodes access to all S3 buckets you intend to use lakeFS with.
If you can't provide such access, you can use an AWS key-pair to authenticate (see configurations below). 

### Configurations

| **Parameter**                               | **Description**                                                                                            | **Default** |
|---------------------------------------------|------------------------------------------------------------------------------------------------------------|-------------|
| `blockstore.type`                           | Type of storage to use: `s3`, `local`, `mem`                                                               |             |
| `blockstore.s3.region`                      | AWS region where to use for storage                                                                        |             |
| `blockstore.s3.credentials.accessKeyId`     | AWS Access Key to use when accessing S3. Leave empty if your Kuberenets nodes have access to your buckets. |             |
| `blockstore.s3.credentials.secretAccessKey` | AWS Secret Key to use when accessing S3. Leave empty if your Kuberenets nodes have access to your buckets. |             |
| `gateways.s3.domain_name` | Domain name to be used by clients to call the lakeFS S3-compatible API |             |
| `databaseConnectionString`                  | Connection string to your lakeFS database                                                                  |             |
| `authEncryptSecretKey`                      | A cryptographically secure random string                                                                   |             |
| `replicaCount`                              | Number of lakeFS pods                                                                                      | `1`         |
| `resources`                                 | Pod resource requests & limits                                                                             | `{}`        |
| `service.type`                              | Kubernetes service type                                                                                   | ClusterIP   |
| `service.port`                              | Kubernetes service external port                                                                           | 80          |

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
$ docker run \
    --name lakefs \
    -p 8000:8000 \
    -v <PATH_TO_CONFIG_FILE>:/home/lakefs/.lakefs.yaml \
    treeverse/lakefs:latest run
```

## Fargate and other container-based environments

Some environments make it harder to use a configuration file, and are best configured using environment variables.

Here is an example of running lakeFS using environment variables. See the [reference](../reference/configuration.md#using-environment-variables) for the full list of configurations.

```sh
$ docker run \
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
   $ lakefs --config <PATH_TO_CONFIG_FILE> run
   ``` 