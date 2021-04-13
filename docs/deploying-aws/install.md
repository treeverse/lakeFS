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

For production deployments, install the lakeFS binary on your host of choice.

## Preqrequisites
{: .no_toc }
A production-suitable lakeFS installation will require three DNS records **pointing at your lakeFS server**.
A good convention for those will be, assuming you already own the domain `example.com`:
  
  * `lakefs.example.com` 
  * `s3.lakefs.example.com` - **this is the S3 Gateway Domain**
  * `*.s3.lakefs.example.com`


The second record, the *S3 Gateway Domain*, is used in lakeFS configuration to differentiate between the S3 Gateway API and the OpenAPI Server. For more info, see [Why do I need these three DNS records?](#why-do-i-need-the-three-dns-records)

Find your preferred installation method:

1. TOC
{:toc}

## Kubernetes with Helm

lakeFS can be easily installed on Kubernetes using a [Helm chart](https://github.com/treeverse/charts/tree/master/charts/lakefs).
To install lakeFS with Helm:
1. Copy the Helm values file relevant to your cloud provider:
   <div class="tabs">
   <ul>
     <li><a href="#helm-tabs-1">AWS</a></li>
     <li><a href="#helm-tabs-2">Google Cloud</a></li>
     <li><a href="#helm-tabs-3">Microsoft Azure</a></li>
   </ul>
   <div markdown="1" id="helm-tabs-1">      
   {% include_relative installation-methods/aws-helm-values.md %}
   </div>
   <div markdown="1" id="helm-tabs-2">
   {% include_relative installation-methods/gcp-helm-values.md %}
   </div>
   <div markdown="1" id="helm-tabs-3">
   {% include_relative installation-methods/azure-helm-values.md %}
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

Once your installation is running, move on to [Load Balancing and DNS](./lb_dns.md).

## Docker
To deploy using Docker, create a yaml configuration file.
Here is a minimal example, but you can see the [reference](../reference/configuration.md#example-aws-deployment) for the full list of configurations.
<div class="tabs">
<ul>
  <li><a href="#docker-tabs-1">AWS</a></li>
  <li><a href="#docker-tabs-2">Google Cloud</a></li>
  <li><a href="#docker-tabs-3">Microsoft Azure</a></li>
</ul>
<div markdown="1" id="docker-tabs-1">      
{% include_relative installation-methods/aws-docker-config.md %}
</div>
<div markdown="1" id="docker-tabs-2">
{% include_relative installation-methods/gcp-docker-config.md %}
</div>
<div markdown="1" id="docker-tabs-3">
{% include_relative installation-methods/azure-docker-config.md %}
</div>
</div>

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
Here is a `docker run` command to demonstrate the use of environment variables:

<div class="tabs">
<ul>
  <li><a href="#docker-run-tabs-1">AWS</a></li>
  <li><a href="#docker-run-tabs-2">Google Cloud</a></li>
  <li><a href="#docker-run-tabs-3">Microsoft Azure</a></li>
</ul>
<div markdown="1" id="docker-run-tabs-1">      
{% include_relative installation-methods/aws-docker-run.md %}
</div>
<div markdown="1" id="docker-run-tabs-2">
{% include_relative installation-methods/gcp-docker-run.md %}
</div>
<div markdown="1" id="docker-run-tabs-3">
{% include_relative installation-methods/azure-docker-run.md %}
</div>
</div>

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

Multiple DNS records are needed to access the two different lakeFS APIs (covered in more detail in the [Architecture](../architecture/overview.md) section):

1. **The lakeFS OpenAPI**: used by the `lakectl` CLI tool. Exposes git-like operations (branching, diffing, merging etc.).
1. **An S3-compatible API**: read and write your data in any tool that can communicate with S3. Examples include: AWS CLI, Boto, Presto and Spark.

lakeFS actually exposes only one API endpoint. For every request, lakeFS checks the `Host` header.
If the header is under the S3 gateway domain, the request is directed to the S3-compatible API.

The third DNS record (`*.s3.lakefs.example.com`) allows for [virtual-host style access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html). This is a way for AWS clients to specify the bucket name in the Host subdomain.
