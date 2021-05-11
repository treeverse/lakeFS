---
layout: default
title: On Kubernetes
parent: Deploy lakeFS
description:
nav_order: 40
---


# Deploy lakeFS on Kubernetes
{: .no_toc }

## Database
{: .no_toc }

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
This section assumes you already have a PostgreSQL database accessible from your Kubernetes cluster.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

{% include_relative includes/prerequisites.md %}

## Installing on Kuberneets

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
   {% include_relative includes/aws-helm-values.md %}
   </div>
   <div markdown="1" id="helm-tabs-2">
   {% include_relative includes/gcp-helm-values.md %}
   </div>
   <div markdown="1" id="helm-tabs-3">
   {% include_relative includes/azure-helm-values.md %}
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

## DNS
As mentioned above, you should create 3 DNS records for lakeFS:
1. One record for the lakeFS API: `lakefs.example.com`
1. Two records for the S3-compatible API: `s3.lakefs.example.com` and `*.s3.lakefs.example.com`.

All records should point to your Load Balancer, preferably with a short TTL value.

## Next Steps
You can now move on to the [Setup](../guides/setup.md) page.

{% include_relative includes/why-dns.md %}
