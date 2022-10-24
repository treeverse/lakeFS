---
layout: default
title: With Docker
parent: Deploy lakeFS
description: This guide will help you deploy your production lakeFS environment with Docker.
nav_order: 50
next: ["Prepare your storage", "../setup/storage/index.html"]
---
# Deploy lakeFS on Docker
{: .no_toc }

## Database
{: .no_toc }

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
This section assumes that you already have a PostgreSQL database accessible from where you intend to install lakeFS.
You can find instructions for creating the database in the deployment instructions for [AWS](./aws.md#creating-the-database-on-aws-rds), [Azure](./azure.md#creating-the-database-on-azure-database) and [GCP](./gcp.md#creating-the-database-on-gcp-sql).

{% include toc.html %}

{% include_relative includes/prerequisites.md %}

## Installing on Docker
To deploy using Docker, create a yaml configuration file.
Here is a minimal example, but you can see the [reference](../reference/configuration.md#example-aws-deployment) for the full list of configurations.
<div class="tabs">
<ul>
  <li><a href="#docker-tabs-1">AWS</a></li>
  <li><a href="#docker-tabs-2">Google Cloud</a></li>
  <li><a href="#docker-tabs-3">Microsoft Azure</a></li>
</ul>
<div markdown="1" id="docker-tabs-1">
{% include_relative includes/aws-docker-config.md %}
</div>
<div markdown="1" id="docker-tabs-2">
{% include_relative includes/gcp-docker-config.md %}
</div>
<div markdown="1" id="docker-tabs-3">
{% include_relative includes/azure-docker-config.md %}
</div>
</div>

Save the configuration file locally as `lakefs-config.yaml` and run the following command:

```sh
docker run \
  --name lakefs \
  -p 8000:8000 \
  -v $(pwd)/lakefs-config.yaml:/etc/lakefs/config.yaml \
  treeverse/lakefs:latest run --config /etc/lakefs/config.yaml
```

## Load balancing
You should have a load balancer direct requests to the lakeFS server.
By default, lakeFS operates on port 8000, and exposes a `/_health` endpoint which you can use for health checks.

## Next Steps
Your next step is to [prepare your storage](../setup/storage/index.md). If you already have a storage bucket/container, you are ready to [create your first lakeFS repository](../setup/create-repo.md).
