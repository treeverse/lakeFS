---
layout: default
title: On Docker
parent: Deploy lakeFS
description: This guide will help you deploy your production lakeFS environment with Docker.
nav_order: 50
---
# Deploy lakeFS on Docker
{: .no_toc }

## Database
{: .no_toc }

lakeFS requires a PostgreSQL database to synchronize actions on your repositories.
This section assumes you already have a PostgreSQL database accessible from where you intend to install lakeFS.
Instructions for creating the database can be found on the deployment instructions for [AWS](./aws.md#creating-the-database-on-aws-rds), [Azure](./azure.md#creating-the-database-on-azure-database) and [GCP](./gcp.md#creating-the-database-on-gcp-sql).

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
