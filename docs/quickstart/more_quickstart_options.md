---
layout: default
title: More QuickStart Options
description: This section outlines additional quickstart options to running lakeFS.
parent: QuickStart
nav_order: 50
has_children: false
next: ["Tutorials", "./tutorials.html"]
---

# More Quickstart Options
{: .no_toc}

{% include learn_only.html %} 

{% include toc.html %}

## Using lakeFS Docker "Everything Bagel"

Get a local lakeFS instance running in a Docker container. This environment includes lakeFS and other common data tools like Spark, dbt, Trino, Hive, and Jupyter.

As a prerequisite, Docker is required to be installed on your machine. For download instructions, [click here](https://docs.docker.com/get-docker/)

The following commands can be run in your terminal to get the Bagel running:

1. Clone the lakeFS repo: `git clone https://github.com/treeverse/lakeFS.git`
2. Start the Docker containers: `cd lakeFS/deployments/compose && docker compose up -d`

Once you have your Docker environment running, it is helpful to pull up the UI for lakeFS. To do this navigate to `http://localhost:8000` in your browser. The access key and secret to login are found in the `docker_compose.yml` file in the `lakefs-setup` section.

Once you are logged in, you should see a page that looks like below.

![Setup Done]({{ site.baseurl }}/assets/img/iso-env-example-repo.png)

The first thing to notice is in this environment, lakeFS comes with a repository called `example` already created, and the repo’s default branch is `main`. If your lakeFS installation doesn't have the `example` repo created, you can use the green `Create Repository` button to do so:

![Create Repo]({{ site.baseurl }}/assets/img/iso-env-create-repo.png)

Now that your lakeFS is running, you can head to the [tutorials](tutorials.html) section.

## On Kubernetes with Helm

1. Install lakeFS on a Kubernetes cluster using Helm:
   ```bash
   # Add the lakeFS Helm repository
   helm repo add lakefs https://charts.lakefs.io
   # Deploy lakeFS with helm release "my-lakefs"
   helm install my-lakefs lakefs/lakefs
   ```

1. The printed output will help you forward a port to lakeFS, so you can access it from your browser at [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"}.

1. Now that your lakeFS is running, you can head to the [tutorials](tutorials.html) section.

## Using the Binary 

Alternatively, you may opt to run the lakefs binary directly on your computer.

1. Download the lakeFS binary for your operating system:

   [Download lakefs](../index.md#downloads){: .btn .btn-green target="_blank"}

1. Create a configuration file:
    
   ```yaml
   ---
   database:
     type: local
     local:
       path: "~/lakefs/metadata"
    
   blockstore: 
     type: "local"
     local:
       path: "~/lakefs/data"
   ```

1. Create a local directories to store objects and metadata:

   ```sh
   mkdir -p ~/lakefs/data ~/lakefs/metadata
   ```

1. Run the server:
    
   ```bash
   ./lakefs --config /path/to/config.yaml run
   ```

1. Check your installation by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser.

1. Now that your lakeFS is running, you can head to the [tutorials](tutorials.html) section.

## Next steps

Now that your lakeFS is running, you can head to the [tutorials](tutorials.html) section.