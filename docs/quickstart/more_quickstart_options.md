---
layout: default
title: More Quickstart Options
description: This section outlines additional quickstart options to deploying lakeFS.
parent: Quickstart
nav_order: 50
has_children: false
---

# More Quickstart Options
{: .no_toc}

{% include learn_only.html %} 

{% include toc.html %}

## With the lakeFS Playground

Spin up a lakeFS installation in seconds. Your installation will be available for 7 days.

[Take me to the playground](https://demo.lakefs.io){: target="_blank" class="btn btn-green" }

## On Kubernetes with Helm

1. Install lakeFS on a Kubernetes cluster using Helm:
   ```bash
   # Add the lakeFS Helm repository
   helm repo add lakefs https://charts.lakefs.io
   # Deploy lakeFS with helm release "my-lakefs"
   helm install my-lakefs lakefs/lakefs
   ```

1. The printed output will help you forward a port to lakeFS, so you can access it from your browser at [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"}.

1. Move on to [create your first repository](repository.md) in lakeFS.

## Using the Binary 

Alternatively, you may opt to run the lakefs binary directly on your computer.

1. Download the lakeFS binary for your operating system:

   [Download lakefs](https://github.com/treeverse/lakeFS/releases){: .btn .btn-green target="_blank"}

2. Run the server:
    
   ```bash
   ./lakefs run --local-settings
   ```

   It will create `~/lakefs` and store data and metadata in `data` and `metadata` subfolders respectively. 

3. Complete the setup by opening [http://127.0.0.1:8000/setup](http://127.0.0.1:8000/setup){:target="_blank"} in your web browser and creating the admin user.

4. You are now ready to [create your first repository](repository.md#create-the-repository) in lakeFS.

## lakeFS "Everything Bagel"

This Docker Compose application includes lakeFS together with other common data tools like Spark, dbt, Trino, Hive, and Jupyter.

Run the following commands to get the Bagel running:

1. Clone the lakeFS repo: `git clone https://github.com/treeverse/lakeFS.git`
2. Start the Docker containers: `cd lakeFS/deployments/compose && docker compose up -d`

Once the environment is running, open the UI for lakeFS by navigating to _http://localhost:8000_ in your browser.
The login credentials can be found in the `docker_compose.yml` file in the `lakefs-setup` section.

Once you are logged in, you should see a page that looks like below.

![Setup Done]({{ site.baseurl }}/assets/img/iso-env-example-repo.png)

Note that a repository called `example` is already created. If your lakeFS installation doesn't have this repository, click the `Create Repository` button to do so:

![Create Repo]({{ site.baseurl }}/assets/img/iso-env-create-repo.png)
