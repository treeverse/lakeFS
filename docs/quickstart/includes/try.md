## lakeFS Docker "Everything Bagel"

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

## Next Steps

You can now [install lakeFS on your computer](installing.html), [try without installing](https://demo.lakefs.io), or [deploy it on your cloud account](../deploy/index.html).
