## lakeFS Playground

Experience lakeFS first hand with your own isolated environment.
You can easily integrate it with your existing tools, and feel lakeFS in action in an environment
similar to your own.

<p>
    <a class="btn btn-green" href="https://demo.lakefs.io/" target="_blank">
        Try lakeFS now without installing
    </a>
</p>

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



## Katacoda Tutorial

Learn how to use lakeFS using the CLI and an interactive Spark shell - all from your browser, without installing anything.

In the tutorial we cover:

- Basic `lakectl` command line usage
- How to read, write, list and delete objects from lakeFS using the `lakectl` command
- Read from, and write to lakeFS using its S3 API interface using [Spark](https://spark.apache.org/){: target="_blank" }
- Diff, commit and merge the changes created by Spark
- Track commit history to understand changes to your data over time

The web based environment provides a full working lakeFS and Spark environment, so feel free to explore it on your own.

<p>
    <a class="btn btn-green" href="https://www.katacoda.com/lakefs/scenarios/lakefs-play" target="_blank">
        Start Katacoda Tutorial Now
    </a>
</p>

## Next Steps

After getting acquainted with lakeFS, easily [install it on your computer](installing.html) or [deploy it on your cloud account](../deploy/index.html).
