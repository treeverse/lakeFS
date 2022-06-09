---
layout: default 
title: Tutorial – Isolated Environment
description: In this section we will explore how to use lakeFS for isolated dev environments.
parent: Quickstart
nav_order: 60
has_children: false
---

# Isolated Environments

## Why Do I Need Multiple Environments?

When developing over a data lake, it is useful to have replicas of your production environment. These replicas allow you to test and understand changes to your data without impacting consumers of the production data.

Running ETL and transformation jobs directly in production is a guaranteed way to have data issues flow into dashboards, ML models, and other consumers sooner or later. The most common approach to avoid making changes directly in production is to create and maintain a second data environment called development (or dev) where updates are implemented first. 

The issue with this approach is that it is time-consuming and costly to maintain this separate dev environment. And for larger teams it forces multiple people to share one environment, requiring co-ordination.

## How do I create isolated environments with lakeFS

lakeFS makes it instantaneous to create isolated development environments. This frees you from spending time on environment maintenance  and makes it possible to create as many environments as needed.

In a lakeFS repository, data is always located on a `branch`. You can think of each `branch` in lakeFS as its own environment. This is because branches are isolated, meaning changes on one branch have no effect other branches.

Data that is the same between two branches is not copied, but rather shared on both via metadata pointers that lakeFS manages. If you make a change on one branch and want it reflected on another, you can perform a `merge` operation to update one branch with the changes from another.
{: .note }

Let’s show an example of using multiple lakeFS branches for isolation.
 

## Using Branches as Environments

The key difference when using lakeFS for isolated data environments is you can create them immediately before testing a change. And once new data is merged into production, you can delete the branch, effectively deleting the old dev environment.

This is different from creating a long-living dev environment that is used as a staging area to test all updates. With lakeFS, we create a new branch for each change to production we want to make. (One benefit of this is the ability to test multiple changes at one time).


### Setup

To get a working lakeFS environment, we’re going to run a pre-configured Docker environment on our local (Mac) machine. This environment (which we call the “Everything Bagel” includes lakeFS and other common data tools like Spark, dbt, Trino, Hive, and Jupyter.)

The following commands can be run in your terminal to get the bagel running:
1. Clone the lakeFS repo: `git clone https://github.com/treeverse/lakeFS.git`
2. Start the Docker containers: `cd deployments/compose && docker compose up -d`

Once you have your Docker environment running, it is helpful to pull up the UI for lakeFS. To do this navigate to http://localhost:8000 in your browser. The access key and secret to login are found in the `docker_compose.yml` file in the `lakefs-setup` section.

![Setup Done]({{ site.baseurl }}/assets/img/iso-env-ex-repo.png)

The first thing to notice is in this environment, lakeFS comes with a repository called `example` already created, and the repo’s default branch is `main`.

Next it’ll be useful to add some data into this lakeFS repo. We’ll use an Amazon review dataset from a public S3 bucket. First we’ll download the file to our local computer using the AWS CLI. Then, we’ll upload it into lakeFS using the `Upload Object` button in the UI.

To install the AWS CLI, follow [these instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) {: .note .note-info }

Download the file
```aws s3 cp s3://amazon-reviews-pds/parquet/product_category=Sports/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet $HOME/
```

![See Objects]({{ site.baseurl }}/assets/img/iso-env-objects.png)

Next, on the `Objects` tab of the example repo, click `Upload Object` then `Choose File` and find it in the Finder window.

![Upload Object]({{ site.baseurl }}/assets/img/iso-env-upload-object.png)

Once it is uploaded, we’ll see the file in the repository on the `main` branch. Currently it is in an uncommitted state. Let’s commit it!

![Commit Object]({{ site.baseurl }}/assets/img/iso-env-commit.png)

To do this we can go to the `Uncommitted Changes` tab and click the green `Commit Changes` button in the top right. Add a commit message and the file is in the version history of our lakeFS repo.

As the final setup step, we're going to create a new branch called `double-branch`. To do this we can use the lakeFS UI by going to the Branches tab and clicking `Create Branch`. Once we create it, we’ll see two branches, `main` and `double-branch`.

![Commit Object]({{ site.baseurl }}/assets/img/iso-env-commit.png)

This new branch serves as an isolated environment on which we can make changes that have no effect on `main`. Let's see that in action by using...

### Data Manipulation with Jupyter & Spark

The Everything Bagel comes with Spark and Jupyter installed. Let’s use them to manipulate the data on one branch, showing how it has no affect on the other.

To access the Jupyter notebook UI, go to http://localhost:8888 in your browser and type in “lakefs” when prompted for a password. 

Next, create a new notebook and start a spark context:

```
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
```

Now we can use spark to read in the parquet file we added to the main branch of our lakeFS repo:

```
df = spark.read.parquet('s3a://example/main/')
```

To see the Dataframe, run `display(df.show())`. If we run `display(df.count())` we'll get returned that the Dataframe has 486k rows.


### Changing one branch

Let’s accidentally write the DataFrame back to the `double-branch` branch, creating a duplicate object on that branch.

```
df.write.mode('append').parquet('s3a://example/double-branch/')
```

What happens if we re-read in the data on both branches and perform a count on the resulting DataFrames?

![Branch Counts]({{ site.baseurl }}/assets/img/iso-env-df-counts.png)

As expected, there are now twice as many rows, 972k, on the `double-branch` branch. On the `main` branch however, there is still just the origin 486k rows. This shows the utility of branch-based isolated environments with lakeFS.

