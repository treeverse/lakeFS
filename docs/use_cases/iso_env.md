---
layout: default 
title: Isolated Environment
description: In this tutorial, we will explore how to use lakeFS for isolated test environments.
parent: Use Cases
nav_order: 10
has_children: false
---

# Isolated Environments

## Why do I need multiple environments?

When working with a data lake, it's useful to have replicas of your production environment. These replicas allow you to test and understand changes to your data without impacting the consumers of the production data.

Running ETL and transformation jobs directly in production is a guaranteed way to have data issues flow into dashboards, ML models, and other consumers sooner or later. The most common approach to avoid making changes directly in production is to create and maintain a second data environment (test environment) where updates are implemented first. 

The issue with this approach is that it's time-consuming and costly to maintain this separate test environment. And for larger teams it forces multiple people to share a single environment, requiring significant co-ordination.

## How do I create isolated environments with lakeFS?

lakeFS makes creating isolated test environments instantaneous. This frees you from spending time on environment maintenance and makes it possible to create as many environments as needed.

In a lakeFS repository, data is always located on a `branch`. You can think of each `branch` in lakeFS as its own environment. This is because branches are isolated, meaning changes on one branch have no effect other branches.

Objects that remain unchanged between two branches are not copied, but rather shared to both branches via metadata pointers that lakeFS manages. If you make a change on one branch and want it reflected on another, you can perform a `merge` operation to update one branch with the changes from another.
{: .note }

Let’s see an example of using multiple lakeFS branches for isolation.
 

## Using branches as environments

The key difference when using lakeFS for isolated data environments is that you can create them immediately before testing a change. And once new data is merged into production, you can delete the branch - effectively deleting the old environment.

This is different from creating a long-living test environment used as a staging area to test all the updates. With lakeFS, **we create a new branch for each change to production** that we want to make. One benefit of this is the ability to test multiple changes at one time.


### Prerequisites

This tutorial will use an exisitng lakeFS environment and an Apache Spark notebook.

To read more about how to setup lakeFS environment, check out [Quickstart](../quickstart/index.md).

Once you have a live environment, it’ll be useful to add some data into the lakeFS repo. We’ll use an Amazon review dataset from a public S3 bucket. First, we’ll download the file to our local computer using the AWS CLI. Then we’ll upload it into lakeFS using the `Upload Object` button in the UI.

To install the AWS CLI, follow [these instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
{: .note .note-info }

Download the file
```bash
aws s3 cp s3://amazon-reviews-pds/parquet/product_category=Sports/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet $HOME/
```

![See Objects]({{ site.baseurl }}/assets/img/iso-env-objects.png)

Next, on the `Objects` tab of the example repo, click `Upload Object` and then `Choose File` and find it in the Finder window.

![Upload Object]({{ site.baseurl }}/assets/img/iso-env-upload-object.png)

Once it is uploaded, we’ll see the file in the repository on the `main` branch. Currently, it's in an uncommitted state. Let’s commit it!

![Commit Object]({{ site.baseurl }}/assets/img/iso-env-commit.png)

To do this, we can go to the `Uncommitted Changes` tab and click the green `Commit Changes` button in the top right. Add a commit message and the file is in the version history of our lakeFS repo.

As the final setup step, we're going to create a new branch called `double-branch`. To do this, we can use the lakeFS UI by going to the Branches tab and clicking `Create Branch`. Once we create it, we’ll see two branches: `main` and `double-branch`.

![Create Branch]({{ site.baseurl }}/assets/img/iso-env-two-branches.png)

This new branch serves as an isolated environment on which we can make changes that have no effect on `main`. Let's see that in action by using...

### Data manipulation with Jupyter & Spark

This use case shows how manipulating data using Spark works, and we're using Juputer. However, you can integrate lakeFS with your favorite [Spark environment](../integrations/spark). Let’s use them to manipulate the data on one branch, showing how it has no effect on the other.

Go to your Spark notebook UI, in our case this is the Jupyter UI.

Inside the notebook, create a new notebook and start a spark context:

```bash
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
```

Now we can use Spark to read in the Parquet file we added to the main branch of our lakeFS repo:

```bash
df = spark.read.parquet('s3a://example/main/')
```

To see the Dataframe, run `display(df.show())`. If we run `display(df.count())` we'll get returned that the Dataframe has 486k rows.


### Changing one branch

Let’s accidentally write the DataFrame back to the `double-branch` branch, creating a duplicate object on that branch.

```bash
df.write.mode('append').parquet('s3a://example/double-branch/')
```

What happens if we re-read in the data on both branches and perform a count on the resulting DataFrames?

![Branch Counts]({{ site.baseurl }}/assets/img/iso-env-df-counts.png)

As expected, there are now twice as many rows, 972k, on the `double-branch` branch. That means we **duplicated our data!** oh no!

Data duplication introduce errors into our data analytics, BI and machine learning efforts, hence we would like to avoid duplicating our data.

On the `main` branch however, there is still just the origin 486k rows. This shows the utility of branch-based isolated environments with lakeFS.

You can safely continue working with the data from main which is unharmed due to lakeFS isolation capabilities. 

