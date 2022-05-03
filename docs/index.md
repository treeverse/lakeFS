---
layout: default
title: What is lakeFS
description: A lakeFS documentation website that provides information on how to use lakeFS to deliver resilience and manageability to data lakes.
nav_order: 0
redirect_from: ./downloads.html
---

## What is lakeFS
{: .no_toc }  

lakeFS transforms object storage buckets into data lake repositories that expose a Git-like interface to data of any size.

This interface means users of lakeFS can use the same development workflows for code and data. Adopting Git workflows greatly improved software development practices, and we designed lakeFS to bring the same benefits to data.

In this way, lakeFS brings a unique combination of performance and manageability to data lakes. *To learn more about applying Git principles to data, [see here](https://lakefs.io/how-to-manage-your-data-the-way-you-manage-your-code/).*

The open source lakeFS project supports AWS S3, Azure Blob Storage and Google Cloud Storage (GCS) as its underlying storage service. It is API compatible with S3 and integrates seamlessly with popular data frameworks such as Spark, Hive, dbt, Trino, and many others.

{: .pb-5 }


## New! lakeFS Playground

Experience lakeFS first hand with your own isolated environment.
You can easily integrate it with your existing tools, and feel lakeFS in action in an environment similar to your own.

<p style="margin-top: 40px;">
    <a class="btn btn-green" href="https://demo.lakefs.io/" target="_blank">
        Try lakeFS now without installing
    </a>
</p>

## How do I use lakeFS?

lakeFS is designed to minimize adoption friction by maintaining compatibility with the S3 API. You can use it as a drop-in replacement for S3 from the perspective of any tool interacting with a data lake.

For example, take the common operation of reading a collection of data from object storage into a Spark DataFrame. For data outside a lakeFS repo, the code will look like:

```df = spark.read.parquet(“s3a://my-bucket/collections/foo/”)```

Once the data collections in my-bucket get added to a repository, the same operation becomes:

```df = spark.read.parquet(“s3a://my-repo/main-branch/collections/foo/”)```

As this example illustrates, you can use the same methods and syntax you already use to read and write data when using a lakeFS repository. This makes adoption process of lakeFS minimal, and can be done incrementally.



## Why is lakeFS the data solution you've been missing?

Working with data in a lakeFS repository––as opposed to a bucket––enables simplified workflows when developing data lake pipelines.

Never again will you spend time doing the following tasks:

* **Copying objects between prefixes to promote new data**
* **Deleting specific objects to recover from data errors**
* **Maintaining auxilliary jobs that populate a development environment with data**

If you execute any of these actions, adopting lakeFS will speed up development and deployment cycles, reduce the chance of incorrect data making it into production, and make it less painful in the event it does.

Through its versioning engine, lakeFS enables the following built-in operations familiar from git:

* **branch:** a consistent copy of a repository, isolated from other branches and their changes. Initial creation of a branch is a metadata operation that does not duplicate objects.
* **commit:** an immutable checkpoint containing a complete snapshot of a repository.
* **merge:** performed between two branches –– merges atomically update one branch with the changes from another.
* **revert:** return a repo to the exact state of a previous commit.
* **tag:** an immutable pointer to a single commit with a readable name.

*See the [CLI reference](./resources/commands.md) for the full list of commands.*

Incorporating these operations into your data lake pipelines provides the same collaboration and organizational benefits you get when managing application code with source control.

### The lakeFS promotion workflow

To illustrate, we’ll show how lakeFS *branches* and *merges* improve the universal process of updating collections with the latest data.

<img src="{{ site.baseurl }}/assets/img/promotion_workflow.png" alt="lakeFS promotion workflow" width="60%" height="60%" />

1. First, create a new **branch** from `main` to instantly generate a complete "copy" of your production data.
2. Apply changes or make updates on the isolated branch to understand their impact prior to exposure.
3. And finally, perform a **merge** from the feature branch back to main to atomically promote the updates into production.

Following this pattern, lakeFS facilitates a streamlined data deployment workflow that consistently produces data assets you can have total confidence in.

## What else does lakeFS do?

lakeFS helps you maintain a tidy data lake in several other ways, including:

#### Recovery from data errors
Erroneous data that makes it into production is an inevitability given the complex and fast-moving nature of modern data pipelines. Similarly, critical data assets are liable to accidental deletion by poorly configured jobs or due to human errors.

Today, recovering from these events relies on periodic backups that 1) may be out of date and 2) require sifting through data at the object level. This process is inefficient and can take hours, days, or in some cases, weeks to complete.
Recovering data in deletion events becomes an instant one-line operation with lakeFS using the ability to restore any historical commit.

Reverting your data lake back to previous version using our command-line tool is explained [here](https://docs.lakefs.io/reference/commands.html#lakectl-branch-revert).

#### Data reprocessing and backfills

Occasionally, we might need to reprocess historical data. This can be due to several reasons:

* Implementing new logic.
* Late arriving data that wasn’t included in previous analysis, and need to be backfilled after the fact.

This is tricky first and foremost because it often involves huge volumes of historical data. In addition, auditing requirements may necessitate keeping the old version of the data handy. 

lakeFS allows you to manage the reprocess on an isolated branch before merging to ensure the reprocessed data is exposed atomically. It also allows you to easily access the different versions of reprocessed data, using a commit ID.

#### Cross-collection consistency guarantees

Data engineers typically have to implement custom logic in scripts in order to guarantee two or more data assets are updated synchronously. The lakeFS merge operation from one branch into another removes the need to implement this logic yourself.

Instead, make updates to the desired data assets on a branch, and then utilize a lakeFS merge to atomically expose the data to downstream consumers.

To learn more about atomic cross-collection updates, [this video](https://www.youtube.com/watch?v=9OsjUvk5UJU) describes the concept in more detail.

#### Troubleshooting production problems

Data engineers are often asked to validate the data. A user might report inconsistencies, question the accuracy, or simply report it to be incorrect. Since the data continuously changes, it is challenging to understand its state at the time of the error

The best way to investigate, therefore, is to have a snapshot of the data as close as possible to the time of the error.
Once implementing a regular commit cadence in lakeFS, each commit represents an accessible historical snapshot of the data. When needed, a branch may be created from a commit ID to debug an issue in isolation.

To learn more on how to access a specific historical commit in a repository, see our seminal post on [data reproducibility](https://lakefs.io/solving-data-reproducibility/).

#### Establishing data quality guarantees

The best way to deal with mistakes is to avoid them. A data source that is ingested into the lake introducing low-quality data should be blocked before exposure if possible.

With lakeFS, you can achieve this by tying data quality tests to commit and merge operations via lakeFS [hooks](./setup/hooks.md).


### Additional things you should know about lakeFS: 

* It is format agnostic
* Your data stays in place
* It minimizes data duplication via a copy-on-write mechanism
* It maintains high performance over data lakes of any size
* It includes configurable garbage collection capabilities
* It is highly available and production ready

<img src="{{ site.baseurl }}/assets/img/lakeFS_integration.png" alt="lakeFS integration into data lake" width="60%" height="60%" />


### Downloads

#### Binary Releases

Binary packages are available for Linux/macOS/Windows on [GitHub Releases](https://github.com/treeverse/lakeFS/releases){: target="_blank" }

#### Docker Images

Official Docker images are available at [https://hub.docker.com/r/treeverse/lakefs](https://hub.docker.com/r/treeverse/lakefs){: target="_blank" }


### Next steps

Get started and [set up lakeFS on your preferred cloud environemnt](https://docs.lakefs.io/deploy/)
