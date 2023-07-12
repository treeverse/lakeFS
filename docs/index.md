---
layout: default
title: lakeFS
description: The lakeFS documentation provides guidance on how to use lakeFS to deliver resilience and manageability to data lakes.
nav_order: 0
redirect_from: /downloads.html
---

# Welcome to the Lake! 

<img src="/assets/img/waving-axolotl-transparent.gif" width="90"/>


**lakeFS brings software engineering best practices and applies them to data engineering.** 

lakeFS provides version control over the data lake, and uses Git-like semantics to create and access those versions. If you know git, you'll be right at home with lakeFS.

With lakeFS, you can use concepts on your data lake such as **branch** to create an isolated version of the data, **commit** to create a reproducible point in time, and **merge** in order to incorporate your changes in one atomic action.

## How Do I Get Started? 

**[The hands-on quickstart](/quickstart) guides you through some of core features of lakeFS**. 

These include [branching](/quickstart/branch.html), [merging](quickstart/commit-and-merge.html), and [rolling back changes](quickstart/rollback.html) to data. 

{: .note}
You can use the [30-day free trial of lakeFS Cloud](https://lakefs.cloud/register) if you want to try out lakeFS without installing anything. 

## Key lakeFS Features

* It is format-agnostic.
* It works with numerous data tools and platforms.
* Your data stays in place.
* It minimizes data duplication via a copy-on-write mechanism.
* It maintains high performance over data lakes of any size.
* It includes configurable garbage collection capabilities.
* It is proven in production and has an active community.

<img src="/assets/img/lakeFS_integration.png" alt="lakeFS integration into data lake" width="60%" height="60%" />

## How Does lakeFS Work With Other Tools? 

lakeFS is an open source project that supports managing data in AWS S3, Azure Blob Storage, Google Cloud Storage (GCS) and any other object storage with an S3 interface. It integrates seamlessly with popular data frameworks such as Spark, Hive Metastore, dbt, Trino, Presto, and many others and includes an S3 compatibility layer.

{: .note}
For more details and a full list see [the integrations pages](/integrations/).

<p class="center">
    <img src="assets/what_is.png"/>
</p>

{: .pb-5 }

lakeFS maintains compatibility with the S3 API to minimize adoption
friction. You can use it as a drop-in replacement for S3 from the perspective of
any tool interacting with a data lake.

For example, take the common operation of reading a collection of data from an object storage into a Spark DataFrame. For data outside a lakeFS repo, the code will look like this:

```py
df = spark.read.parquet("s3a://my-bucket/collections/foo/")
```

After adding the data collections into my-bucket to a repository, the same operation becomes:

```py
df = spark.read.parquet("s3a://my-repo/main-branch/collections/foo/")
```

You can use the same methods and syntax you are already using to read and write data when using a lakeFS repository. This simplifies the adoption of lakeFS - minimal changes are needed to get started, making further changes an incremental process.

## lakeFS is Git for Data

Git conquered the world of code because it had best supported engineering best practices required by developers, in particular:

* Collaborate during development.
* Develop and Test in isolation
* Revert code repository to a sable version in case of an error
* Reproduce and troubleshoot issues with a given version of the code
* Continuously integrate and deploy new code (CI/CD)

lakeFS provides these exact benefits, that are data practitioners are missing today, and enables them a clear intuitive Git-like interface to easily manage their data like they manage code. Through its versioning engine, lakeFS enables the following built-in operations familiar from Git:

* **branch:** a consistent copy of a repository, isolated from other branches and their changes. Initial creation of a branch is a metadata operation that does not duplicate objects.
* **commit:** an immutable checkpoint containing a complete snapshot of a repository.
* **merge:** performed between two branches &mdash; merges atomically update one branch with the changes from another.
* **revert:** returns a repo to the exact state of a previous commit.
* **tag:** a pointer to a single immutable commit with a readable, meaningful name.

*See the [object model](./understand/model.md) for an in-depth
definition of these, and the [CLI reference](reference/cli.md) for the
full list of commands.*

Incorporating these operations into your data lake pipelines provides the same collaboration and organizational benefits you get when managing application code with source control.

### The lakeFS Promotion Workflow

Here's how lakeFS *branches* and *merges* improve the universal process of updating collections with the latest data.

<img src="/assets/img/promotion_workflow.png" alt="lakeFS promotion workflow" width="60%" height="60%" />

1. First, create a new **branch** from `main` to instantly generate a complete "copy" of your production data.
2. Apply changes or make updates on the isolated branch to understand their impact prior to exposure.
3. And finally, perform a **merge** from the feature branch back to main to atomically promote the updates into production.

Following this pattern, lakeFS facilitates a streamlined data deployment workflow that consistently produces data assets you can have total confidence in.

## How Can lakeFS Help Me?

lakeFS helps you maintain a tidy data lake in several ways, including:

### Isolated Dev/Test Environments with copy-on-write

lakeFS makes creating isolated dev/test environments for ETL testing instantaneous, and through its use of copy-on-write, cheap. This enables you to test and validate code changes on production data without impacting it, as well as run analysis and experiments on production data in an isolated clone. 

👉🏻 [Read more](/use_cases/etl_testing.html)

### Reproducibility: What Did My Data Look Like at a Point In Time?

Being able to look at data as it was at a given point is particularly useful in at least two scenarios: 

1. Reproducibility of ML experiments

    ML experimentation is usually an iterative process, and being able to reproduce a specific iteration is important. 
    
    With lakeFS you can version all components of an ML experiment including its data, as well as make use of copy-on-write to minimise the footprint of versions of the data

2. Troubleshooting production problems

    Data engineers are often asked to validate the data. A user might report inconsistencies, question the accuracy, or simply report it to be incorrect. 
    
    Since the data continuously changes, it is challenging to understand its state at the time of the error.

    With lakeFS you can create a branch from a commit to debug an issue in isolation.

👉🏻 [Read More](/use_cases/reproducibility.html)

### Rollback of Data Changes and Recovery from Data Errors

Human error, misconfiguration, or wide-ranging systematic effects are
unavoidable. When they do happen, erroneous data may make it into
production or critical data assets might accidentally by deleted.

By their nature, backups are a wrong tool for recovering from such events.
Backups are periodic events that are usually not tied to performing
erroneous operations. So, they may be out of date, and  will require
sifting through data at the object level. This process is inefficient and
can take hours, days, or in some cases, weeks to complete. By quickly
committing entire snapshots of data at well-defined times, recovering data
in deletion or corruption events becomes an instant one-line operation with
lakeFS: just identify a good historical commit, and then restore to it or
copy from it.

👉🏻 [Read more](/use_cases/rollback.html)

### Multi-Table Transactions guarantees

Data engineers typically need to implement custom logic in scripts to guarantee
two or more data assets are updated synchronously. This logic often
requires extensive rewrites or periods during which data is unavailable.
The lakeFS merge operation from one branch into another removes the need to
implement this logic yourself.

Instead, make updates to the desired data assets on a branch and then utilize a lakeFS merge to atomically expose the data to downstream consumers.

To learn more about atomic cross-collection updates, check out [this video](https://www.youtube.com/watch?v=9OsjUvk5UJU) which describes the concept in more detail, along with [this notebook](https://github.com/treeverse/lakeFS-samples/blob/main/00_notebooks/write-audit-publish/wap-lakefs.ipynb) in the [lakeFS samples repository](https://github.com/treeverse/lakeFS-samples/).



### Establishing data quality guarantees - CI/CD for data

The best way to deal with mistakes is to avoid them. A data source that is ingested into the lake introducing low-quality data should be blocked before exposure if possible.

With lakeFS, you can achieve this by tying data quality tests to commit and merge operations via lakeFS [hooks](./use_cases/cicd_for_data.md#using-hooks-as-data-quality-gates).

👉🏻 [Read more](/use_cases/cicd_for_data.html)

## Downloads

{: .note}
lakeFS is also available as a fully-managed hosted service on [lakeFS Cloud](https://lakefs.cloud/)

### Binary Releases

Binary packages are available for Linux/macOS/Windows on [GitHub Releases](https://github.com/treeverse/lakeFS/releases){: target="_blank" }

Or using [Homebrew](https://brew.sh/)

```sh
# add repository
brew tap treeverse/lakefs

# installing lakefs/lakectl
brew install lakefs
```

### Docker Images

The official Docker images are available at [https://hub.docker.com/r/treeverse/lakefs](https://hub.docker.com/r/treeverse/lakefs){: target="_blank" }
