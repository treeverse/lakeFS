---
layout: default
title: What is lakeFS
description: A lakeFS documentation website that provides information on how to use lakeFS to deliver resilience and manageability to data lakes.
nav_order: 0
redirect_from: ./downloads.html
---

# What is lakeFS
{: .no_toc }  

lakeFS is an open-source project that provides a git-like version control interface for data lakes, with seamless integration to most data tools and frameworks.

lakeFS enables you to easily implement parallel pipelines for experimentation, reproducibility and CI/CD for data.

lakeFS supports AWS S3, Azure Blob Storage and Google Cloud Storage (GCS) as its underlying storage service. It is [API compatible with S3](reference/s3.md) and works seamlessly with all modern data frameworks such as Spark, Hive, AWS Athena, Presto, etc.

<img src="{{ site.baseurl }}/assets/img/wrapper.png" alt="lakeFS" width="650px"/>


{: .pb-5 }


## New! lakeFS Playground

Experience lakeFS first hand with your own isolated environment.
You can easily integrate it with your existing tools, and feel lakeFS in action in an environment similar to your own.

<p style="margin-top: 40px;">
    <a class="btn btn-green" href="https://demo.lakefs.io/" target="_blank">
        Try lakeFS now without installing
    </a>
</p>

## Why you need lakeFS and what it can do

With lakeFS data teams can stop worrying about issues that take away much of their time:
* Roll back changes to data: recover quickly from bugs and mistakes
* Test and validate data before exposing it to consumers
* Travel back in time, across collections - for reproducing and debugging issues

Since lakeFS is compatible with the S3 API, all popular applications will work without modification, by simply adding the branch name to the object path:

<img src="{{ site.baseurl }}/assets/img/s3_branch.png" alt="lakeFS s3 addressing" width="60%" height="60%" />

## Benefits of lakeFS
* It is **format agnostic**
* Your data **stays in place**
* It helps you prevent data duplication by using **copy-on-write**
* It is **highly performant** for huge data lakes
* It includes **configurable garbage collection** capabilities
* lakeFS is **highly available and production ready**

<img src="{{ site.baseurl }}/assets/img/lakeFS_integration.png" alt="lakeFS integration into data lake" width="60%" height="60%" />

## Use-cases:

lakeFS enhances processing workflows at each step of the data lifecycle:

### In Development
* **Experiment** - try new tools, upgrade versions, and evaluate code changes in isolation. By creating a branch of the data you get an isolated snapshot to run experiments over, while others are not exposed. Compare between branches with different experiments or to the main branch of the repository to understand a change's impact.  
* **Debug** - checkout specific commits in a repository's commit history to materialize consistent, historical versions of your data. See the exact state of your data at the point-in-time of an error to understand its root cause.
* **Collaborate** - avoid managing data access at the two extremes of either 1) treating your data lake like a shared folder or 2) creating multiple copies of the data to safely collaborate. Instead, leverage isolated branches managed by metadata (not copies of files) to work in parallel.

[Learn more](./usecases/data-devenv.md){:id="user-content-learn-more-env"}

### During Deployment
* **Version Control** - retain commits for a configurable duration, so readers are able to query data from the latest commit or any other point in time. Writers atomically introduce new data preventing inconsistent data views.
* **Test** - define pre-merge and pre-commit hooks to run tests that enforce schema and validate properties of the data to catch issues before they reach production.

[Learn more](./usecases/ci.md){:id="user-content-learn-more-int"}

### In Production
* **Roll Back** - recover from errors by instantly reverting data to a former, consistent snapshot of the data lake. Choose any commit in a repository's commit history to revert in one atomic action.
* **Troubleshoot** - investigate production errors by starting with a snapshot of the inputs to the failed process. Spend less time re-creating the state of datasets at the time of failure, and more time finding the solution.
* **Cross-collection Consistency** - provide consumers multiple synchronized collections of data in one atomic, revertable action. Using branches, writers provide consistency guarantees across different logical collections - merging to the main branch only after all relevant datasets have been created or updated successfully.
   
[Learn more](./usecases/production.md){:id="user-content-learn-more-dep"}


## Downloads

### Binary Releases

Binary packages are available for Linux/macOS/Windows on [GitHub Releases](https://github.com/treeverse/lakeFS/releases){: target="_blank" }

### Docker Images

Official Docker images are available at [https://hub.docker.com/r/treeverse/lakefs](https://hub.docker.com/r/treeverse/lakefs){: target="_blank" }


## Next steps

Get started and [set up lakeFS on your preferred cloud environemnt](https://docs.lakefs.io/deploy/)
