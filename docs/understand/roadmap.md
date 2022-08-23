---
layout: default
title: Roadmap
parent: Understanding lakeFS
description: New features and improvements lined-up for lakeFS. Become part of building the lakeFS roadmap.
nav_order: 40
has_children: false
redirect_from: ../roadmap.html
---

# Roadmap
{: .no_toc }

{% include toc.html %}

---
## Ecosystem

### Repository Templates: Easily use lakeFS with your data stack <span>High Priority</span>{: .label .label-blue }

A wizard will walk you through launching a repository tailored for your use case.
Integrate with Spark, Hive Metastore and your other tools.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/3411){: target="_blank" class="btn" }

### Table format support

Currently, lakeFS supports merging and comparing references by doing an object-wise comparison. 
For unstructured data and some forms of tabluar data (namely, Hive structured tables), this works fine. 

However, in some cases simply creating a union of object modifications from both references isn't good enough. 
Modern table formats such as Delta Lake, Hudi, and Iceberg rely on a set of manifest or log files that describe the logical structure of the table. 
In those cases, a merge operation might have to be aware of the structure of the data: 
generate a new manifest or re-order the log in order for the output to make sense.
Additionally, the definition of a conflict is also a bit different: 
simply looking at object names to determine whether or not a conflict occured might not be good enough.

With that in mind, we plan to make the diff and merge operations pluggable. 
lakeFS already supports injecting custom behavior using hooks. Ideally, we can support this by introducing `on-diff` and `on-merge` hooks that allow implementing hooks in different languages, possibly utilizing existing code and libraries to aid with understanding these formats.

Once this is done, one may implement:

#### Delta Lake merges and diffs across branches

Delta lake stores metadata files that represent a [logical transaction log that relies on numerical ordering](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries).

Currently, when trying to modify a Delta table from two different branches, lakeFS would correctly recognize a conflict: this log diverged into two different copies, representing different changes.
Users would then have to forgo one of the change sets by either retaining the destination's branch set of changes or the source's branch.

A much better user experience would be to allow merging this log into a new unified set of changes, representing changes made in both branches as a new set of log files (and potentially, data files too!).

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/3380){: target="_blank" class="btn" }


#### Iceberg support <span>High Priority</span>{: .label .label-blue }

A table in Iceberg points to a single metadata file, containing a "location" property. Iceberg uses this location to store:
1. Manifests describing where the data is stored.
2. The actual data.

Once a table is created, the location property doesn't change. Therefore, a branch creation in lakeFS in meaningless, since new data added to this branch will be added to the main branch. There are some workarounds for this, but it is our priority to create an excellent integration with Iceberg.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/3381){: target="_blank" class="btn" }

### Hadoop 3 support in all lakeFS clients <span>High Priority</span>{: .label .label-blue }

We intend to test, verify and document the version support matrix for our Hadoop-based clients:
1. [lakeFS Hadoop Filesystem](https://github.com/treeverse/lakeFS/tree/master/clients/hadoopfs)
2. [Spark metadata client](https://github.com/treeverse/lakeFS/tree/master/clients/spark)
3. [RouterFS](https://github.com/treeverse/hadoop-router-fs)

In particular, all features will support Hadoop versions 3.x.


### Native Spark OutputCommitter

We plan to add a Hadoop OutputCommitter that uses existing lakeFS operations for atomic commits that are efficient and safely concurrent.

This comes with several benefits:

- Performance: This committer does metadata operations only and doesn't rely on copying data.
- Atomicity: A commit in lakeFS is guaranteed to either succeed or fail, but will not leave any intermediate state on failure.
- Traceability: Attaching metadata to each commit means we get quite a lot of information on where data is coming from, how it's generated, etc. This allows building reproducible pipelines in an easier way.
- Resilience: Since every Spark write is a commit, it's also undoable by reverting it.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/spark-outputcommitter/committer.md){: target="_blank" class="btn" }


### Native connector: Trino

Currently, the Trino integration works well using the [lakeFS S3 Gateway](architecture.md#s3-gateway). 

While easy to integrate and useful out-of-the-box, due to the S3 protocol, it means that the data itself must pass through the lakeFS server.

For larger installations, a native integration where lakeFS handles metadata and returns locations in the underlying object store that Trino can then access directly would allow reducing the operational overhead and increasing the scalability of lakeFS.
This would be done in a similar way to the [Native Spark integration](../integrations/spark.md) using the [Hadoop Filesystem implementation](../integrations/spark.md#access-lakefs-using-the-lakefs-specific-hadoop-filesystem).

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/2357){: target="_blank" class="btn" }

### Improved streaming support for Apache Kafka

Committing (along with attaching useful information to the commit) makes a lot of sense for batch workloads: 
- run a job or a pipeline on a separate branch and commit,
- record information such as the git hash of the code executed, the versions of frameworks used, and information about the data artifacts,
- once the pipeline has completed successfully, commit, and attach the recorded information as metadata.


For streaming, however, this is currently less clear: There's no obvious point in time to commit as things never actually "finish successfully".
[The recommended pattern](../using_lakefs/production.md#example-1-rollback---data-ingested-from-a-kafka-stream) would be to ingest from a stream on a separate branch, periodically committing - storing not only the data added since last commit but also capturing the offset read from the stream, for reproducibility.
These commits can then be merged into a main branch given they pass all relevant quality checks and other validations using hooks, exposing consumers to validated, clean data.

In practice, implementing such a workflow is a little challenging. Users need to:

1. Orchestrate the commits and merge operations.
2. Figure out how to attach the correct offset read from the stream broker.
3. Handle writes coming in while the commit is taking place.

Ideally, lakeFS should provide tools to automate this, with native support for [Apache Kafka](https://kafka.apache.org/){: target="_blank" }.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/2358){: target="_blank" class="btn" }

### User management using OIDC providers <span>High Priority</span>{: .label .label-blue }

Allow admins to manage their users externally using any OIDC-compatible provider.

### Reproducible data images
Make it easy for users to compose data coming from different sources (lakeFS repositories & references, external object store locations) as input for processing jobs and data science experiments.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/pull/3657){: target="_blank" class="btn" }

## Versioning Capabilities

### Support asynchronous hooks <span>High Priority</span>{: .label .label-blue }

Support running hooks that might possibly take many minutes to complete. 
This is useful for things such as data quality checks - where we might want to do big queries or scans to ensure the data being merged adheres to certain business rules.

Currently, `pre-commit` and `pre-merge` hooks in lakeFS are tied to the lifecycle of the API request that triggers the said commit or merge operation.
In order to support long running hooks, there are enhancements to make to lakeFS APIs in order to support an asynchronous commit and merge operations that are no longer tied to the HTTP request that triggered them.

### Support Garbage Collection on Azure <span>High Priority</span>{: .label .label-blue }

The lakeFS [Garbage Collection](https://docs.lakefs.io/reference/garbage-collection.html) capability hard-deletes objects deleted from branches, helping users reduce costs and 
comply with data privacy policies. Currently, lakeFS only supports Garbage Collection of S3 objects managed by lakeFS. Extending the support to Azure will allow lakeFS users that use Azure as their underlying storage to use this feature.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/3271){: target="_blank" class="btn" }

### Garbage Collection on Google Cloud Platform

The lakeFS [Garbage Collection](https://docs.lakefs.io/reference/garbage-collection.html) capability hard-deletes objects deleted from branches, helping users reduce costs and 
comply with data privacy policies. Currently, lakeFS only supports Garbage Collection of S3/Azure objects managed by lakeFS. Extending the support to GCP will allow lakeFS users that use GCP as their underlying storage to use this feature.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/issues/3626){: target="_blank" class="btn" }

### Collaborate on your data

Use lakeFS to comment, review and request changes before your data reaches consumers.

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/pull/3741){: target="_blank" class="btn" }


## Architecture

### Decouple ref-store from PostgreSQL <span>High Priority</span>{: .label .label-blue }

Currently, lakeFS requires a PostgreSQL database. Internally, it's used to store references (branches, tags, etc.), uncommitted objects metadata, and other metadata such as user management.

Making this store a pluggable component would allow the following:

1. Simpler quickstart using **only an object store**: allow running lakeFS without any dependencies. This ref-store will use the underlying object store to also store the references. For S3 (or any object store that doesn't support any native transaction/compare-and-swap semantics), this will be available only when running in single-instance mode. This is still beneficial for running lakeFS in POC or development mode, removing the need to run and connect multiple Docker containers.
1. Flexible production setup: A PostgreSQL option will still be available, but additional implementations will also be possible. Using other RDBMS types such as MySQL &emdash; or using managed services such as DynamoDB that lakeFS will be able to manage itself.
1. Easier scalability: Scaling RDBMS for very high throughput while keeping it predictable in performance for different loads and access patterns has a very high operational cost.

This release will mark the completion of project **["lakeFS on the Rocks"](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){:target="_blank"}**

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/metadata_kv/index.md){: target="_blank" class="btn" }

### Ref-store implementation for DynamoDB <span>High Priority</span>{: .label .label-blue }

Once we've decoupled the ref-store from PostgreSQL, we'd like to create a ref-store implementation that supports DynamoDB.
This has several advantages for users looking to run lakeFS on AWS:

1. DynamoDB is fast to provision and requires very little configuration.
1. The operational overhead of maintaining a serverless database is very small.
1. Scaling according to usage is much more fine grained, which eliminates a lot of the cost for smaller installations (as opposed to RDS).

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/metadata_kv/index.md#databases-that-meet-these-requirements-examples){: target="_blank" class="btn" }

### Ref-store implementation for RocksDB (for testing and experimentation)

Once we've decoupled the ref-store from PostgreSQL, we'd like to create a ref-store implementation that supports running with an embedded RocksDB database.
While not fit for real world production use, it makes trying lakeFS when running locally easier - either by directly executing the binary or doing a single `docker run` with the right configuration (as opposed to having to use `docker-compose` or run PostgreSQL locally).

[Track and discuss it on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/metadata_kv/index.md#databases-that-meet-these-requirements-examples){: target="_blank" class="btn" }
