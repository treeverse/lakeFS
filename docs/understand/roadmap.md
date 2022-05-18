---
layout: default
title: Roadmap
parent: Understanding lakeFS
description: New features and improvements are lined-up next for lakeFS. We would love you to be part of building lakeFS’s roadmap.
nav_order: 40
has_children: false
redirect_from: ../roadmap.html
---

# Roadmap
{: .no_toc }

{% include toc.html %}

---
## Ecosystem

### Snowflake Support: External tables on lakeFS <span>Requires Discussion</span>{: .label .label-yellow }

Since Snowflake supports reading external tables from an object store, we'd like to extend this support to work with lakeFS repositories that are hosted on top of [supported object stores](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro.html#supported-cloud-storage-services){: target="_blank" }.
This could be done by utilizing Snowflake's support for `SymlinkInputFormat` similar to how [Delta Lake support is implemented](https://docs.databricks.com/delta/snowflake-integration.html){: target="_blank" }, and later on by having a native integration with Snowflake itself. If you'd like to hear more:

[Contact us, we'd love to talk about it!](mailto:hello@treeverse.io?subject=using+lakeFS+with+Snowflake){: target="_blank" class="btn" }


### Pluggable diff/merge operators

Currently, lakeFS supports merging and comparing references by doing an object-wise comparison. 
For unstructured data and some forms of tabluar data (namely Hive structured tables), this works fine. 

However, in some cases, simply creating a union of object modifications from both references isn't good enough. 
Modern table formats such as Delta Lake, Hudi and Iceberg rely on a set of manifest or log files that describe the logical structure of the table. 
In those cases, a merge operation might have to be aware of the structure of the data: 
generate a new manifest or re-order the log in order for the output to make sense.
Additionally, the definition of a conflict is also a bit different: 
simply looking at object names to determine whether or not a conflict occured might not be good enough.

With that in mind, we plan on making the diff and merge operations pluggable. 
lakeFS already supports injecting custom behavior using hooks. Ideally, we can support this by introducing `on-diff` and `on-merge` hooks that allow implementing hooks in different languages, possibly utilizing existing code and libraries to aid with understanding these formats.

Once implemented we could support:

#### Delta Lake merges and diffs across branches

Delta lake stores metadata files that represent a [logical transaction log that relies on numerical ordering](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries).

Currently, when trying to modify a Delta table from 2 different branches, lakeFS would correctly recognize a conflict: this log diverged into 2 different copies, representing different changes.
Users would then have to forgo one of the change sets, by either retaining the destination's branch set of changes, or the source's branch.

A much better user experience would be to allow merging this log into a new unified set of changes, representing changes made in both branches as a new set of log files (and potentially, data files too!).

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/3380){: target="_blank" class="btn" }


#### Iceberg merges and diffs across branches <span>High Priority</span>{: .label .label-blue }

Iceberg stores metadata files ("manifests") that represent a [snapshot of a given Iceberg table](https://iceberg.apache.org/spec/#manifests).

Currently, when trying to modify an Iceberg table from 2 different branches, lakeFS would not be able to create a logical snapshot that consists of the changes applied in both references.
Users would then have to forgo one of the change sets, by either retaining the destination's branch set of changes, or the source's branch.

A much better user experience would be to allow merging snapshots into a new unified set of changes, representing changes made in both refs as a new snapshot.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/3381){: target="_blank" class="btn" }



### Native Spark OutputCommitter

Add a Hadoop OutputCommitter that uses existing lakeFS operations for atomic commits that are efficient and safely concurrent.

This has several benefits:

- Performance: This committer does metadata operations only, and doesn't rely on copying data
- Atomicity: A commit in lakeFS is guaranteed to either succeed or fail, but will not leave any intermediate state on failure.
- Traceability: Attaching metadata to each commit means we get quite a lot of information on where data is coming from, how it's generated, etc. This allows building reproducible pipelines in an easier way.
- Resilience: Since every Spark write is a commit, it is also undoable by reverting it.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/spark-outputcommitter/committer.md){: target="_blank" class="btn" }


### Native connector: Trino

Currently, the Trino integration works well using the [lakeFS s3 Gateway](architecture.md#s3-gateway). 

While easy to integrate and useful out of the box, due to the S3 protocol, it means that the data itself must pass through the lakeFS server.

For larger installations, a native integration where lakeFS handles metadata, returning locations in the underlying object store that Trino can then access directly, would allow reducing the operational overhead and increasing the scalability of lakeFS.
This would be done in a similar way to the [Native Spark integration](../integrations/spark.md) using the [Hadoop Filesystem implementation](../integrations/spark.md#access-lakefs-using-the-lakefs-specific-hadoop-filesystem).

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2357){: target="_blank" class="btn" }


### Improved streaming support for Apache Kafka

Committing (along with attaching useful information to the commit) makes a lot of sense for batch workloads: 
- run a job or a pipeline on a separate branch and commit 
- record information such as the git hash of the code executed, the versions of frameworks used, and information about the data artifacts
- once the pipeline has completed successfully, commit and attach the recorded information as metadata


For streaming however, this is currently less clear: There's no obvious point in time to commit, as things never actually "finish successfully".
[The recommended pattern](../usecases/production.md#example-1-rollback---data-ingested-from-a-kafka-stream) would be to ingest from a stream on a separate branch, periodically committing - storing not only the data added since last commit but also capturing the offset read from the stream, for reproducibility.
These commits can then be merged into a main branch given they pass all relevant quality checks and other validations using hooks, exposing consumers to validated, clean data.

In practice, implementing such a workflow is a little challenging. Users need to:

1. Orchestrate the commits and merge operations.
2. figure out how to attach the correct offset read from the stream broker
3. Handle writes coming in while the commit is taking place

Ideally, lakeFS should provide tools to automate this, with native support for [Apache Kafka](https://kafka.apache.org/){: target="_blank" }.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2358){: target="_blank" class="btn" }


## Versioning Capabilities

### Git-lakeFS integration
The ability to connect Git commits with lakeFS commits.
Especially useful for reproducibility: By looking at a set of changes to the **data**, be able to reference (or ever run) the job that produced it. 

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2073){: target="_blank" class="btn" }


### Support asyncronous hooks <span>High Priority</span>{: .label .label-blue }

support running hooks that might possibly take many minutes to complete. 
This is useful for things such as data quality checks - where we might want to do big queries or scans to ensure the data being merged adheres to certain business rules.

Currently, `pre-commit` and `pre-merge` hooks in lakeFS are tied to the lifecycle of the API request that triggers the said commit or merge operation.
In order to support long running hooks, there are enhancements to make to lakeFS APIs in order to support an asynchronous commit and merge operations that are no
longer tied to the HTTP request that triggered them.


### Partial Commits

In some cases, lakeFS users might want to commit only a set of staged objects instead of all modifications made to a branch
This is especially useful when experimenting with multiple data sources, but we only care about an output as opposed to intermediate state.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2512){: target="_blank" class="btn" }


### Rebase

Since some users are interested in achieving parity between their Git workflow and their lakeFS workflow, extending the merge behavior to support
history rewriting in which the changes that occured in the source branch are then reapplied to the HEAD of the destination branch (aka "rebase") is desired.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2739){: target="_blank" class="btn" }

## Architecture


### Decouple ref-store from PostgreSQL <span>High Priority</span>{: .label .label-blue }

Currently lakeFS requires a PostgreSQL database. Internally, it is used to store references (branches, tags, etc), uncommitted objects metadata and other metadata such as user management.

Making this store a pluggable component would allow the following:

1. Simpler quickstart using **only an object store**: allow running lakeFS without any dependencies. This ref-store will use the underlying object store to also store the references. For S3 (or any object store that doesn't support any native transaction/compare-and-swap semantics) this will be available only when running in single-instance mode. This is still beneficial for running lakeFS in POC or development mode, removing the need to run and connect multiple Docker containers.
1. Flexible production setup: A PostgreSQL option will still be available, but additional implementations will also be possible:  Using other RDBMS types such as MySQL &emdash; or using managed services such as DynamoDB that lakeFS will be able to manage itself
1. Easier scalability: Scaling RDBMS for very high throughput while keeping it predictable in performance for different loads and access patterns has a very high operational cost.

This release will mark the completion of project **["lakeFS on the Rocks"](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){:target="_blank"}**

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/metadata_kv/index.md){: target="_blank" class="btn" }

### Ref-store implementation for DynamoDB

Once we've decoupled the ref-store from PostgreSQL, we'd like to create a ref-store implementation that supports DynamoDB.
This has several advantages for users looking to run lakeFS on AWS:

1. DynamoDB is fast to provision and requires very little configuration
1. The operational overhead of maintaining a serverless database is very small
1. Scaling according to usage is much more fine grained, which eliminates a lot of the cost for smaller installations (as opposed to RDS)

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/metadata_kv/index.md#databases-that-meet-these-requirements-examples){: target="_blank" class="btn" }


### Ref-store implementation for RocksDB (for testing and experimentation)

Once we've decoupled the ref-store from PostgreSQL, we'd like to create a ref-store implementation that supports running with an embedded RocksDB database.
While not fit for real world production use, it makes it much easier to try lakeFS when running locally, either by directly executing the binary, or by doing a single `docker run` with the right configuration (as opposed to having to use `docker-compose` or run PostgreSQL locally).

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/blob/master/design/open/metadata_kv/index.md#databases-that-meet-these-requirements-examples){: target="_blank" class="btn" }
