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

## Use Case: Development

### Ephemeral branches with a TTL
Throwaway development or experimentation branches that live for a pre-configured amount of time, and are cleaned up afterwards. This is especially useful when running automated tests or when experimenting with new technologies, code or algorithms. We want to see what the outcome looks like, but don’t really need the output to live much longer than the duration of the experiment.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2180){: target="_blank" class="btn" }


---

## Use Case: Deployment

### Repo linking
The ability to explicitly depend on data residing in another repository. While it is possible to state these cross-links by sticking them in the report’s commit metadata, we think a more explicit and structured approach would be valuable. Stating our dependencies in something that resembles a [pom.xml](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html#:~:text=A%20Project%20Object%20Model%20or,default%20values%20for%20most%20projects.){: .button-clickable} or [go.mod](https://github.com/golang/go/wiki/Modules#gomod){: .button-clickable} file would allow us to support better CI and CD integrations that ensure reproducibility without vendoring or copying data.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/1771){: target="_blank" class="btn" }

### Git-lakeFS integration
The ability to connect Git commits with lakeFS commits.
Especially useful for reproducibility: By looking at a set of changes to the **data**, be able to reference (or ever run) the job that produced it. 

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2073){: target="_blank" class="btn" }


---

### DBT Integration <span>High Priority</span>{: .label }

Allow DBT users to gain isolation for running in production, as well as being able to create zero-copy test and dev environments:
This is done by allowing the following:

1. Create a DBT environment that is mapped to a lakeFS branch. Since branches are zero-copy, these environments are verifiably identical to production without having to copy any data
2. Be able to run transformations on production data, with `dbt test` running before the data gets exposed to downstream consumers (via lakeFS merge)

This is an item that was often requested by DBT users, and is currently actively worked on.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2530){: target="_blank" class="btn" }


### Native Metastore Integration <span>High Priority</span>{: .label }

Create a robust connection between a Hive Metastore and lakeFS.
Ideally, metastore representations of tables managed in lakeFS should be versioned in the same way.

This will allow users to move across different branches or commits for both data and metadata, so that querying from a certain commit will always produce the same results.

Additionally, for CD use cases, it will allow a merge operation to introduce Hive table changes (schema evolution, partition addition/removal) atomically alongside the change to the data itself - as well as track those changes with the same set of commits - a lakeFS diff will show both metadata and data changes.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/1846){: target="_blank" class="btn" }


### Support Delta Lake merges and diffs across branches <span>Requires Discussion</span>{: .label .label-yellow }

New data formats ([Apache Hudi](https://hudi.apache.org/){: target="_blank" .button-clickable}, [Apache Iceberg](https://iceberg.apache.org/){: target="_blank" .button-clickable} and most notably, [Delta Lake](https://delta.io/){: target="_blank" .button-clickable}) don't rely simply on a hierarchical directory structure - they are usually accompanied by metadata files.
These files contain information about changes made, partition and indexing information, as well as represent small deltas to be applied to the larger, typically columnar data objects.

For Delta Lake in particular, these metadata files represent a [logical transaction log that relies on numerical ordering](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#delta-log-entries){: .button-clickable}.

Currently, when trying to modify a Delta table from 2 different branches, lakeFS would correctly recognize a conflict: this log diverged into 2 different copies, representing different changes.
Users would then have to forgo one of the change sets, by either retaining the destination's branch set of changes, or the source's branch.

A much better user experience would be to allow merging this log into a new unified set of changes, representing changes made in both branches as a new set of log files (and potentially, data files too!).

While beneficial for Delta Lake users, this is a departure from the un-opinionated nature of lakeFS, that is kept simple by treating objects as opaque blobs.
We're still gathering information on the use cases and access patterns where this is beneficial - please let us know if this is something you'd find interesting


[Contact us, we'd love to talk about it!](mailto:hello@treeverse.io?subject=using+lakeFS+with+Delta+Lake){: target="_blank" class="btn" }



### Hooks: usability improvements <span>High Priority</span>{: .label }

While hooks are an immensely useful tool that provides strong guarantees to data consumers, we want to make them more useful but also easier to implement:

#### Extended hook types beyond webhooks

While webhooks are easy to understand, they can be challenging in terms of operations: they require a running server listening for requests,
 network access and authentication information need to be applied between lakeFS and the hook server and network timeouts might interfere with long running hooks.

 To help reduce this burden, we plan on adding more hook types that are easier to manage - running a command line script, executing a docker image or calling out to an external orchestration/scheduling system such as Airflow or even Kubernetes. 

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2231){: target="_blank" class="btn" }

#### Expose merged snapshot to pre-merge hooks

pre-merge hooks are a great place to introduce data validation checks. However, currently lakeFS exposes the source branch, the destination branch and the diff between them. In many cases, the desired input is actually the merged view of both branches. By having a referencable commit ID that could be passed to e.g. Spark,
users will be able to directly feed the merged view into a dataframe, a testing framework, etc.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/1742){: target="_blank" class="btn" }

#### Create and edit hooks directly in the UI:

Hooks require a YAML configuration file to describe the required functionality and triggers. Instead of having to edit them somewhere else and then carefully uploading to the correct path for them to take effect, we want to allow creating and editing hooks with a single click in the UI

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2232){: target="_blank" class="btn" }


#### One-click reusable hook installation

Expanding on the abilities above (executing hooks locally as a command line or docker container - and the ability to create hooks in the UI), lakeFS
can expose a set of pre-defined hooks that could be installed in a single click through the UI and provide useful functionality (schema validation, metastore updates, format enforcement, Symlink generation - and more, etc).

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2233){: target="_blank" class="btn" }

## Use Case: Production

### Webhook Alerting
Support integration into existing alerting systems that trigger in the event a webhook returns a failure. This is useful for example when a data quality test fails, so new data is not merged into main due to a quality issue, so will alert the owning team.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2183){: target="_blank" class="btn" }

---


## Architecture & Operations

_TL;DR_ - After receiving feedback on early versions of lakeFS, project **["lakeFS on the Rocks"](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){:target="_blank" .button-clickable}** represents a set of changes to the architecture and data model of lakeFS. The main motivators are simplicity, reduced barriers of entry, scalability -  and the added benefit of having lakeFS adhere more closely to Git in semantics and UX.
{: .note .note-warning }

There are 3 big shifts in design:


1. ~~Using the underlying object store as a source of truth for all committed data. We do this by storing commits as RocksDB SSTable files, where each commit is a "snapshot" of a given repository, split across multiple SSTable files that could be reused across commits.~~
1. ~~Expose metadata operations as part of the OpenAPI gateway to allow other client types (e.g., Hadoop FileSystem) except for the S3 gateway interface~~
1. Implement a pluggable ref-store that allows storing references not (only) on PostgreSQL

### Decouple ref-store from PostgreSQL

Currently lakeFS requires a PostgreSQL database. Internally, it is used to store references (branches, tags, etc) other metadata such as user management.

Making this store a pluggable component would allow the following:

1. Simpler quickstart using **only an object store**: allow running lakeFS without any dependencies. This ref-store will use the underlying object store to also store the references. For S3 (or any object store that doesn't support any native transaction/compare-and-swap semantics) this will be available only when running in single-instance mode. This is still beneficial for running lakeFS in POC or development mode, removing the need to run and connect multiple Docker containers.
1. Flexible production setup: A PostgreSQL option will still be available, but additional implementations will also be possible: running lakeFS as a Raft consensus group, using an other RDBMS types such as MySQL &emdash; or using managed services such as DynamoDB that lakeFS will be able to manage itself
1. Easier scalability: Scaling RDBMS for very high throughput while keeping it predictable in performance for different loads and access patterns has a very high operational cost.

This release will mark the completion of project **["lakeFS on the Rocks"](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){:target="_blank" .button-clickable}**

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/pull/1685){: target="_blank" class="btn" }


### Snowflake Support <span>Requires Discussion</span>{: .label .label-yellow }

TBD - We don't yet have concrete plans on how to handle Snowflake (and potentially other Data Warehouse/Database sources).
If you'd like to have data in Snowflake managed by lakeFS, with full branching/merging/CI/CD capabilities, please contact us!

[Contact us, we'd love to talk about it!](mailto:hello@treeverse.io?subject=using+lakeFS+with+Snowflake){: target="_blank" class="btn" }


### Metadata operations security and access model <span>High Priority</span>{: .label }
Reduce the operational overhead of managing access control: Currently operators working with both lakeFS and the native object store are required to manage a similar set of access controls for both.
Moving to a federated access control model using the object store’s native access control facilities (e.g. [IAM](https://aws.amazon.com/iam/){:.button-clickable}) will help reduce this overhead. This requires more discovery around the different use cases to help design something coherent. If you’re using lakeFS and have strong opinions about access control, please reach out on Slack.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2184){: target="_blank" class="btn" }


### Native Spark OutputCommitter

Provide a Spark OutputCommitter that actually... commits.
This allows creating atomic Spark writes that are automatically tracked in lakeFS as commits.

Each job will use its native job ID as (part of) a branch name for isolation, with the Output Committer doing a commit and merge operation to the requested branch on success. This has several benefits:

- Performance: This committer does metadata operations only, and doesn't rely on copying data
- Atomicity: A commit in lakeFS is guaranteed to either succeed or fail, but will not leave any intermediate state on failure.
- Allows incorporating simple hooks into the spark job: users can define a webhook to happen before such a merge is completed successfully
- Traceability: Attaching metadata to each commit means we get quite a lot of information on where data is coming from, how it's generated, etc. This allows building reproducible pipelines in an easier way.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2042){: target="_blank" class="btn" }


### Native connector: Trino

Currently, the Trino integration works well using the [lakeFS s3 Gateway](architecture.md#s3-gateway){: .button-clickable}. 

While easy to integrate and useful out of the box, due to the S3 protocol, it means that the data itself must pass through the lakeFS server.

For larger installations, a native integration where lakeFS handles metadata, returning locations in the underlying object store that Trino can then access directly, would allow reducing the operational overhead and increasing the scalability of lakeFS.
This would be done in a similar way to the [Native Spark integration](../integrations/spark.md){: .button-clickable} using the [Hadoop Filesystem implementation](../integrations/spark.md#access-lakefs-using-the-lakefs-specific-hadoop-filesystem){: .button-clickable}.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2357){: target="_blank" class="btn" }



### Improved streaming support for Apache Kafka

Committing (along with attaching useful information to the commit) makes a lot of sense for batch workloads: 
- run a job or a pipeline on a separate branch and commit 
- record information such as the git hash of the code executed, the versions of frameworks used, and information about the data artifacts
- once the pipeline has completed successfully, commit and attach the recorded information as metadata


For streaming however, this is currently less clear: There's no obvious point in time to commit, as things never actually "finish successfully".
[The recommended pattern](../usecases/production.md#example-1-rollback---data-ingested-from-a-kafka-stream){: .button-clickable} would be to ingest from a stream on a separate branch, periodically committing - storing not only the data added since last commit but also capturing the offset read from the stream, for reproducibility.
These commits can then be merged into a main branch given they pass all relevant quality checks and other validations using hooks, exposing consumers to validated, clean data.

In practice, implementing such a workflow is a little challenging. Users need to:

1. Orchestrate the commits and merge operations.
2. figure out how to attach the correct offset read from the stream broker
3. Handle writes coming in while the commit is taking place

Ideally, lakeFS should provide tools to automate this, with native support for [Apache Kafka](https://kafka.apache.org/){: target="_blank" .button-clickable}.

[Track and discuss on GitHub](https://github.com/treeverse/lakeFS/issues/2358){: target="_blank" class="btn" }
