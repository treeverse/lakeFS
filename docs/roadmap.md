---
layout: default
title: Roadmap
description: New features and improvements are lined up next for lakeFS. We would love you to be part of building lakeFS’s roadmap.
nav_order: 8
has_children: false
---

# Roadmap
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}


## Architecture

_TL;DR_ - After receiving feedback on early versions of lakeFS, project **["lakeFS on the Rocks"](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){:target="_blank"}** represents a set of changes to the architecture and data model of lakeFS. The main motivators are simplicity, reduced barriers of entry, scalability -  and the added benefit of having lakeFS adhere more closely to Git in semantics and UX.
{: .note .note-warning }

There are 3 big shifts in design:


1. Using the underlying object store as a source of truth for all committed data. We do this by storing commits as RocksDB SSTable files, where each commit is a “snapshot” of a given repository, split across multiple SSTable files that could be reused across commits.
1. Removal of PostgreSQL as a dependency: scaling it for very high throughput while keeping it predictable in performance for different loads and access patterns has a very high operational cost.
1. Separating out the metadata operations into a separate service (with the S3 API Gateway remaining as a stateless client for this service). Would allow for the development of “native” clients for big data tools that don’t require passing the data itself through lakeFS, but rather talk directly to the underlying object store.

The change will be made gradually over (at least) 3 releases:

### lakeFS On The Rocks: Milestone #1 - Committed Metadata Extraction
The initial release of the new lakeFS design will include the following changes:
1. Commit metadata stored on S3 in [SSTable format](https://blog.lowentropy.info/topics/deep-into-rocksdb/sstable-format-blockbased)
1. Uncommitted entries will be stored in PostgreSQL
1. Refs (branches, tags) will be stored in PostgreSQL

This release doesn't change the way lakeFS is deployed or operates - but represents a major change in data model (moving from legacy MVCC data model, to a Merkle tree structure, which brings lakeFS much closer to Git).

While there's still a strong dependency on PostgreSQL, the schema and access patterns are much simpler, resulting in improved performance and reduced operational ovearhead.

### lakeFS On The Rocks: Milestone #2  - Metadata Service
Extracting metadata into its own service:
1. [gRPC](https://grpc.io/) service that exposes metadata operations
1. S3 gateway API and OpenAPI servers become stateless and act as clients for the metadata service
1. Native [Hadoop Filesystem](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html) implementation on top of the metadata service, with [Apache Spark](https://spark.apache.org/) support (depends on Java native SDK for metadata server below)

### lakeFS On The Rocks: Milestone #3 - Remove PostgreSQL
Removing PostgreSQL for uncommitted data and refs, moving to [Raft](https://raft.github.io/):
1. Turn the metadata server into a Raft consensus group, managing state in [RocksDB](https://rocksdb.org/)
1. Remove dependency on PostgreSQL
1. Raft snapshots stored natively on underlying object stores

This release will mark the completion of project **["lakeFS on the Rocks"](https://docs.google.com/document/d/1jzD7-jun-tdU5BGapmnMBe9ovSzBvTNjXCcVztV07A4/edit?usp=sharing){:target="_blank"}** 

## Operations


### Kubernetes operator for gateways and Metadata servers
We see Kubernetes as a first class deployment target for lakeFS. While deploying the stateless components such as the S3 Gateway and the OpenAPI server is relatively easy, deploying the metadata service which is a stateful Raft group is a little more involved. Design is still pending while we learn more about the best practices (for example, the [Consul Kubernetes operator](https://www.consul.io/docs/k8s/installation/install#architecture)).

### Azure Data Lake Storage Support
Allow lakeFS to run natively on Azure, with full support for storing both metadata and data on [ADLS](https://azure.microsoft.com/en-us/services/storage/data-lake-storage/)

### Metadata operations security and access model
Reduce the operational overhead of managing access control: Currently operators working with both lakeFS and the native object store are required to manage a similar set of access controls for both.
Moving to a federated access control model using the object store’s native access control facilities (e.g. [IAM](https://aws.amazon.com/iam/)) will help reduce this overhead. This requires more discovery around the different use cases to help design something coherent. If you’re using lakeFS and have strong opinions about access control, please reach out on Slack.

## Clients


### Java native SDK for metadata server
Will be the basis of a [Hadoop Filesystem](http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html) implementation. It will allow compute engines to access the data directly without proxying it through lakeFS, improving operational efficiency and scalability.

### Python SDK for metadata server
In order to support automated data CI/CD integration with pipeline management and orchestration tools such as [Apache Airflow](https://airflow.apache.org/) and [Luigi](https://luigi.readthedocs.io/en/stable/) (see [Continuous Deployment](#use-case-continuous-deployment) below), a Python SDK for lakeFS metadata API is required. It will be used by the libraries native to each orchestration tool.

## Use Case: Development Environment

### Ephemeral branches with a TTL
Throwaway development or experimentation branches that live for a pre-configured amount of time, and are cleaned up afterwards. This is especially useful when running automated tests or when experimenting with new technologies, code or algorithms. We want to see what the outcome looks like, but don’t really need the output to live much longer than the duration of the experiment.


## Use Case: Continuous Integration

### Repo linking
The ability to explicitly depend on data residing in another repository. While it is possible to state these cross links by sticking them in the report’s commit metadata, we think a more explicit and structured approach would be valuable. Stating our dependencies in something that resembles a [pom.xml](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html#:~:text=A%20Project%20Object%20Model%20or,default%20values%20for%20most%20projects.) or [go.mod](https://github.com/golang/go/wiki/Modules#gomod) file would allow us to support better CI and CD integrations that ensure reproducibility without vendoring or copying data.

### Webhook Support
Being able to have pre-defined code execute before a commit or merge operation - potentially preventing that action from taking place. This allows lakeFS users to codify best practices (format and schema enforcement before merging to master) as well as run tools such as [Great Expectations](https://greatexpectations.io/) or [Monte Carlo](https://www.montecarlodata.com/) **before** data ever reaches consumers. 

### Protected Branches
A way to ensure certain branches (i.e. main or master) are only merged to, and are not being directly written to. In combination with [Webhook Support](#webhook-support) (see above), this allows users to provide a set of quality guarantees about a given branch (i.e., reading from 
master ensures schema never breaks and all partitions are complete and tested)

### Webhook Support integration: Metastore registration
Using webhooks, we can automatically register or update collections in a Hive/Glue metastore, using [Symlink Generation](https://docs.lakefs.io/using/glue_hive_metastore.html#create-symlink), this will also allow systems that don’t natively integrate with lakeFS to consume data produced using lakeFS.

### Webhook Support integration: Metadata validation
Provide a basic wrapper around something like [pyArrow](https://pypi.org/project/pyarrow/) that validates Parquet or ORC files for common schema problems such as backwards incompatibility.

## Use Case: Continuous Deployment

### Airflow Operators
Provide a set of reusable building blocks for Airflow that can create branches, commit and merge. The idea here is to enhance existing pipelines that, for example, run a series of Spark jobs, with an easy way to create a lakeFS branch before starting, passing that branch as a parameter to all Spark jobs, and upon successful execution, commit and merge their output to master.

### Webhook Support integration: Data Quality testing
Provide a webhook around a tool such as [Great Expectations](https://greatexpectations.io/) that runs data quality tests before merging into a main/master branch.


### Webhook Alerting
Support integration into existing alerting systems that trigger in the event a webhook returns a failure. This is useful for example when a data quality test fails, so new data is not merged into main/master due to a quality issue, so will alert the owning team.

