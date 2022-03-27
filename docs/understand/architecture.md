---
layout: default
title: Architecture
parent: Understanding lakeFS
description: lakeFS architecture overview. Learn more about lakeFS components, including its S3 API gateway.
nav_order: 10
has_children: false
redirect_from:
    - ../architecture/index.html
    - ../architecture/overview.html
---
# Architecture Overview
{: .no_toc }


{% include toc.html %}

## Overview

lakeFS is distributed as a single binary encapsulating several logical services:

The server itself is stateless, meaning you can easily add more instances to handle bigger load.

lakeFS stores data in an underlying object store ([GCS](https://cloud.google.com/storage){: .button-clickable}, [ABS](https://azure.microsoft.com/en-us/services/storage/blobs/){: .button-clickable},
[S3](https://aws.amazon.com/s3/){: .button-clickable} or any S3-compatible stores like [MinIO](https://min.io/){: .button-clickable} or [Ceph](https://docs.ceph.com/){: .button-clickable}), with some of its metadata stored in [PostgreSQL](https://www.postgresql.org/){:target="_blank" .button-clickable} (see [Versioning internals](../understand/versioning-internals.md){: .button-clickable}).


<!-- The below draw.io diagram source can be found here: https://drive.google.com/file/d/1lctPtGVEmOlCNHi3jiW4XXmyQQFkxzyx/view?usp=sharing -->

![Architecture]({{ site.baseurl }}/assets/img/arch.png)

## Ways to deploy lakeFS

lakeFS releases includes [binaries](https://github.com/treeverse/lakeFS/releases){: .button-clickable} for common operating systems, a [containerized option](https://hub.docker.com/r/treeverse/lakefs){: .button-clickable} or 
an [Helm chart](https://artifacthub.io/packages/helm/lakefs/lakefs){: .button-clickable}.
Check out our guides for running lakeFS on [K8S](../deploy/k8s.md){: .button-clickable}, [ECS](../deploy/aws.md#on-ecs){: .button-clickable}, [Google Compute Engine](../deploy/gcp.md#on-google-compute-engine){: .button-clickable} and [more](../deploy/){: .button-clickable}.

### Load Balancing

Accessing lakeFS is done using HTTP.
lakeFS exposes a frontend UI, an [OpenAPI server](#openapi-server){: .button-clickable}, as well as an S3-compatible service (see [S3 Gateway](#s3-gateway){: .button-clickable} below).
lakeFS uses a single port that serves all 3 endpoints, so for most use-cases a single load-balancer pointing
to lakeFS server(s) would do.

## lakeFS Components

### S3 Gateway

S3 Gateway implements lakeFS's compatibility with S3. It implements a compatible subset of the S3 API to ensure most data systems can use lakeFS as a drop-in replacement for S3.

See the [S3 API Reference](../reference/s3.md){: .button-clickable} section for information on supported API operations.

### OpenAPI Server

The Swagger ([OpenAPI](https://swagger.io/docs/specification/basic-structure/){:target="_blank" .button-clickable}) server exposes the full set of lakeFS operations (see [Reference](../reference/api.md){: .button-clickable}). This includes basic CRUD operations against repositories and objects, as well as versioning related operations such as branching, merging, committing and reverting changes to data.

### Storage Adapter

The Storage Adapter is an abstraction layer for communicating with any underlying object store. 
Its implementations allow compatibility with many types of underlying storage such as S3, GCS, Azure Blob Storage, or non-production usages such as the local storage adapter.

See the [roadmap](roadmap.md){: .button-clickable} for information on future plans for storage compatibility. 

### Graveler

Graveler handles lakeFS versioning by translating lakeFS addresses to the actual stored objects.
<<<<<<< HEAD
To learn about the data model used to store lakeFS metadata, see the [data model section](versioning-internals.md).
=======
To learn about the data model used to store lakeFS metadata, see the [data model section](data-model.md){: .button-clickable}.
>>>>>>> b4a28217 (Understanding lakeFS, Commitment, Contributing and FAQ pages)

### Authentication & Authorization Service

The Auth service handles creation, management and validation of user credentials and [RBAC policies](https://en.wikipedia.org/wiki/Role-based_access_control){:target="_blank" .button-clickable}.

The credential scheme, along with the request signing logic are compatible with AWS IAM (both [SIGv2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html){: .button-clickable} and [SIGv4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html){: .button-clickable}).

Currently, the auth service manages its own database of users and credentials and does not use IAM in any way. 

### Hooks Engine

The hooks engine enables CI/CD for data by triggering user defined [Actions](../setup/hooks.md){: .button-clickable} that will run during commit/merge. 

### UI

The UI layer is a simple browser-based client that uses the OpenAPI server. It allows management, exploration and data access to repositories, branches, commits and objects in the system.

## Applications

As a rule of thumb, lakeFS supports any s3-compatible application. This means that many common data applications work with lakeFS out of the box.
Check out our [integrations](../integrations){: .button-clickable} to learn more.

## lakeFS Clients

Some data applications benefit from deeper integrations with lakeFS to support different use-cases or enhanced functionality which is provided by lakeFS clients.

### OpenAPI Generated SDKs

OpenAPI specification can be used to generate lakeFS clients for many programming languages.
For example, the Python [lakefs-client](https://pypi.org/project/lakefs-client/){: .button-clickable} or the [Java client](https://search.maven.org/artifact/io.lakefs/api-client){: .button-clickable} are published 
with every new lakeFS release.

### lakectl

[lakectl](../reference/commands.md){: .button-clickable} is a CLI tool that enables lakeFS operations using the lakeFS API from your preferred terminal.

### Spark Metadata Client

The lakeFS [Spark Metadata Client](../reference/spark-client.md){: .button-clickable} makes it easy to perform
operations related to lakeFS metadata, at scale. Examples include [garbage collection](../reference/garbage-collection.md){: .button-clickable} or [exporting data from lakeFS](../reference/export.md){: .button-clickable}.

### lakeFS Hadoop FileSystem

Thanks to the [S3 Gateway](#s3-gateway){: .button-clickable}, it is possible to interact with lakeFS using Hadoop's S3AFIleSystem, 
but due to limitations of the S3 API, doing so requires reading and writing data objects through the lakeFS server.
Using [lakeFSFileSystem](../integrations/spark.md#access-lakefs-using-the-lakefs-specific-hadoop-filesystem){: .button-clickable} increases Spark ETL jobs performance by executing the metadata operations on the lakeFS server,
and all data operations directly through the same underlying object store that lakeFS uses.
