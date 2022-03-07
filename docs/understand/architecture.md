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

lakeFS stores data in an underlying object, ([S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage) or [ABS](https://azure.microsoft.com/en-us/services/storage/blobs/)) store with some of its metadata stored in [PostgreSQL](https://www.postgresql.org/){:target="_blank"}. (see [Data Model](data-model.md))

![Architecture]({{ site.baseurl }}/assets/img/arch.png)

## Deployment models

lakeFS releases includes [binaries](https://github.com/treeverse/lakeFS/releases) for common operating systems and a [containerized option](https://hub.docker.com/r/treeverse/lakefs).
Check out our guides for running lakeFS on [K8S](../deploy/k8s.md), [ECS](../deploy/aws.md#on-ecs), [Google Compute Engine](../deploy/gcp.md#on-google-compute-engine) and [more](../deploy/).

### Load Balancing

lakeFS receives HTTP requests through 3 components: S3 Gateway, OpenApi Gateway and the Frontend UI.
lakeFS uses a single port that listens to all 3 endpoints, so for most use-cases a single load-balancer pointing
to lakeFS server(s) would do.

## lakeFS Components

### S3 Gateway

The API Gateway implements lakeFS' compatibility with S3. It implements a compatible subset of the S3 API to ensure most data systems can use lakeFS as a drop-in replacement for S3.

To achieve this, the gateway exposes an HTTP listener on a dedicated host and port.

See the [S3 API Reference](../reference/s3.md) section for information on supported API operations.

### OpenAPI Server

The Swagger ([OpenAPI](https://swagger.io/docs/specification/basic-structure/){:target="_blank"}) Server exposes the full set of lakeFS operations (see [Reference](../reference/api.md)). This includes basic CRUD operations against repositories and objects, as well as versioning related operations such as branching, merging, committing and reverting changes to data.

### Storage Adapter

The Storage Adapter is the component in charge of communication with the underlying object store. 
It is logically decoupled from the S3 Gateway to allow for compatibility with other types of underlying storage such as GCS or Azure Blob Storage, or non-production usages such as the local storage adapter.

See the [roadmap](roadmap.md) for information on future plans for storage compatibility. 

### Graveler

Graveler handles lakeFS versioning by translating lakeFS addresses to the actual stored objects.
To learn about the data model used to store lakeFS metadata, see the [data model section](data-model.md).

### Authentication & Authorization Service

The Auth service handles creation, management and validation of user credentials and [RBAC policies](https://en.wikipedia.org/wiki/Role-based_access_control){:target="_blank"}.

The credential scheme, along with the request signing logic are compatible with AWS IAM (both [SIGv2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html) and [SIGv4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)).

Currently, the auth service manages its own database of users and credentials and does not use IAM in any way. 

### Hooks Engine

The hooks engine enables CI/CD for data by triggering user defined [Actions](../setup/hooks.md) that will run during commit/merge. 

### UI

The UI layer is a simple browser-based client that uses the OpenAPI server. It allows management, exploration and data access to repositories, branches, commits and objects in the system.

## lakeFS Clients

There are several ways of interacting with lakeFS. Many data applications have different use-cases for lakeFS and 
we try to support as many as possible.

### OpenAPI Generated SDKs

OpenAPI specification can be used to generate lakeFS clients for many programming languages.
For example, the python [lakefs-client](https://pypi.org/project/lakefs-client/) or the [Java Client](https://search.maven.org/artifact/io.lakefs/api-client).

### lakectl

[lakectl](../reference/commands.md) is a CLI tool that enables lakeFS operations from your preferred terminal.

### Spark Client

The lakeFS [spark-client](../reference/spark-client.md) makes it easy to perform
ETL operations on lakeFS stored data, like [garbage-collection](../reference/garbage-collection.md) or [export](../reference/export.md).

### lakeFS Hadoop FileSystem

The [filesystem](../integrations/spark.md#access-lakefs-using-the-lakefs-specific-hadoop-filesystem) increases Spark ETL jobs performance by executing the metadata operations on the lakeFS server,
and all data operations directly through the same underlying object store that lakeFS uses.