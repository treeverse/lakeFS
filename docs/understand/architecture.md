---
title: Architecture
parent: Understanding lakeFS
description: lakeFS architecture overview. Learn more about lakeFS components, including its S3 API gateway.
redirect_from:
    - /architecture/index.html
    - /architecture/overview.html
---
# lakeFS Architecture

lakeFS is distributed as a single binary encapsulating several logical services.

The server itself is stateless, meaning you can easily add more instances to handle a bigger load.

![Architecture]({{ site.baseurl }}/assets/img/architecture.png)

{% include toc_2-3.html %}
### Object Storage

lakeFS stores data in object stores. Those supported include: 

- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO
- NetApp StorageGRID
- Ceph

### Metadata Storage

In additional a Key Value storage is used for storing metadata, with supported databases including PostgreSQL, DynamoDB, and CosmosDB Instructions of how to deploy such database on AWS can be found [here][dynamodb-permissions].

Additional information on the data format can be found in [Versioning internals](./how/versioning-internals.md) and [Internal database structure](./how/kv.md)

### Load Balancing

Accessing lakeFS is done using HTTP.
lakeFS exposes a frontend UI, an [OpenAPI server](#openapi-server), as well as an S3-compatible service (see [S3 Gateway](#s3-gateway) below).
lakeFS uses a single port that serves all three endpoints, so for most use cases a single load balancer pointing
to lakeFS server(s) would do.

<iframe width="420" height="315" src="https://www.youtube.com/embed/1vNQXFceFx4"></iframe>

## lakeFS Components

### S3 Gateway

The S3 Gateway is the layer in lakeFS responsible for the compatibility with S3. It implements a compatible subset of the S3 API to ensure most data systems can use lakeFS as a drop-in replacement for S3.

See the [S3 API Reference]({% link reference/s3.md %}) section for information on supported API operations.

### OpenAPI Server

The Swagger ([OpenAPI](https://swagger.io/docs/specification/basic-structure/){:target="_blank"}) server exposes the full set of lakeFS operations (see [Reference]({% link reference/api.md %})). This includes basic CRUD operations against repositories and objects, as well as versioning related operations such as branching, merging, committing, and reverting changes to data.

### Storage Adapter

The Storage Adapter is an abstraction layer for communicating with any underlying object store. 
Its implementations allow compatibility with many types of underlying storage such as S3, GCS, Azure Blob Storage, or non-production usages such as the local storage adapter.

See the [roadmap][roadmap] for information on the future plans for storage compatibility. 

### Graveler

The Graveler handles lakeFS versioning by translating lakeFS addresses to the actual stored objects.
To learn about the data model used to store lakeFS metadata, see the [versioning internals page]({% link understand/how/versioning-internals.md %}).

### Authentication & Authorization Service

The Auth service handles the creation, management, and validation of user credentials and [RBAC policies](https://en.wikipedia.org/wiki/Role-based_access_control){:target="_blank"}.

The credential scheme, along with the request signing logic, are compatible with AWS IAM (both [SIGv2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html) and [SIGv4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)).

Currently, the Auth service manages its own database of users and credentials and doesn't use IAM in any way. 

### Hooks Engine

The Hooks Engine enables CI/CD for data by triggering user defined [Actions][data-quality-gates] that will run during commit/merge. 

### UI

The UI layer is a simple browser-based client that uses the OpenAPI server. It allows management, exploration, and data access to repositories, branches, commits and objects in the system.

## Applications

As a rule of thumb, lakeFS supports any S3-compatible application. This means that many common data applications work with lakeFS out-of-the-box.
Check out our [integrations]({% link integrations/index.md %}) to learn more.

## lakeFS Clients

Some data applications benefit from deeper integrations with lakeFS to support different use cases or enhanced functionality provided by lakeFS clients.

### OpenAPI Generated SDKs

OpenAPI specification can be used to generate lakeFS clients for many programming languages.
For example, the [Python lakefs-client](https://pypi.org/project/lakefs-client/) or the [Java client](https://central.sonatype.com/artifact/io.lakefs/api-client) are published with every new lakeFS release.

### lakectl

[lakectl]({% link reference/cli.md %}) is a CLI tool that enables lakeFS operations using the lakeFS API from your preferred terminal.

### Spark Metadata Client

The lakeFS [Spark Metadata Client]({% link reference/spark-client.md %}) makes it easy to perform
operations related to lakeFS metadata, at scale. Examples include [garbage collection]({% link howto/garbage-collection/index.md %}) or [exporting data from lakeFS]({% link howto/export.md %}).

### lakeFS Hadoop FileSystem

Thanks to the [S3 Gateway](#s3-gateway), it's possible to interact with lakeFS using Hadoop's S3AFIleSystem, 
but due to limitations of the S3 API, doing so requires reading and writing data objects through the lakeFS server.
Using [lakeFSFileSystem][hadoopfs] increases Spark ETL jobs performance by executing the metadata operations on the lakeFS server,
and all data operations directly through the same underlying object store that lakeFS uses.


[data-quality-gates]:  {% link understand/use_cases/cicd_for_data.md %}#using-hooks-as-data-quality-gates
[dynamodb-permissions]:  {% link howto/deploy/aws.md %}#grant-dynamodb-permissions-to-lakefs
[roadmap]:  {% link project/index.md %}#roadmap
[hadoopfs]:  {% link integrations/spark.md %}#lakefs-hadoop-filesystem
