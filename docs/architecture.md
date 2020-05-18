---
layout: default
title: Architecture
nav_order: 7
has_children: false
---
# Architecture

LakeFS is distributed as a single binary encapsulating several logical services:

The server itself is stateless, meaning you can easily add more instances to handle bigger load
LakeFS stores data in an underlying [S3 bucket](https://aws.amazon.com/s3/) and metadata in [PostgreSQL](https://www.postgresql.org/).

![Architecture](assets/img/arch.png)

## LakeFS Components

### S3 Gateway

The API Gateway implements LakeFS' compatibility with S3. It implements a compatible subset of the S3 API to ensure most data systems can use LakeFS as a drop-in replacement for S3.

To achieve this, the gateway exposes an HTTP listener on a dedicated host and port.

See the [S3 API Reference](/api_reference) section for information on supported API operations.

### OpenAPI Server

The Swagger (OpenAPI) Server exposes the full set of LakeFS operations. This includes basic CRUD operations against repositories and objects, as well as versioning related operations such as branching, merging, committing and reverting changes to data.

### S3 Storage Adapter

The S3 Storage Adapter is the component in charge of communication with the underlying S3 bucket. It is logically decoupled from the S3 Gateway to allow for future compatibility with other types of undelying storage such as HDFS or S3-Compatible storage providers.

See the [roadmap]() for information on future plans for storage compatibility. 

### Metadata Index

The Metadata index contains the versioning logic used inside LakeFS. It maps logical entities such as branches, commits and object paths to the relevant underlying storage.

The metadata itself is managed in PostgreSQL. This makes it relatively easy to maintain, is offered as a managed service by many providers and has a rock solid foundation.

Additionally, using the consistency and durability guarantees provided by Postgres, LakeFS ensures its metadata is [strongly consistent]() and doesn't suffer from [S3's eventual consistency woes](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel).

### Authentication & Authorization Service

The Auth service handles creation, management and validation of user credentials and [RBAC policies](https://en.wikipedia.org/wiki/Role-based_access_control).

The credential scheme, along with the request signing logic are compatible with AWS IAM (both [Sigv2]() and [Sigv4]()).

Currently, the auth service manages its own database of users and credentials and does not use IAM in any way. 


Note

### Frontend UI

