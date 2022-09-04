---
layout: default
title: Database Migration
description: A guide to migrating lakeFS database.
parent: Reference
nav_order: 51
has_children: false
---

# lakeFS Database Migrate
{: .no_toc }

**Note:** Feature in development
{: .note }

The lakeFS database migration tool simplifies switching from one database implementation to another.
More information can be found [here](https://github.com/treeverse/lakeFS/issues/3899)

## lakeFS with Key Value Store

Starting at version 0.80.0, lakeFS abandoned the tight coupling to [PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) and moved all database operations to work over [Key-Value Store](https://en.wikipedia.org/wiki/Key%E2%80%93value_database)

While SQL databases, and Postgres among them, have their obvious advantages, we felt that the tight coupling to Postgres is limiting our users and so, lakeFS with Key Value Store is introduced.
Our KV Store implements a generic interface, with methods for `Get`, `Set`, `Compare-and-Set`, `Delete` and `Scan`. Each entry is represented by a [`partition`, `key`, `value`] triplet. All these fields are generic byte-array, and the using module has maximal flexibility on the format to use for each field

Under the hood, our KV implementation relies on a backing DB, which persists the data. Theoretically, it could be any type of database and out of the box, we already implemented drivers for [DynamoDB](https://en.wikipedia.org/wiki/Amazon_DynamoDB), for AWS users, and [PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL), using its relational nature to store a KV Store. More databases will be supported in the future, and lakeFS users and contributors can develop their own driver to use their own favorite database. For experimenting purposes, an in-memory KV store can be used, though it obviously lack the persistency aspect

In order to store ref store objects (that is `Repositories`, `Branches`, `Commits`, `Tags`, and `Uncommitted Objects`), lakeFS implements another layer over the generic KV Store, which supports serialization and deserialization of these objects as [protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers). As this layer relies on the generic interface of the KV Store layer, it is totally agnostic to whichever store implementation is in use, gaining our users the maximal flexibility

For further reading, please refer to our [KV Design](https://github.com/treeverse/lakeFS/blob/master/design/accepted/metadata_kv/index.md)