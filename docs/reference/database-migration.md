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

### Optimistic Locking with KV

One important key difference between SQL databases and Key Value Store is the ability to lock resources. While this is a common practice with relational databases, Key Value stores lack this ability. Let us take a look at a common lakeFS flow, `Commit`, during this flow several ref store operations are performed:
* All relevant (`Branch` correlated) uncommitted objects are collected and marked as committed 
* A new `Commit` object is created
* The relevant `Branch` is updated to point to the new commit
The `Commit` flow includes multiple database accesses and modifications, and is very sensitive to concurrent executions: If 2 `Commit` flows run in parallel, we must guarantee correctness of the data. `lakeFS` with PostgreSQL simply locks the `Branch` for the entire `Commit` operation, preventing concurrent execution of such flows.
Now, with KV Store replacing the SQL database, this easy solution is no longer available. Instead, we implemented an [Optimistic Locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) algorithm, which leverages the KV Store `Compare-and-Set` (`CaS`) functionality to remember the `Branch` state at the beginning of the `Commit` flow, and updating the branch at the end, only if it remains unchanged, using `CaS`. Before sampling the `Branch` another update, of the `Branch`'s `StagingToken`, is made, and so, if another `Commit` is already running in parallel, it will fail the final step, of updating the `Branch`, as it was already changed. If 2 commits do run in parallel there are only 2 possible outcomes:
1. Either 1 of the `Commits` manages to start, sample the `Branch` state, modify the `StagingToken`, create a new `Commit` and use `Cas` to find out that the branch was not modified and update it, and so it succeeds and causing the other `Commit` to fail, or
2. Both `Commits` interfere with each other, and both will fail, as  depicted in the following scenario:
  * `Commit` A sets the `StagingToken` to val1 and samples the `Branch`,
  * `Commit` B sets the `StagingToken` to val2 and samples the `Branch`,
  * `Commit` A finishes, tries to update the `Branch` and fails due to the recent modification by `Commit` B,
  * `Commit` A is retired, setting `StagingToken` to val3 and samples the `Branch`
  * `Commit` B finishes, tries to update the `Branch` and fails due to the recent modification by `Commit` A,
  * And so on...
Eventually , as retries will be exhausted, at least one commit will succeed, if not earlier, but this was definitely something that had to be taken into consideration when designing our KV Store. You can read more on the Commit Flow in the [dedicated section in the KV Design](https://github.com/treeverse/lakeFS/blob/master/design/accepted/metadata_kv/index.md#graveler-metadata---branches-and-staged-writes)

### DB Transactions and Atomic Updates

Another notable difference is the existence of DB transactions with PostgreSQL, ability that our KV Store lacks. This ability was leveraged by `lakeFS` to construct several DB updates, into one "atomic" operation - each failure, in each step, rolled back the entire operation, keeping the DB consistent and clean.
Once again, with KV Store, this ability is gone, and we had to come up with various solutions. As a starting point, the DB consistency is, obviously, not anything we can risk. On the other hand, maintaining the DB clean, and as a result smaller, is something that can be sacrificed, at least as a first step. Let us take a look at a relatively simple flow of a new `Repository` creation:
A brand new `Repository` has 3 objects: The `Repository` object itself, an initial `Branch` object and an initial `Commit`, which the `Branch` points to. With SQL DB, it was as simple as creating all 3 objects in the DB under one transaction (at this order). Any failure resulted in a rollback and no redundant leftovers in our DB.
Now, with KV Store, the transaction ability is gone, and so each operation for itself, and each can fail. Now, if, for example, the `Repository` creation fails, it is not much of a problem, as this is the first entity to be created, but what happens if the `Branch` creation fails? It will leave the `Repository` without an initial `Branch` (or a `Branch` at all), yet, the `Repository` will be accessible. Trying to delete the `Repository` as a response to `Branch` creation failure is ony a partial solution as the `Repository` deletion can fail too.
In order to overcome this obstacle we introduced a per-`Repository`-partition, which holds all repository related objects (the `Branch` and `Commit` in this scenario). The partition ket can only be derived from the `Repository` object it self. In addition we reversed the order of objects creations, where the `Commit` and the `Branch` are created, under the `Repository`'s partition key, and the `Repository` is created last. The `Repository`, and as a result its partition, will be accessible only after a successful creation of all 3 entities, and every failure might leave some dangling objects, but the consistency remains in place, as these objects are not reachable.
The amount of such dangling objects is not expected to be significant, and we plan to implement a cleaning algorithm to keep our KV Store neat and clean