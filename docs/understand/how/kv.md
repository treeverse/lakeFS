---
layout: default
title: Internal database structure
parent: How it Works
grand_parent: Understanding lakeFS
description: Brief introduction to lakeFS over KV
nav_order: 20
has_children: false
redirect_from:
  - /understand/kv-in-a-nutshell.html
---
# Internal database structure
{: .no_toc }

{% include toc.html %}

Starting at version 0.80.2, lakeFS abandoned the tight coupling to [PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL) and moved all database operations to work over [Key-Value Store](https://en.wikipedia.org/wiki/Key%E2%80%93value_database)

While SQL databases, and Postgres among them, have their obvious advantages, we felt that the tight coupling to Postgres is limiting our users and so, lakeFS with Key Value Store is introduced.
Our KV Store implements a generic interface, with methods for _Get_, _Set_, _Compare-and-Set_, _Delete_ and _Scan_. Each entry is represented by a [_partition_, _key_, _value_] triplet. All these fields are generic byte-array, and the using module has maximal flexibility on the format to use for each field

Under the hood, our KV implementation relies on a backing DB, which persists the data. Theoretically, it could be any type of database and out of the box, we already implemented drivers for [DynamoDB](https://en.wikipedia.org/wiki/Amazon_DynamoDB), for AWS users, and [PostgreSQL](https://en.wikipedia.org/wiki/PostgreSQL), using its relational nature to store a KV Store. More databases will be supported in the future, and lakeFS users and contributors can develop their own driver to use their own favorite database. For experimenting purposes, an in-memory KV store can be used, though it obviously lack the persistency aspect

In order to store its metadata objects (that is _Repositories_, _Branches_, _Commits_, _Tags_, and _Uncommitted Objects_), lakeFS implements another layer over the generic KV Store, which supports serialization and deserialization of these objects as [protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers). As this layer relies on the generic interface of the KV Store layer, it is totally agnostic to whichever store implementation is in use, gaining our users the maximal flexibility

For further reading, please refer to our [KV Design](https://github.com/treeverse/lakeFS/blob/master/design/accepted/metadata_kv/index.md)

## Optimistic Locking with KV

One important key difference between SQL databases and Key Value Store is the ability to lock resources. While this is a common practice with relational databases, Key Value stores not always support this ability. When designing our KV Store, we tried to support the most simplistic straight-forward interface, with flexibility in backing DB selection, and so, we decided not to support locking. This decision brought some concurrency challenges we had to overcome. Let us take a look at a common lakeFS flow, _Commit_, during which several database operations are performed:
* All relevant (_Branch_ correlated) uncommitted objects are collected and marked as committed 
* A new _Commit_ object is created
* The relevant _Branch_ is updated to point to the new commit

The _Commit_ flow includes multiple database accesses and modifications, and is very sensitive to concurrent executions: If 2 _Commit_ flows run in parallel, we must guarantee correctness of the data. lakeFS with PostgreSQL simply locks the _Branch_ for the entire _Commit_ operation, preventing concurrent execution of such flows.
Now, with KV Store replacing the SQL database, this easy solution is no longer available. Instead, we implemented an [Optimistic Locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) algorithm, which leverages the KV Store _Compare-And-Set_ (_CAS_) functionality to remember the _Branch_ state at the beginning of the _Commit_ flow, and updating the branch at the end, only if it remains unchanged, using _CAS_, with the former _Branch_ state, used as a comparison criteria. If the sampled _Branch_ state and the current state differ, it could only mean that another, later, _Commit_ is in progress, causing the first _Commit_ to fail, and give the later _Commit_ a chance to complete.
Here's a running example:
  * _Commit_ A sets the _StagingToken_ to tokenA and samples the _Branch_,
  * _Commit_ B sets the _StagingToken_ to tokenB and samples the _Branch_,
  * _Commit_ A finishes, tries to update the _Branch_ and fails due to the recent modification by _Commit_ B - the _StagingToken_ is set to tokenB and not tokenA as expected by _Commit_ A,
  * _Commit_ B finishes and updates the branch, as tokenB is set as _StagingToken_ and it matches the flow expectation

An important detail to note, is that as a _Commit_ starts, and the _StagingToken_ is set a new value, the former value is added to a list of 'still valid' _StagingToken_s - _SealedToken_ - on the _Branch_, which makes sure no _StagingToken_ and no object are lost due to a failed _Commit_

You can read more on the Commit Flow in the [dedicated section in the KV Design](https://github.com/treeverse/lakeFS/blob/master/design/accepted/metadata_kv/index.md#graveler-metadata---branches-and-staged-writes)

## DB Transactions and Atomic Updates

Another notable difference is the existence of DB transactions with PostgreSQL, ability that our KV Store lacks. This ability was leveraged by lakeFS to construct several DB updates, into one "atomic" operation - each failure, in each step, rolled back the entire operation, keeping the DB consistent and clean.
With KV Store, this ability is gone, and we had to come up with various solutions. As a starting point, the DB consistency is, obviously, not anything we can risk. On the other hand, maintaining the DB clean, and as a result smaller, is something that can be sacrificed, at least as a first step. Let us take a look at a relatively simple flow of a new _Repository_ creation:
A brand new _Repository_ has 3 objects: The _Repository_ object itself, an initial _Branch_ object and an initial _Commit_, which the _Branch_ points to. With SQL DB, it was as simple as creating all 3 objects in the DB under one transaction (at this order). Any failure resulted in a rollback and no redundant leftovers in our DB.
With no transaction in KV Store, if for example the _Branch_ creation fails, it will leave the _Repository_ without an initial _Branch_ (or a _Branch_ at all), yet the _Repository_ will be accessible. Trying to delete the _Repository_ as a response to _Branch_ creation failure is ony a partial solution as this operation can fail as well.
To mitigate this we introduced a per-_Repository_-partition, which holds all repository related objects (the _Branch_ and _Commit_ in this scenario). The partition key can only be derived from the specific_Repository_ instance itself. In addition we first create the _Repository_ objects, the _Commit_ and the _Branch_, under the _Repository_'s partition key, and then the _Repository_ is created. The _Repository_ and its objects will be accessible only after a successful creation of all 3 entities. A failure in this flow might leave some dangling objects, but consistency is maintained.
The number of such dangling objects is not expected to be significant, and we plan to implement a cleaning algorithm to keep our KV Store neat and clean

## So, Which Approach is Better?

This documents provides a peek into our new database approach - Key Value Store instead of a Relational SQL. It discusses the challenges we faced, and the solutions we provided to overcome these challenges. Considering the fact that lakeFS over with relational database did work, you might ask yourself why did we bother to develop another solution. The simple answer, is that while PostgreSQL was not a bad option, it was the only option, and any drawback of PostgreSQL, reflected on our users:
* PostgreSQL can only scale vertically and that is a limitation. At some point this might not hold.
* PostgreSQL is not a managed solution, meaning that users had to take care of all maintenance tasks, including the above mentioned scale (when needed)
* As an unmanaged database, scaling means downtime - is that acceptable?
* It might even get to the point that your organization is not willing to work with PostgreSQL due to various business considerations

If none of the above apply, and you have no seemingly reason to switch from PostgreSQL, it can definitely still be used as an excellent option for the backing database for the lakeFS KV Store. If you do need another solution, you have DynamoDB support, out of the box. DynamoDB, as a fully managed solution, with horizontal scalability support and optimized partitions support, answers all the pain-points specified above. It is definitely an option to consider, if you need to overcome these
And, of course, you can always decide to implement your own KV Store driver to use your database of choice - we would love to add your contribution to lakeFS
