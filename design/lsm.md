# Proposal: Modeling lakeFS as an object stored LSM

## Overview

The basic idea is to treat commits (as in Graveler trees), as a bottom layer in an [LSM Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree).<br>
New writes are logically placed "on top" of that tree.

Object stores are a little tricky in that regard, because latency is not great, but throughput and parallelism is,
so we want to take advantage of that.

For simplicity, we're going to model a very basic LSM tree: a commit (or rather, the tree represented by its metarange)
is our `L3` (chosen arbitrarly for illustrating the concept). A few, slowly changing sstable objects will act as `L2`, and a bigger (but bounded) amount of much smaller sstable objects will act together as `L1`. 
Since `L1` files need to be totally ordered, we'll rely on a ref stored in a `RefStore` for ordering using atomic list appends/removals, and coordination using conditional updates.

![LSM on object store](https://gist.github.com/ozkatz/950e3a440fe278b9d74614dc2a226be7/raw/71d4ed04d710d3ef6b12500daf6fa14c1bc55996/lsm.png)

## RefStore design and schema

A `RefStore` holds a small but important dataset: for each branch - what current commit does it point to, along with information about its uncommitted layers (`L1` and `L2`).

### RefStore requirements

A database can act as a ref store, provided it can do the following:

- ordered listing of keys (to enumerate/search for branches)
- atomically add/remove values to an array/list (for ordering `L1` and `L2` layers)
- Provide one of `{CAS, conditionalWrite, serializable transaction}` on a single key (for coordination of commits)
- Provide reasonable latency (preferablly, < 10ms p90)

This set of requirements can be answered using DynamoDB, Cassandra, MongoDB (רחמנא לצלן), MySQL, PostgreSQL and many other databases.

### RefStore schema

The `RefStore` holds a single logical table, `refs`, with the following structure:

- `type` - `text`. one of `{"tag, "branch"}`
- `name` - `text`. name of the ref. This is the primary key
- `base_commit` - `text`. The commit ID this ref is pointing to
- `L1` - `[]text`**\***. sstable IDs representing `L1`
- `L2` - `[]text`**\***. sstable IDs representing `L2`

**\*** - For simplicity. Could also be a random set of bytes, a random string, doesn't really matter.

An example on a key value store/document DB would be:

| Key              | Value                                                                                                        |
|------------------|--------------------------------------------------------------------------------------------------------------|
| `"master"`       | `{"base_commit": "abcdef123456", "type": "branch", "L2": [dedbeef12], "L1": [5b31659b, 303b9493, b6b98ea8]}` |
| `"dev-2"`        | `{"base_commit": "abcdef123456", "type": "branch", "L2": [a2b32739], "L1": [84d1164f, 194f3084]}`            |

## Reader flow

- Get ref from `RefStore` (could be a batched read as we currently do for PostgreSQL)
- Using a local FS cache, compute missing `L1` files: all IDs not locally cached. we can then fetch them in parallel from the object store using the naming convention `<ref>/L1/<id>.sst`.
- Get latest `L2` file by ID (if not cached locally already, using same method as L1)
- Get `base_commit` metarange (if not cached locally already)
- Use the assembled tree: `ordered L1 files -> L2 files -> base commit` to respond to request
- *Optimization: keep minmax stats and/or bloomfilters somewhere to see what files are really needed to satisfy the read request.*

## Writer Flow

- For every incoming write, hold onto it for **N milliseconds** before acking. [N trades-off write latency with `L1` file size](https://en.wikipedia.org/wiki/Little%27s_law).
- In the meantime, add it to an sorted in-memory structure. This is our Memstore 
- Flush Memstore into an sstable (range file) called `<ref>/L1/<id>.sst`
- Append sstable ID to `L1` list from `RefStore`
- Once acknowledged by the `RefStore`, return success to all pending writes

*smart clients can do even better: instead of going through a memstore, we can append any prepared range file. For example, a Spark client can take a logical `spark.write.parquet(...)` operation and stage all its writes as an atomic unit*

**compaction:** Once every X writes (or M milliseconds, or L1.length, or another hueristic):

- read `L1` list from `RefStore`
- merge all `L1` files with underlying `L2` layer
- if persisted back to object store, update `$ref.L2 = [newIDs], remove($ref.L1, [...L1 ids we've read])`, **if L2 isn't a different value already**
- could do this outside request/response cycles
- appends to L1 can still happen concurrently, no need to block them

## Committer Flow

- Get ref with its list of L1 and L2 from `RefStore`
- Read missing L1 and L2 files not cached locally (same as reader's flow)
- These act together as our `DiffIterator`
- Using the `base_commit` we got for the ref, generate new ranges, metarange and commit objects in S3
- update `$ref.base_commit = newCommitID, remove($ref.L2, [...L2 ids we've read]), ($ref.L1, [...L1 ids we've read])` **CAS: if current base commit is the same as the one we saw when we started**

*Once updated, find a way to remove from object store all previous L2 and L1 ranges (we should maybe prefix L1/L2 with a token to make that easier). Could also be deffered to some GC task in background, or a periodic retention Spark job*

## Concerns, open questions, random thoughts

1. If we add minmax statistics, `L1` and `L2` are basically baby metaranges! (well `L1` less so, because it overlaps, but there's some reuse to be done here if designed carefully) 
1. For low throughput, we might end up having to bump up N, resulting in delayed writes, or accept the fact that we have *very* small ranges due to low throughput. Since the amount of L1 ranges is bounded (by compaction), we won't have too many of them, but need to come up with a strategy to manage those.
1. Since `L1` and `L2` file versions are immutable, we ~~can also~~ should keep a small bloomfilter for each, to reduce the amount of L1 reads we do
1. This design already employes write-behind, but we can also do read-ahead for frequently accessed branches
1. This design seems to benefit from fewer, larger lakeFS instances vs many smaller ones (due to batching of writes)
1. Not having to rely on any listing operation or anything other than read-after-write on the object store while still relying on it for almost all metadata is kind of nice

# What can we take from Workspaces that we did (?) like:

- IAM: this is completely decoupled from entries and their meaning: we can still write them with a meaningful path that could utilize IAM permissions
- Same for retention using S3 lifecycle
- Can still do `lakefs run -dev --dont-ever-use-in-production!!1` where a single instance does coordination with no external DB
- `lakefs ingest` can still be a thing to onboard (continuosly or not), data from the object store. Not sure we agree on this one being a benefit :)
- Can still create a logical "workspace" if that's desired: an SST that will get pushed as a single unit to L1
- These "workspaces" can also be stashed and popped, or at least the data models makes that cheap (so we get the flexibility, from a product perspective it's now a possibility)

