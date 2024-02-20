# Proposal: Staging Compaction

## Problem Description

lakeFS staging area is built on top of the [KV store](../accepted/metadata_kv/index.md).
In short, the structure for the `Branch` entity in the kv-store is as follows:
```go
type Branch struct {
	CommitID     CommitID
	StagingToken StagingToken
	// SealedTokens - Staging tokens are appended to the front, this allows building the diff iterator easily
	SealedTokens []StagingToken
}
```

Uncommitted entries are read from the staging token first, then from the 
sealed tokens by order. Writes are performed on the staging token.

The KV design has proven to meet lakeFS requirements for its consistency 
guarantees. For most use-cases of uncommitted areas in lakeFS, it does that 
efficiently. However, there are some cases where it falls short, for example 
this [issue](https://github.com/treeverse/lakeFS/issues/2092).  
There might be other cases where the structure of the `Branch` entity impacts 
the performance of reading from the staging area, for example when the 
number of sealed tokens is large (N)[^1] and reading a missing entry requires 
reading from all N sealed tokens. 

## Goals

- Preserve lakeFS consistency guarantees for all scenarios.
- A more efficient way to read from branches with a large number of 
  tombstones (i.e. Fix the issue mentioned above).
- Preserve lakeFS performance for other scenarios.[^2]


## Non Goals

- Improve the performance of the KV store in general.
- Simplify the uncommitted area model in lakeFS. While we'll try to keep the 
  changes minimal, we might need to add some complexity to the model to 
  achieve the goals.


## Proposed Design

We propose to add a compaction mechanism to the staging area. We'll explain 
how to decide when to compact (Sensor), how to compact (Compactor), 
data read/write flows and commit flows after compaction.

```go
type Branch struct {
	CommitID     CommitID
	StagingToken StagingToken
	// SealedTokens - Staging tokens are appended to the front, this allows building the diff iterator easily
	SealedTokens []StagingToken
	
	CompactedMetaRange MetaRangeID
	CompactionStartTime time.Time
}
```

### Sensor

The Sensor is responsible for deciding when to compact the staging area.
The Sensor will be linked to the Graveler and will collect 
information on writes to the staging area. It will decide when to compact a 
certain branch based on the number of deleted entries to its staging area, 
taking into accounts operations like commits/resets etc. It can also decide 
based on the number of sealed tokens, although not necessarily a priority 
for the first version. We can probably avoid the need to query the kv-store 
to retrieve that information (except for the service startups) by caching 
the information in the Sensor.

Notice there's a single Sensor for each lakeFS. While the followup sections 
describe why concurrent compactions don't harm consistency, they may be very 
inefficient. Therefore, a Sensor deciding to compact will only do so if the 
branch's `CompactionStartTime` is not within the last x<TBD> minutes using 
`SetIf` to minimize collisions (although not avoiding them completely due to 
clock skews). 

### Compactor

Upon deciding to compact, the Sensor will trigger the Compactor. The 
Compactor will perform an operation very similar to Commit. Before starting 
to compact, it will atomically:

1. Push a new `StagingToken` to the branch.
1. Add the old `StagingToken` to the `SealedTokens` list.
1. Set the `CompactionStartTime` to the current time.

The Compactor will now read the branch's `SealedTokens` in order and apply 
them on either the `CompactedMetaRange` if it exists, or on the branch's 
HEAD CommitID MetaRange to create a newMetaRange. The Compactor will then 
atomically update the branch entity:

1. Set the `CompactedMetaRangeID` to the new MetaRangeID.
1. Remove the compacted `SealedTokens` from the branch.

The Compactor should fail the operation if the sealed tokens have changed 
since the compaction started (SetIf). Although the algorithm can tolerate 
additional sealed tokens being added during the compaction, it's better to 
avoid competing with concurrent commits. Commits have the same benefits as 
compactions, but they are proactively triggered by the user (and might fail 
if compaction succeeds).

Consistency guarantees for the compaction process are derived from the 
Commit operation consistency guarantees. In short, the compaction process 
follows the same principles as the Commit operation. Writes to a branch 
staging token succeeds only if it's the staging token after the write 
occurred. Therefore, replacing the sealed token with an equivelant MetaRange 
guarantees no successful writes are gone missing.

### Branch Write Flow

No change here. Writes are performed on the staging token.

### Branch Read Flow

Reading from a branch includes both a specific entry lookup and a listing 
request on entries of a branch. The read flow is more complicated than what 
we have today, and is different for compacted and uncompacted branches.

#### Combined (Committed+ Uncommitted)

There are 3 layers of reading from a branch:
1. Read from the staging token.
2. Read from the sealed tokens (in order).
3. Depends on compaction:
   1. If a CompactedMetaRangeID exists, read from the compacted MetaRange.
   1. If a CompactedMetaRangeID doesn't exist, read from the CommitID.

#### Committed

Just like today, reads are performed on the Branch's CommitID.

#### Uncommitted

For operations such as `DiffUncommitted` or checking if the staging 
area is empty, the read flow will be as follows:

1. Read from the staging token.
2. Read from the sealed tokens (in order).
3. If a CompactedMetaRangeID exists, read the 2-way diff between the compacted
   metarange and the CommitID's metarange.

* There's an inefficiency here, as there's an option we'll need to read 2 whole 
  metaranges to get the diff, like when there's a single change in every 
  range. The nature of changes to a lakeFS branch is such that changes are 
  expected in a small number of ranges, and the diff operation is expected 
  to skip most ranges. Moreover, Pebble caching the ranges should remove the 
  most costly operation of ranges comparison - fetching them from S3. If 
  this is still inefficient, we can use the immutability trait[^3] of the 
  diff: we can calculate the diff result once and cache it.

### Commit Flow

The commit flow is slightly affected by the compaction process. If 
compaction never happened, the commit flow is the same as today. If a 
compaction happened, apply the changes to the compacted metarange instead of 
the HEAD commit metarange. A successful commit will reset the the 
`CompactedMetaRangeID` field.

## Metrics

We can collect the following prometheus metrics:
- Compaction time.
- Number of sealed tokens.

[^1]: The number of sealed tokens increases by one for every HEAD changing 
operation (commit, merge, etc.) on the branch. The number of sealed tokens 
resets to 0 when one of the operations succeeds. Therefore, a large number of 
sealed tokens is unlikely and is a sign of a bigger problem like the 
inability to commit.   
[^2]: Any additional call to the kv-store might have an indirect performance 
impact, like in the case of DynamoDB throttling. We'll treat a performance 
impact as a change of flow with a direct impact on a user operation. 
[^3]: A diff between two immutable metaranges is also immutable.
