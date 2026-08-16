# Proposal: Branch Read Optimization

## Overview

This document proposes an optimization to Graveler's read path by introducing a **branch-level `dirty` flag**, indicating whether a branch currently has uncommitted (staged) changes.

Today, Graveler always checks the staging area when reading from a branch because every branch always has a staging token. In practice, most branches are *clean* most of the time, and staging reads frequently miss. These unnecessary staging lookups introduce additional metadata reads and cause **hot partition pressure in DynamoDB**, where each staging token maps to a partition.

By explicitly tracking whether a branch has uncommitted changes, Graveler can safely **skip staging reads entirely for clean branches**, significantly reducing metadata traffic and improving read scalability.

---

## Problem Statement

### Current behavior

In Graveler, a branch is represented by:
- a commit ID (HEAD),
- a staging token,
- a list of sealed staging tokens.

For every read (`Get`, `List`), Graveler:
1. Attempts to read from the current staging token.
2. Falls back to committed data if not found.

This happens **even when the branch has no staged changes**.

### Why this is a problem

#### DynamoDB Partition Limitations

DynamoDB distributes data across partitions based on partition keys. Each partition has hard limits:

- **3,000 RCU** (Read Capacity Units) per partition for strongly consistent reads
- **1,000 WCU** (Write Capacity Units) per partition
- **10 GB** storage per partition

When a single partition key receives more traffic than these limits, requests are **throttled** regardless of the table's total provisioned capacity. This is known as a **hot partition**.

#### How staging tokens create hot partitions

- **Each staging token maps to a single KV partition** (via `StagingTokenPartition(token)`).
- Every read operation performs a **strongly consistent lookup** against the staging partition.
- For read-heavy workloads on popular branches, the staging partition receives disproportionate traffic.
- Even when staging is empty (the common case), the lookup still consumes RCUs against that partition.

#### Impact

For a branch receiving 5,000 GET requests per second:
- Each GET performs one strongly consistent staging read (1 RCU for items ≤4KB)
- Total: **5,000 RCU/s** against a single partition
- This exceeds the 3,000 RCU partition limit by 67%
- Result: **throttling, increased latency, and failed requests**

The code already acknowledges that the common case is that data exists in committed storage, yet the staging lookup is always paid first.

### Constraints

- Staging tokens are fundamental to write paths and commit semantics.
- Removing staging tokens or creating them lazily introduces wide-reaching invariant changes.
- The solution must preserve correctness, especially around commits, resets, and diffs.

---

## Goals

- Reduce unnecessary metadata reads on clean branches.
- Alleviate DynamoDB partition hot spots caused by staging tokens.
- Preserve existing staging and commit semantics.
- Minimize invasive changes to Graveler invariants.
- Enable safe, measurable rollout with clear observability.

---

## Non-Goals

- Redesign the staging token lifecycle.
- Parallelize staging and committed reads.
- Change commit, diff, or merge semantics.
- Optimize write-heavy or permanently dirty branches.

---

## Proposed Solution

### High-level idea

Introduce a **branch-level boolean flag**:

```go
dirty bool
```

This flag indicates whether the branch currently has any uncommitted (staged) changes.

### Semantics

- `dirty = false`
  - Branch has no uncommitted changes.
  - No entries exist in staging token or sealed tokens.
  - Reads can skip staging entirely.

- `dirty = true`
  - Branch may have staged data.
  - Reads must check staging first (current behavior).

### Formal definition

```
dirty = true  ⟺  (StagingToken has entries) OR (SealedTokens is non-empty)
dirty = false ⟺  (StagingToken is empty) AND (SealedTokens is empty)
```

### Key insight

The existence of a staging token does **not** imply the existence of staged data.

By explicitly tracking whether staging *contains meaningful data*, we can make the read path conditional without altering the staging mechanism itself.

---

## Detailed Design

### Branch model changes

Extend the branch metadata model with a new persisted field:

```go
dirty bool
```

This field is stored alongside existing branch metadata (commit ID, staging token, sealed tokens).

### Migration strategy

For existing branches without the `dirty` field:

- **Default value: `true`** (conservative/safe)
- This ensures no data loss from incorrectly skipping staging reads
- Branches become optimized after their next commit or explicit reset

New branches created after deployment will have `dirty` properly maintained from creation.

---

### Read path changes

#### `Get`

Current logic:

```
if stagingToken exists:
    try staging
read committed
```

Proposed logic:

```
if dirty:
    try staging
read committed
```

For clean branches (`dirty == false`), the staging lookup is skipped entirely.

#### `List`

Same change applies:
- Skip building and merging staging iterators when the branch is clean.

---

### Write path changes

**Critical: The dirty flag must be set BEFORE writing to staging.**

If we wrote to staging first, a concurrent reader could see `dirty=false`, skip staging, and miss the write.

Acceptable failure mode: If the dirty update succeeds but the staging write fails, the branch is marked dirty with no staged data. Readers check staging unnecessarily (performance penalty), but correctness is preserved.

```
on staging write:
    if not dirty:
        set dirty = true (must complete before proceeding)
    write to staging
```

Operations that set `dirty = true`:
- object put (first write only)
- object delete (first write only)
- any staging mutation (first write only)

---

### Commit and reset paths

Operations that guarantee the absence of uncommitted changes set `dirty = false`.

Examples:
- successful commit (after sealed tokens are cleared)
- hard reset that clears staging

Notes:
- Token rotation and sealed-token handling remain unchanged.
- The `dirty` flag reflects *user-visible uncommitted state*, not internal token mechanics.
- `dirty` is set to `false` only when **both** staging token and sealed tokens are empty.

---

### Diff and status operations

- `DiffUncommitted`:
  - If `dirty == false`, return an empty diff immediately.

- `isUncommittedEmpty`:
  - Can short-circuit and return `true` when `dirty == false`.

This avoids unnecessary staging scans for clean branches.

---

## Consistency Model

### Guaranteed behavior

When a read operation fetches the branch record and observes `dirty=false`, it is guaranteed that:
- The staging token contains no entries
- The sealed tokens list is empty
- All branch data is in committed storage

### Acceptable race condition

The following scenario is **expected and acceptable**:

1. Request A reads branch, sees `dirty=false`
2. Request B writes to staging, sets `dirty=true`
3. Request A skips staging (based on its cached view), reads only committed
4. Request A returns data without Request B's write

This is acceptable because:
- Request A observed a consistent snapshot at read time
- lakeFS does not guarantee read-your-own-writes across separate API calls
- The behavior is equivalent to Request A completing before Request B started

This matches the existing consistency model where concurrent operations see consistent snapshots at their respective read times.

---

## DynamoDB Impact and Expected RCU Reduction

### Background

- Staging metadata is stored in DynamoDB.
- Each staging token corresponds to a DynamoDB partition.
- Reads use **strong consistency** (1 RCU per 4KB item).

### DynamoDB partition limits (per partition)

| Resource       | Limit       |
|----------------|-------------|
| Read Capacity  | 3,000 RCU/s |
| Write Capacity | 1,000 WCU/s |
| Storage        | 10 GB       |

Exceeding these limits causes **throttling** even if table-level capacity is available.

---

### What the optimization removes

For each read on a clean branch, Graveler skips:
- one strongly consistent staging lookup (`Get`), or
- an entire staging iterator and merge (`List`).

---

### Expected RCU savings (GET)

Let:
- `Q_get` = GET requests per second
- `P(clean)` = fraction of reads hitting clean branches

Then:

```
RCU_saved ≈ Q_get × P(clean)
```

Examples:
- 10k GET/s, 90% clean → ~9,000 RCUs saved per second
- 25k GET/s, 95% clean → ~23,750 RCUs saved per second

### Write overhead

Additional branch record updates occur only on the **first write** after a branch becomes clean:

```
Additional_writes ≈ Number of commit cycles
```

For a branch with N writes per commit:
- Overhead = 1/N of staging writes require branch update
- Example: 100 writes per commit → 1% overhead
- Example: 1000 writes per commit → 0.1% overhead

This overhead is negligible compared to read savings for read-heavy workloads.

---

### LIST operations

For `List`, savings can exceed 1 RCU per request because the entire staging iterator is skipped. The exact savings depend on paging and iterator behavior and should be measured empirically.

---

## Metrics and Observability

### Branch cleanliness metric

Emit a counter on every read dereference:

```
graveler_branch_reads_total{repo, branch, dirty="true|false", op="get|list|..."}
```

This allows direct measurement of:

```
P(clean) = reads where dirty=false / total reads
```

---

### Supporting metric (optional)

Track whether staging was consulted at all:

```
graveler_staging_read_attempts_total{repo, branch, op="get|list"}
```

This validates the actual reduction in staging reads once the optimization is enabled.

---

## Experimental Rollout Plan

### Feature flag

Introduce an experimental configuration flag:

```
experimental.graveler_dirty_read_optimization
```

- Default: **false**
- When enabled:
  - Reads on `dirty=false` branches skip staging entirely.

This enables safe rollout and immediate rollback.

---

### Phased implementation

1. **Add `dirty` field** to branch metadata (no behavior change).
   - Existing branches default to `dirty=true` (safe).
2. **Maintain correctness**:
   - Set `dirty=true` on first staging mutation (conditional update).
   - Set `dirty=false` after commit/reset flows that guarantee cleanliness.
3. **Emit metrics** for branch cleanliness and staging read attempts.
4. **Enable fast-path reads** behind the experimental flag.
5. **Validate** in staging/canary:
   - DynamoDB RCU consumption on staging partitions
   - Read latency (p50/p95)
   - Correctness tests:
     - Write → immediate read → should see write
     - Write → commit → read → should see committed data
     - Write → reset → read → should NOT see write
     - Concurrent writes and reads
6. **Gradual rollout** to production.

---

## Alternatives Considered

### Lazy creation of staging tokens

Rejected.

While feasible, this approach:
- breaks existing invariants that staging tokens always exist,
- complicates commit, diff, and delete paths,
- makes the first write heavier and more contention-prone.

The `dirty` flag achieves the same read optimization with significantly lower risk.

### Parallel staging and committed reads

Rejected.

This may reduce latency but:
- does not reduce DynamoDB load,
- can increase total read capacity usage,
- does not address partition hot spots.

---

## Summary

Introducing a **branch-level `dirty` flag** allows Graveler to skip unnecessary staging reads for clean branches without changing core staging or commit semantics.

This directly addresses DynamoDB partition limitations (3,000 RCU/partition) caused by staging token hot spots, providing a clear, low-risk path to improving read scalability in lakeFS with strong observability and controlled rollout.

The write overhead is minimal (one branch update per commit cycle) compared to the substantial read savings (up to 95% reduction in staging reads for clean branches).
