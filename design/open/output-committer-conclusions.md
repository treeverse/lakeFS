# Technical conclusions from the LakeFSOutputCommitter attempt

## Planning

### Poor performance planning

#### Merges

The original design was based on a single branch per task.  This allows for
very fast writing (all writes are independent) but merging 4000 times turns
out to be a slow operation.

Earlier communication with VE team would have allowed avoiding this.

**Conclusion:** Merge operations can be slow.  We saw multiple seconds 90th
percentile on *independent* branch merges.  FindMergeBase was very slow for
moderately far merge bases: seconds to search a few thousand commits.

**Possible mitigations:**

All except the first might need issues to open; **communicate with VE**.

* In future avoid using multiple merges for atomicity :-)
* Commit cache is too small and expires too quickly.  Increase its size and
  expiry.  (#4637)
* FindMergeBase performs batched operations in a loop.  Every BFS iteration
  could accumulate all its commit gets into a single batch and dispatch it.
  At the very least, don't batch these operations.  (#2981 is old but still
  relevant, maybe even more, it seems!)
* Consider allowing multi-way merges.
* Increase Pyramid cache sizes.

#### Limited support for relinking objects

We switched to _moving_ objects from per-task branches to a job branch.  It
avoids multiple merges, and is safe because per-task objects have different
names.  In practice the API does not allow moving object so this was really
relinking unstaged objects across branches.

Support for this was going away as part of incremental GC concurrently with
implementation.  Luckily we immediately caught it.

**Conclusion:** We cannot reduce branch concurrency using multiple branches
like this. :shrug:

#### Single-branch write performance on lakeFSFS

Switching to writing everything to a _single_ branch shows poor performance
of lakeFS as exercised by lakeFSFS.  CloudWatch Insights shows that one key
takes a huge amount of load -- the branch key.  DynamoDB cannot partition a
single key, so it is necessarily throttled regardless of purchased quota.

**Conclusion:** lakeFSFS performance is poor on lakeFS with DynamoDB as its
KV store.  Work on improving lakeFS performance with DynamoDB (some of this
work is proposed in #4769, #4734, #4712), and also on improving lakeFSFS on
lakeFS (some of this work is proposed in #4722, #4721, #4676).

### Poor communication with other teams

We assumed multiple non-functional and one functional requirements that did
not actually exist.  We should have specified these requirements as part of
the design and communicated them with VE team.

## Shared vision

### No expected performance of lakeFSFS

We spec'ed lakeFSFS to be a few percent slower than S3A on S3 with "magic",
but we have no typical load to use to measure.

As a result we cannot design efficient flows that use lakeFSFS, or that the
performance we get in some setup is adequate.

We lack an understanding of what performance to expect from lakeFSFS.  This
should include expected performance and affecting variables for these:

* List "directory", recursively and nonrecursively.
* Create file.
* Open file.
* Check file existence.
* Delete file or "directory", recursively and nonrecursively.
* Write a 1000-partition Parquet file (for instance).
* Read from all objects of a 1000-partition Parquet file (for instance).  A
  good refinement is to read _all_ fields and also to read _just one_ field
  from the file.

### No expected performance of lakeFS

We spec'ed lakeFS to be a few percent slower than S3 but we have no typical
load that we intend to use to measure.

As a result we cannot design efficient flows that use lakeFS, or state that
the performance we get in some setup is adequate.

We lack an understanding of what performance to expect from lakeFS.  It can
include expected performance and affecting variables for these:

* Commits.  (Possible affecting variables: # of modified objects, ranges)
* Merges.  (Possible affecting variables: # of commits to scan, # of ranges
  modified, # of contending and uncontending concurrent merges)
* Reads and lists.  (Possible affecting variables: # of concurrent reads on
  same branch, # of concurrent reads, expected size of returned listing)
* Writes.  (Possible affecting variables: # of concurrent mutations on same
  branch, # of concurrent mutations)

### No best practices

In order to support single-writer and multi-writer configurations we should
decide on best practices for how to perform several basic tasks.  These may
well depend on the result of the above performance work.  Currently nothing
is blocked on this, giving us some time to do this correctly.  However some
things don't currently work and one reasonable outcome is to remove support
for them.

* How to cause multiple files to appear at atomically
  * Commits?
  * Merges?
  * Some new mechanism, perhaps multiple staging areas?
* How to abort work on failure after emitting some objects.
* Recommended synchronization mechanisms in lakeFS
  * Merges?
  * Return status of delete object?  (Currently broken, #4798)
  * Return status of Set IfAbsent object?  (Currently broken, #4796)
  * Some new mechanism, perhaps a KV API?
