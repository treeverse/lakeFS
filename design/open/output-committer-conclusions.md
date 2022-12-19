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

### Failed to test the project can increase Spark write performance on lakeFS

As proposed, the main goal of the project was to increase write performance
on lakeFS.  However our plan did not include an early enough test for this,
and in fact we did not measure it early enough.

We did have some reasons for this:

* We estimated system performance based on counting API calls.  However the
  estimates of API latencies proved incorrect.
* Whole system performance is hard to measure before the system is finished
  and wanted to advance to that stage.
* We considered functional requirements (actually being able to implement a
  working OutputCommitter) at a higher risk than nonfunctional requirements
  (performance and compatibility).

However these reasons are clearly wrong in retrospect.  Requirements should
have set clear performance requirements.

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

## Background: stages and experiments performed

In all measurements we wrote of a small CSV onto 4000 partitions, measuring
OutputCommitter performance only.

### 1. Per-task branches, merged during taskCommit

Initial implementation had one branch for working on the job and one branch
per task.  Each successful task was merged into the job branch.  At the end
the job branch was merged to the output branch.

This ran slowly, with concurrent merges racing to update the job branch.  A
client API timeout increase was needed, after which we got >30m to write.

The proposal to [re-use metaranges in merge retries][meta-retry] could help
here.

(We also had to boost Pyramid cache size to avoid spills.  But a cache fill
takes very little time, and cannot account for 30m.)

At this stage we switched the lakeFS cloud staging cluster to use DynamoDB.
This slowed things down further, to >50m.  Increasing DynamoDB quota helped
but did not go below 40m.

[meta-retry]: https://github.com/treeverse/lakeFS/blob/1dcefc2eecdc9b8693439c1afc6beccc134a11a8/design/open/reuse-contented-merge-metaranges/reuse-contented-merge-metaranges.md 

### 2. Independent concurrent merges during jobCommit

We worked around concurrent merges by merging only in jobCommit.  It allows
independent concurrent merges of all the branches[^1] and eliminates racing
merges.  There was no significant speedup (~30-40m, but on DynamoDB).

DynamoDB was throttled continually at this point.  One issue: FindMergeBase
has to be called once per merge but it is slow to fetch commits.  The cache
is much too small and unconfigurable.  So cache spills happen.  And fetches
are batched, adding even more to each iteration.  See issues #2981, #4637.

One way to make this work well would be to support multiway merges like Git
does.  It would remove the need for a fast FindMergeBase, and simplify user
experience when looking at commit logs.

[^1]: Performed optimally: keep a fixed number of workers.  Each repeatedly
    merges two branches that are not participating in any merges.  Being so
    dynamic means that the result of a particular slow merge is used later,
    minimizing delay.

### 3. taskCommit without merging

We next avoided all task branch merges.  Instead, we re-linked objects from
the task branch into the job branch.  This worked nicely, 2.75m.  Still not
the desired 2m but seemingly withing reach.

But [support for this re-linking had just been removed][no-relink].

[no-relink]: https://github.com/treeverse/lakeFS/blob/master/design/accepted/gc_plus/uncommitted-gc.md#getlinkphysicaladdress

### 4. Tasks on job branch

In the final attempt, we wrote all objects to a temporary prefix on the job
branch and re-linked them to their final location during taskCommit.  It is
allowed by the new API.  However this still overloads lakeFS, regardless of
configured DynamoDB capacity.  lakeFS repeatedly accesses the branch entry,
and DynamoDB cannot partition it so it is subject to a hard rate-limit.  As
a result we went back to >30m.

### Complication: two job IDs

This applies to all work done.

One complication seems like a bug in Spark writers (FileFormatWriter).  The
writers generate two distinct job contexts: one to call startJob/commitJob,
the other to call startTask/commitTask on all tasks.  So it is not possible
to use the job ID to identify the job.

Instead we simply used the output path.  Doing this prevents implementation
of concurrent writers; obviously we never got that far.

