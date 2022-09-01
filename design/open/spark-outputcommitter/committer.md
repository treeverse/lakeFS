# Proposal

Add a Hadoop OutputCommitter that uses _existing_ lakeFS operations for
**atomic** commits that are efficient and safely concurrent.

## Terminology

lakeFS and Hadoop both use the term "commit", but with different meanings.
In this document:
  * A [_lakeFS commit_][lakefs-commit] is a revision entry in the lakeFS
    repository log.
  * A [_Hadoop OutputCommitter commit_][hadoop-commit] (_HOC commit_ for
    short) is the action taken by a Hadoop OutputCommitter to finish writing
    all objects of a job to their final locations.

# Why <a href="#user-content-why" id="user-content-why">#</a>

## Summary

The LakeFSOutputCommitter will be:

* **Faster.** Because it uses native lakeFS features we can avoid the long
  troubled history of Spark writes on object stores.

  * 3x fewer API calls than when using the FileOutputCommitter v1 algorithm
	(which seems the most commonly used by our users).  Many of these are
	entirely useless statObject calls.
  * 2x fewer API calls that when using the FileOutputCommitter v2 algorithm
    (which seems less used, and is often not as safe as the v1 algorithm).
  * Around the same number of API calls as we would achieve if we supported
    the new magic committer.  Many of these are entirely useless statObject
    call.
* **Atomic.** All objects and partitions of a file appear at once as a
  committed object on the target branch. `_SUCCESS` files are still
  generated, but only as a courtesy to AirFlow and similar triggers.  A
  process that examines the target branch will never see a partial state.
* **Integrated.** A full Spark write is a native lakeFS commit, a
  first-class object in the lakeFS ecosystem.  It has a commit log (useful
  for lineage and structured metadata tagging), it can be reverted and
  merged, it has clear semantics for garbage collection, etc.

## Issues with existing OutputCommitters

OutputCommitters are charged with writing a directory tree (a multi-part
Hadoop file, for example a partitioned Parquet file) atomically.

Hadoop comes with several OutputCommitter algorithms.  For S3 these try to
bridge the gap between the Hadoop assumed semantics of a POSIX-ish
filesystem (HDFS) and the actual semantics of an object store (S3).

Of course, any Hadoop OutputCommitter that is unrelated to lakeFS can only
ever perform HOC commits.  Any lakeFS commits on the written data will need
to be separately managed by user code or manually.

The best correct committer is probably the [magic committer][magic].  It
works using only S3 (with its current consistency guarantees!), without
auxiliary systems such as a small HDFS or S3Guard.  The magic committer
uploads a directory tree by starting a multipart upload to its intended location,
creating a completion record for that multipart upload on S3, and uploading
the object without completing the upload.  To complete the upload, all
completion records are processed and the objects magically appear.

The [staging committer][staging] (aka "Netflix committer") is an older
committer for S3A.  It requires a "staging" filesystem on HDFS and offers
manual modes for overwriting outputs ("conflict resolution").

These issues remain with the magic and staging committer:

* Not atomic.  Multipart objects appear sequentially.  Finding a "_success"
  object indicator is still required before a directory tree can be
  processed.
* The magic committer requires "magic committer" support in the FileSystem,
  in order to write `__magic` objects to a _different_ path.  (Current
  lakeFSFS does not contain this support.)
* The magic committer is somewhat new:
  > It’s also not been field tested to the extent of Netflix’s committer;
  > consider it the least mature of the committers.
* Documentation is still somewhat lacking.

The both versions 1 and 2 of the FileOutputCommitter are not as good.  They
_rename_ files, which on S3A works by _copying and deleting_.  This is both
slow and highly non-atomic, as well as making it difficult to recover from
failed attempts.  The current recommendations are to use the partitioned
staging committer for overwriting or updating partitioned data trees, and
the magic committer is most other cases.

## LakeFSOutputCommitter

We propose to leverage the atomic capabilities of lakeFS to write a specific
OutputCommitter for lakeFSFS.  In the initial version, it will branch out,
prepare the desired output, and merge back in as part of the HOC commit.[^1]
Aborting will be done by dropping the branch (or repurposing it if the same
job ID is requested again).  Regular lakeFS retention can handle dropping
data objects; we might want to add file patterns to retention to allow
temporary objects to be dropped rapidly.

[^1]: A merge is a type of commit, of course, so in this model HOC commits
	*are* lakeFS commits!

# How

## Conflict modes

Spark supports multiple "save modes": Append, Overwrite, ErrorIfExists, and
Ignore.  These impact conflict resolution.  We will initially support just
overwrite modes: an entire previous table will be deleted on write.

## Sample flow

* User configures lakeFSFS and configures LakeFSOutputCommitter as the
  OutputCommitter for `lakefs` protocol Paths.  **Possibly** lakeFSFS will
  set up this OutputCommitter by default.

  Successive steps are controlled by Spark/Hadoop to output, and correspond
  to the Hadoop OutputCommitter protocol.
* **Setup (job/task TBD)**: Create a new branch for this job/task.  Its name
  is predictable from the job ID and/or task ID, so can be easily found
  again.  In "overwrite" mode, immediately delete the entire subtree of the
  intended output path, and possibly delete branches of previous tasks.
  This handles cases where the names of the objects written by the output
  format change, for instance because of repartitioning to a smaller (or
  different) number of partitions or because of nondeterminism in the names
  of the objects.
* **Write objects**: Everything is written to its correct path on the branch.
* **[HOC] Commit**: Merge back to the original branch.

## Properties

* The merge is performed by lakeFS so it is **atomic**.
* In the **single-writer case** the merge succeeds: no other operations
  occur on the subtree.
* **In-place updates** work: the old objects are deleted and replaced by new
  objects.  This is true regardess of partitioning etc.
* **Multiple writers** are detected and the first to HOC-commit succeeds.
  But all the others fail: they deleted the same previous files, or created
  a conflicting file (at least their `_SUCCESS` indicator).  So their lakeFS
  merge fails due to a conflict with the first (successful) merge.
* (Conflicting) **non-OutputCommitter writes are detected** and clearly
  handled.  As long as other writes create _one_ object with an overlapping
  name the merge will fail.  So LakeFSOutputCommitter can achieve its
  correct semantics regardless of other writers used.
* **Clearly correct by construction**: Rather than rely on single atomic
  operations and carefully tailoring operations to Spark retry mechanisms,
  we use lakeFS capabilities and guarantees.  Analyzing correctness becomes
  simpler.
* **Fast**: No data copies, just only (required) metadata operations.  Cost
  of the lakeFS commit is linear in the number of objects it touches (and a
  fast operation to add many thousands objects).  Total time to write is
  close to 3* faster than the existing FileOutputCommitter in v1 mode, close
  to 2* faster than the existing FileOutputCommitter in v2 mode (which is
  unsafe in various cases), and about as fast as the magic OutputCommitter
  _if_ lakeFSFS supported it.
* **Good semantics**: HOC commits will be lakeFS commits.  The history of a
  Spark job appears right in lakeFS history.  Metadata even includes some
  data lineage -- and in future we can easily add more, for instance as
  merge (lakeFS) commit user metadata.

### Implementation details

#### Hadoop >=3.1

Hadoop 3.1 offered a fairly complete overhaul of committer architecture,
configuration, and S3A support.  Supporting new committers on older Hadoops
will be challenging.  It also seems to be the version where the magic output
committer is recommended for use, so potentially our users will be there or
will agree to upgrade.

#### ParquetOutputCommitters

Parquet requires its committers to be
[`ParquetOutputCommitter`](https://github.com/apache/parquet-mr/blob/5608695f5777de1eb0899d9075ec9411cfdf31d3/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetOutputCommitter.java#L37)s
(of course it does), see e.g. [Cloudera's
explanation](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/bk_cloud-data-access/content/enabling-directory-committer-spark.html).
It's not intended to be derived from, and cannot easily use another
OutputCommitter.  However there's a
[`BindingParquetOutputCommitter`](https://github.com/apache/spark/blob/08e6f633b5bc3a7d8d008db2a264b1607d269f25/hadoop-cloud/src/hadoop-3/main/scala/org/apache/spark/internal/io/cloud/BindingParquetOutputCommitter.scala#L37)
in Spark (not in Hadoop, not in Parquet-MR, in *Spark*) that claims to
transform the selected output committer into a ParquetOutputCommitter.  This
is resolvable per documentation but will require some work.

## Future work

### More conflict resolution and save modes

Support all 4 Spark "save modes": Append, Overwrite, ErrorIfExists, and
Ignore.

### Multiple writers

We can support multiple concurrent writers and allow all writers to succeed
(keeping the results of the last writer).

We add a "merge-if" operation to lakeFS: atomically merge branch B into
branch A if branch A is at a given lakeFS commit.

Now change LakeFSOutputCommitter to loop during HOC commit:

* Attempt to merge the task branch into the source branch _if the source
  branch has not moved_.
* On failure:
  - Merge the source branch into the task branch using the "destination
	wins"[^2] strategy and delete all added files under the prefix (or add and
	use a new "merge but *never* copy from source branch" strategy).
  - Go back and attempt another merge.

This is essentially (noncooperative!) locking of output paths on top of
lakeFS, with no additional DB.  We can even add cooperation by means of
various locking hints, _informing_ multiple jobs about attempting to update
the same paths but keeping things safe regardless.

[^2]: 	Whenever there is a conflict, we want the task branch (which will become
    the "latest writer" after a successful HOC commit) to win.

# Potential wins

* Explicit requests _not_ to write 3 times and give better behaviour than
  the default FileOutputCommitter from Spark have appeared on our Slack
  [#data-architecture-discussion][slack-dont-write-thrice].
* Multiple users have requested "overwrite" save mode.
* Multiple users have requested multi-writer support.
* Note by a developer that Spark performs many lakeFS API operations when
  writing.

[magic]:  https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html#The_Magic_Committer
[staging]:  https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html#The_Staging_Committer
[lakefs-commit]:  https://docs.lakefs.io/understand/object-model.html#commits
[slack-dont-write-thrice]:  https://app.slack.com/client/T013V60QY06/C020N7X2Y0H/thread/C020N7X2Y0H-1660298516.202499
