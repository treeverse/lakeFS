# Object Retention: Top-Level Design

[Ariel Shaqed (Scolnicov)](mailto://ariels@treeverse.io)

## Introduction

Object retention deletes configured objects from the lake at a
prescribed time.  The user-visible portions of the feature are
entirely described by its [configuration reference][config reference].
This is a design for how to realize it.

## Process

### Configuration

Retention configuration is uploaded using a single `lakectl` command.
There is a single configuration for the entire repository, just like
AWS S3 Lifecycle Management.

Configuration is stored in the database.  Because we handle an entire
configuration at once during expiry, this is a single JSON-valued
field (or similar).

### Triggering

Running `lakefs expire` on the server with identical parameters to the
LakeFS server process performs all expiration.  Triggering is out of
scope for this design, beyond noting to document how to do it using
Cron and possibly Helm charts.

### Expiration query

The expiration query is an `UPDATE ... RETURNING ...` that immediately
marks objects as expired in the database.  All queries returning
objects will be predicated on `NOT expired` and will not see these
"tombstone" entries.  We do need to keep tombstones to recover from
failed object tagging in the next step; how to vacuum old tombstones
TBD.

As soon as this query returns the objects are no longer visible to
users.  However nothing changes on S3.

### Tagging

Results of the expiration query are passed to an AWS S3 [Batch Tagging
operation][batch tagging] with a special tag `lakefs-expire-now` (name
configurable and TBD).

Objects are actually deleted by S3 Object Lifecycle Management, using
a rule to delete objects with this tag.  We shall document the
required S3 lifecycle policy to use, as well as provide a single file
and command line to apply it to an entire bucket _if_ that bucket is
solely used by LakeFS.

This part is constrained by AWS S3 peculiarities.  S3 has no batch
deletion API.  There is a "bulk delete" API, but it only works for up
to 1000 objects per API call.  The easy way to delete many objects is
to use AWS Lifecycle Management, and there is a batch tagging API.

## Nongoals

This is a cleanup feature, and avoids complexity in return for
predictability.  Think of intended use-cases such as controlling
storage costs and managing GDPR, rather than workflow-level features.

In particular we do *not* provide:
* **Guaranteed atomicity on LakeFS.** Initially objects will expire
  atomically from LakeFS.  It is not clear that we can continue to do
  this for databases with many objects -- so we do not guarantee this
  implementation detail.
* **Predictable deletion time.** Objects expire some time after their
  configure lifetime.  There is no guarantee about when objects are
  actually removed (however there should be monitoring).  In
  particular some steps are controlled by AWS and LakeFS can never
  guarantee their timeliness.
* **Consistent deletion on S3.** Expired objects are removed by AWS as
  multiple operations.  There are no guarantees of consistency:
  objects will disappear from S3 in some order, not necessarily
  creation order.

## Dependencies

* [MVCC][mvcc]: For querying

## References

 <!-- BUG(ariels): this (probably) doesn't link anywhere useful -->
[configuration reference]: ../reference/retention
[mvcc]: https://github.com/treeverse/lakeFS/pull/223
[batch tagging]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
