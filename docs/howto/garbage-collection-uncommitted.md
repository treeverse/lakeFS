---
layout: default
title: Uncommitted Objects
description: Clean up uncommitted objects that are no longer needed.
parent: Garbage Collection
grand_parent: How-To
nav_order: 20
has_children: false
---

## Garbage collection: uncommitted objects <sup>BETA</sup>
{: .no_toc }

Note: Uncommitted GC is in Beta mode. Users should read this manual carefully and
take precautions before applying the actual delete ("sweep"), like copying the marked objects.
{: .note }

Deletion of objects that were never committed was always a difficulty for lakeFS, see
[#1933](https://github.com/treeverse/lakeFS/issues/1933) for more details. Examples for
objects that will be collected as part of the uncommitted GC job:
1. Objects that were uploaded to lakeFS and deleted.
2. Objects that were uploaded to lakeFS and were overridden.

{% include toc.html %}

See discussion on the original [design PR](https://github.com/treeverse/lakeFS/pull/4015) to understand why we didn't go with a server-only solution.
{: .note}

The uncommitted GC will not clean:
1. Committed objects. See [Committed Garbage Collection](./garbage-collection.html)
2. Everything mentioned in [what does not get collected](./gc-internals.md#what-does-_not_-get-collected)

### Prerequisites

1. lakeFS server version must be at least [v0.87.0](https://github.com/treeverse/lakeFS/releases/tag/v0.87.0).
   If your version is lower, you should first upgrade.
2. Read the [limitations](#limitations) section.
3. Setup [rclone](https://rclone.org/) to access underlying bucket for backup and restore.


### Running the uncommitted GC

1. Mark the files to delete - summary and report will be generated under `<REPOSITORY_STORAGE_NAMESPACE>/_lakefs/retention/gc/uncommitted/<MARK_ID>/`.
   By listing the bucket under 'uncommitted' the last entry represents the last mark ID of the uncommitted GC.
   The GC job prints out "Report for mark_id=..." which includes the mark ID with the run summary.

   ```bash
   spark-submit \
       --conf spark.hadoop.lakefs.gc.do_sweep=false \
       --conf spark.hadoop.lakefs.api.url=<LAKEFS_ENDPOINT> \
       --conf spark.hadoop.fs.s3a.access.key=<AWS_ACCESS_KEY_ID> \
       --conf spark.hadoop.fs.s3a.secret.key=<AWS_SECRET_ACCESS_KEY> \
       --conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY_ID> \
       --conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_ACCESS_KEY> \
       --class io.treeverse.gc.UncommittedGarbageCollector \
       --packages org.apache.hadoop:hadoop-aws:2.7.7 \
       <APPLICATION-JAR-PATH> <REPOSITORY_NAME> <REGION>
   ```

2. Backup (optional but recommended) - when you start using the feature you may want to first gain confidence in the decisions uncommitted GC makes. Backup will copy the objects marked to be deleted for run ID to a specified location.
   Follow [rclone documentation](https://rclone.org/docs/) to configure remote access to lakeFS storage.
   Note that the lakeFS and backup locations are specified as `remote:path` based on how rclone was configured.

   ```shell
   rclone --include "*.txt" cat "<LAKEFS_STORAGE_NAMESPACE>/_lakefs/retention/gc/uncommitted/<MARK_ID>/deleted.text/" | \
     rclone -P --no-traverse --files-from - copy <LAKEFS_STORAGE_NAMESPACE> <BACKUP_STORAGE_LOCATION>
   ```

4. Sweep - delete reported objects to delete based on mark ID

   ```bash
   spark-submit \
       --conf spark.hadoop.lakefs.gc.mark_id=<MARK_ID> \
       --conf spark.hadoop.lakefs.gc.do_mark=false \
       --conf spark.hadoop.lakefs.api.url=<LAKEFS_ENDPOINT> \
       --conf spark.hadoop.fs.s3a.access.key=<AWS_ACCESS_KEY_ID> \
       --conf spark.hadoop.fs.s3a.secret.key=<AWS_SECRET_ACCESS_KEY> \
       --conf spark.hadoop.lakefs.api.access_key=<LAKEFS_ACCESS_KEY_ID> \
       --conf spark.hadoop.lakefs.api.secret_key=<LAKEFS_SECRET_ACCESS_KEY> \
       --class io.treeverse.gc.UncommittedGarbageCollector \
       --packages org.apache.hadoop:hadoop-aws:2.7.7 \
       <APPLICATION-JAR-PATH> <REPOSITORY_NAME> <REGION>
   ```

5. Restore - in any case we would like to undo and restore the data from from our backup. The following command will copy the objects back from the backup location using the information stored under the specific mark ID.
   Note that the lakeFS and backup locations are specified as `remote:path` based on how rclone was configured.

   ```shell
   rclone --include "*.txt" cat "remote:<LAKEFS_STORAGE_NAMESPACE>/_lakefs/retention/gc/uncommitted/<MARK_ID>/deleted.text/" | \
     rclone -P --no-traverse --files-from - copy <BACKUP_STORAGE_LOCATION> <LAKEFS_STORAGE_NAMESPACE>
   ```


### Uncommitted GC job options
{: .no_toc }

Similar to the [committed GC option](./garbage-collection.md#gc-job-options).

### Limitations

The uncommitted GC job has several limitations in its Beta version:
1. Support is limited to S3 repositories, it was not tested on ABS, GS or MinIO.
1. Scale may be limited, see performance results below.
1. [Issue](https://github.com/treeverse/lakeFS/issues/5088) associated to commit during copy object.

### Next steps

The uncommitted GC is under development, next releases will include:

1. Incorporation of committed & uncommitted GC into a single job. We understand the friction
   of having 2 garbage collection jobs for a lakeFS installation and working to creating a
   single job for it.
2. Removing the limitation of a read-only lakeFS during the job run.
3. Performance improvements:
    1. Better parallelization of the storage namespace traversal.
    2. Optimized Run: GC will only iterate over objects that were written to the
       repository since the last GC run. For more information see the [proposal](https://github.com/treeverse/lakeFS/blob/master/design/accepted/gc_plus/uncommitted-gc.md#flow-2-optimized-run).
4. Backup & Restore, similar to [committed GC](./garbage-collection.md#backup-and-restore).
5. Support for non-S3 repositories.

### Performance
{: .no_toc }

The uncommitted GC job was tested on a repository with 1K branches,
25K uncommitted objects and 2K commits.
The storage namespace number of objects prior to the cleanup was 103K objects.
The job ran on a Spark cluster with a single master and 2 workers of type [i3.2xlarge](https://aws.amazon.com/ec2/instance-types/i3/)
The job finished after 5 minutes deleting 15K objects.

