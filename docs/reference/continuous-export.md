---
layout: default
title: Continuous export
parent: Reference
nav_order: 10
has_children: false
---
# Continuous Export

You can configure a lakeFS repository can store the latest of a branch on an external
object store using its native paths and access methods.  This allows clients and tools
read-only access to repository objects without any lakeFS-specific configuration.
Initially, only AWS S3 is supported.

For instance, the contents `lakefs://example@master` might be stored on
`s3://company-bucket/example/latest`.  Clients entirely unaware of lakeFS could use that
base URL to access latest files on `master`.  Clients aware of lakeFS can continue to use
the lakeFS S3 endpoint to access repository files on `s3://example/master`, as well as
other versions and uncommitted versions.

## Configuration

To configure continuous export for branch `master`, run

```shell
lakefs repo export --branch master --start --export-path s3://company-bucket/path/to/export/ [--export-status-path s3://company-bucket/path/to/status]
```

To stop continuous export, run

```shell
lakefs repo export --branch master --stop
```

### Alternatives

#### Multiple branches

If we go with per-branch export this is almost the exact same command!

```shell
lakefs branch export --branch master --start --export-path s3://company-bucket/path/to/export/
```

#### One-time export

Either get rid of the `--start` or just support one-time export as a command!

## Operation

When continuous export is enabled for a branch its current state is exported to S3.  From
then on, every commit or merge operation to that branch will be exported _*optional:* just
as though a non-continuous export were run_.  The commit or merge operation on the branch
returns as soon as export starts.

The export process runs for a while.  To help track its status it creates a file
`_STATUS_<timestamp>.json`.  This file contains multiple JSON records, each on a single
line:
* An initial record with fields
  - `"status"`: one of `"success"`, `"failure"`, `"creating"`, or `"in-progress"`.
  - `"message"`: a human-readable message, the error message for status `"failure"`.
  - `"commit"`: the commit-ID of the export.
* When status is not `"creating"`, a record for each exported file with fields
  - `"path"`: the path of the object within the bucket.  The exported object will be on
    that path under the export path specified.
  - `"etag"`: the etag of the exported object.  You can use this to ensure the correct
    version of the object is visible.

Note that S3 does not offer atomic operations.  Objects read are only valid while the
status file shows `"success"` and the `"commit"` is unchanged!
