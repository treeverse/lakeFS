---
layout: default
title: Continuous export
parent: Reference
nav_order: 10
has_children: false
---
# Continuous Export

You can configure a lakeFS repository to store the latest of a branch on an external
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
lakefs branch export --branch master --start --export-path s3://company-bucket/path/to/export/ [--export-status-path s3://company-bucket/path/to/status]
```

To stop continuous export, run

```shell
lakefs branch export --branch master --stop
```

This works any number of your branches, not only `master`.

If you omit the `--start` flag, the branch tip will be exported just once, at that time.

```shell
lakefs branch export --branch experiment1 --export-path s3://company-bucket/path/to/experiment/ [--export-status-path s3://company-bucket/path/to/experiment_status]
```

## Operation

When continuous export is enabled for a branch its current state is exported to S3.  From
then on, every commit or merge operation to that branch will be exported.  The commit or
merge operation on the branch returns as soon as export starts.  Multiple commits to the
same exported branch may cause some otherwise-overlapping exports to be skipped.
Eventually the last export will appear.  This is in line with "eventual consistency" of
S3.

The export process runs for a while.  When it completes successfully it writes an empty
file `_STATUS` under the configured export-path.

For more granular status tracking, set an export-status-path.  Export writes two files to that path:

1. A file `_STATUS_<timestamp>.json`, updated on every status change to hold a single JSON
   record with fields
  - `"status"`: one of `"success"`, `"failure"`, `"pending"`, or `"in-progress"`.
  - `"message"`: a human-readable message, the error message for status `"failure"`.
  - `"commit"`: the commit-ID of the export.
  - (maybe?) `"commit-message"`: the message associated with that commit.
2. A file `_MANIFEST_<timestamp>.json` written when entering state `in-progress`.  It
   contains newline-separated records for each exported file with fields
  - `"path"`: the path of the object within the bucket.  The exported object will be on
    that path under the export path specified.
  - `"etag"`: the etag of the exported object.  You can use this to ensure the correct
    version of the object is visible.
  - (maybe?) `"version-id"`: the version ID of the object, if versioned.  (Without a
    version ID it can be very hard to retrieve a non-latest version of the object!)

Note that S3 does not offer atomic operations.  Objects read are only valid while the
`_STATUS` file shows `"success"` and the `"commit"` is unchanged!

## FAQs

#### What happens if a continuously exported branch changes while a previous export is still in progress?

lakeFS exports objects safely and will not export the same branch twice concurrently.
However when multiple concurrent exports occur intermediate exports cannot be observed and
may be dropped.  A dropped intermediate export does not create the `_SUCCESS` or
`_STATUS_<timestamp>.json` and `_MANIFEST_<timestamp>.json` files; conversely, once these
appear the export will run to completion.

#### How consistent is export once the `_SUCCESS` file appears?

Once the `_SUCCESS` is present, or once the `_STATUS_<timestamp>.json` file holds status
`success`, the export has finished:

* Any file can be accessed.
* Files names appear in the `_MANIFEST_<timestamp>.json` file.
* S3 file listings will eventually show all accessible files.

lakeFS exports to S3 and is bound by its consistency guarantees.
