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

A one-time export copies all objects on a branch to a location on S3, once.  A continuous
export tracks the latest state of a branch on a location in S3: it updates that location
in S3 after each change in lakeFS (either a commit or a merge)

## Configuration and management

Configuration and management are performed via a CLI that calls a simple REST API on the
lakeFS server.  Either can be used.  You may configure continuous export on any number of your
branches.

### CLI

The command `lakectl export` is the entrypoint to all export commands.

#### `lakectl export get`

Read the current export configuration for a branch using
```shell
lakectl export get lakefs://REPO@BRANCH
```

For instance,  `./lakectl export get lakefs://foo@master`.

#### `lakectl export set`

Set an export configuration for a branch by using
```shell
lakectl export set lakefs://REPO@BRANCH FLAGS...
```

Use these flags:
* `--continuous`: enables _continuous export_, after every commit or merge to the branch.
  Must be specified, so set `--continuous=false` to immediately start one single export.
* `--path`: specifies the destination path to export to.
* `--status-path`: specifies the export status path, under which a file is written giving the
  status after each export.
* `--prefix-regex`: identifies prefixes (directories) in which to place "export done" objects
  (useful for triggering workflows).  May be repeated to specify multiple regular expressions.
  
#### `lakectl export run`

Start exporting the requested branch now by using
```shell
lakectl export run lakefs://REPO@BRANCH
```

#### `lakectl export repair`

If an export fails, for instance if the daemon has no permissions to write to the export
directory, lakeFS has no way of determining how best to proceed or when to try to proceed.
After repairing the issue, e.g. by granting permissions, restart by using
```shell
lakectl export repair lakefs://REPO@BRANCH
```

### API

#### Configuration

Configure and query continuous export configuration of a branch with a `PUT` or a `GET` to
`/repositories/{repository}/branches/{branch}/continuous-export`.  Pass these JSON
parameters in the body:
```json
{
  "exportPath": "s3://company-bucket/path/to/export", // export objects to this path (required)
  "exportStatusPath": "s3://company-bucket/path/to/status", // write status to this path (optional)
  "lastKeysInPrefixRegexp": [
    // list of regexps of keys to exported last in each prefix (for signalling) (optional)
    ".*/_SUCCESS"
  ]
}
```
Once set on a branch, every commit or merge to that branch will be exported.

#### Operation

Export current branch state just once, without continuous operation, with `POST` to
`/repositories/{repository}/branches/{branch}/export-hook`.

Resume export after repairing with a `POST` to
`/repositories/{repository}/branches/{branch}/repair-export`.

## Operation

When continuous export is enabled for a branch its current state is exported to S3.  From
then on, every commit or merge operation to that branch will be exported.  The commit or
merge operation on the branch returns as soon as export starts.  Multiple commits to the
same exported branch may cause some otherwise-overlapping exports to be skipped.
Eventually the last export will appear.  This is in line with "eventual consistency" of
S3.

The export process runs for a while and copies all files.  This process is highly parallel and
does not maintain the creation or modification orders of the source files.  Export can write
specific status files _after_ all other files in a directory. Set `lastKeysInPrefixRegexp` to
match directories that need a status file. lakeFS export will create an empty file
`_lakefs_success` in each matching directory.  These status files are written *last* in their
S3 prefix, allowing triggers based on S3 write ordering to succeed.

Export can additionally write a status file to the export status path.  If set, lakeFS will
write a file `<REPO>-<BRANCH>-<COMMIT>` to that directory when done exporting.  It looks like:
> ``` > status: <STATUS>, signalled_errors: <NUM-ERRORS> > ```

The status will be one of:

* `exported-successfully`
* `export-failed`
* `export-repaired`

S3 does not offer atomic operations across objects.  Exported objects read from S3 remain
valid only while the above `_STATUS` file shows `"success"` and the `"commit"` is unchanged!

## FAQs

#### What happens if a continuously exported branch changes while a previous export is still in progress?

lakeFS exports objects safely and will not export the same branch twice concurrently.
However when multiple concurrent exports occur intermediate exports cannot be observed and
may be dropped.  A dropped intermediate export does not create the `_SUCCESS` or
`_STATUS_<timestamp>.json` and `_MANIFEST_<timestamp>.json` files; conversely, once these
appear the export will run to completion.

#### How consistent is export once the `_STATUS_<timestamp>.json` file appears?

Once the `_STATUS_<timestamp>.json` file holds status `success`, the export has finished,
and S3 rules apply:

* Any file can be accessed.
* Files names appear in the `_MANIFEST_<timestamp>.json` file.
* However S3 file listings will only eventually show all accessible files.

lakeFS exports to S3 and is bound by its consistency guarantees.

#### How can I trigger processes external to lakeFS to run after an export?

The file `_STATUS_<timestamp>.json` contains success -- and a relevant commit-ID -- once
export is done.  You can use the file and its contents to trigger after an export ends.

You can also use existing workflows that are based on `_SUCCESS` files (like those created
by Hadoop or by Spark).  Just configure the a regular expression matching those files in
`lastKeysInPrefixRegexp`, to ensure that each appears last in its S3 prefix.

#### Can I export lakeFS metadata to S3?

Configuration option `exportStatusPath` exports some metadata about the exported commit to S3:
* Export status
* Commit ID and message
* Manifest of all objects in the commit, including:
  - all keys
  - ETag of each file
