# Staging on the object store

## Overview

The main idea is to change the API slightly to work around the need to synchronize state between object store paths or object store / DB.

To acheieve this, I suggest introducing another Git concept: the "working tree". When writing, users need to get a **"workspace ID"** from lakeFS,
write/read through it, and then either `git add && git commit`, or `git commit -a` the changes in that workspace.

This moves some of the concurrency control from lakeFS to the user.
I believe this new responsibility could be mostly abstracted away from the user.

## Benefits

- closer in model to Git
- as scalable as the underlying object store - millions of ops/second are possible.
- state in external DB: only HEADs (they require CAS/conditional writes/some form of concurrency control)
- Users can rely on IAM permisssions!
- Users can rely on S3 lifecycle!
- Users can start using lakeFS with no external DB at all, on a single instance (i.e. `docker run` instead of current compose)
- Makes it easier to integrate with things like Kinesis Firehose, Kafka Connect, other managed ingest tools
- Much much simpler than Raft: both operationally for the user, for us to develop/maintain and for the community to reason about

## Downsides

- The client needs to do more: hold on to a workspace ID and commit after done writing to it. In general, **thinking** about concurrency is a burden.
- Working with streaming data where we're never "done" requires some external tooling or internal integrations.

## Pseudo API:

```scala
master : = lakefs.repo("my-repo").branch("master")
ws = new Workspace(master)

// read
df = spark.read.parquet(ws.path("my/collection"))

// write
df.write.parquet(ws.path("another/location"))

// commit!
ws.all().commit("commit everything!")

// or add stuff
ws.add("**/*.parquet", "**/*.json", "another/location/_SUCCESS")
    .commit("a commit of only some of the stuff!")

// or revert it
ws.revert()
```

## How does it work?

basically, when calling `new Workspace(Branch)`, lakeFS returns a path with a newly generated UUID: `s3://${storageNamespace}/${uuid.New()}`
 That's it. It doesn't keep track of it (more on that later).

Writes just append the path to be written to the workspace path: `${workspaceAddr}/${writePath}`
Deletes are also writes, but of a sentinel tombstone value.
Reads are now a getObject/listObjects call on that prefix, implementing our "DiffIterator".

When calling `commit`, we take that diffIterator and apply it. Once we have a commit ID, we do a `CAS` on the ref: if no one else committed so far, this completes the commit process successfully. Otherwise, its an `ErrConcurrentCommit` and should be retried/aborted.
Reading these refs is done using the same batching method we currently do for PG, so we could support pretty much ANY external storage for this as long as it is:

* Read-after-write consistent
* Has at least one of `{CAS, conditional-write, serializable transaction}`

On this list are: DynamoDB (which we can abstract away(!)), PostgreSQL, MySQL, MongoDB, even a Hive Metastore..
Using good object stores (i.e., practically anything other than S3), this could be done with no DB at all, with some additional latency.

It is the user's responsibility to ensure they commit at the end of their job/pipeline/script,
but it's possible to pass around that `workspaceID` between jobs and processes, even if reading/writing concurrently.

Once a commit is done for a given workspace, it should no longer be used. if using the lakeFS SDK (as shown above),
calling any operation on a `commit()`ed `Workspace` object will raise an error. We can go further and place a "lockfile" (`_lakefs_committed/${workspaceID}`) and check it before writing if we think this is neccesary (on a best effort basis).

## Using the S3 gateway

just append a workspace ID: `s3://my-repo/master+abc123/my/collection/` to read/write.
otherwise, just pass a ref as you normally would, to read committed data.

## Out-of-band ingest

Now that a commit is basically a diffIterator on top of `listObjects` we can do something similar for stream processing.
Working with streams is different: we can't always ensure no writes are happening, but fortunately, most streaming processors 
do [guarantee immutability](https://stackoverflow.com/a/61201083): A written file is never overwritten, only written once and potentially deleted: this allows us to take
a regular old object store path and do:

```sh
$ lakectl ingest commit \
    --from-path "s3://bucket/kafka/events/2021/" \
    --to-path "lakefs://my-repo@ingest-branch/kafka/events/2021/" \
    -m "ingest events: $(date -u)"

# Or just see what changes would be introduced
$ lakectl ingest diff \
    --from-path "s3://bucket/kafka/events/2021/" \
    --to-path "lakefs://my-repo@ingest-branch/kafka/events/2021/" \
```
Here, instead of treating the given object store location as a diffIterator, we look at it as a subtree, to be diffed with the underlying commit.


## IAM permissions

set them normally on `s3://storagenamespace/*/path/a`, `s3://storagenamespace/*/path/b`, etc.


## S3 Lifecycle rules

when writing, we can pass tags using the SDK. let's say we write to `ws.path("foo/bar/baz.parquet")`:
we can tag that write with `path[0]=foo,path[1]=bar,path[2]=baz.parquet`

Now we can set a lifecycle rule: delete after 30 days if tags match:

```xml
<Tags>
    <Tag>
        <key>path[0]</key>
        <value>foo</key>
    </Tag>
    <Tag>
        <key>path[1]</key>
        <value>bar</key>
    </Tag>
</Tags>
```

We can provide tooling to generate or configure those on behalf of the user.

## Standalone mode

Running on a single instance we don't *need* a CAS operation, we can serialize access to the ref keys in-process, so a ref could sit in any object store or local directory. `lakefs -dev` now works without a database (I'd still show a big red warning in the UI when running like this - also true for the local:// adapter in general).


## Uploading/diff staged in the UI

When uploading objects we can generate a workspace ID and stick it in `localStorage`.
From there the experience is mostly the same as it is today, with the "changes" tab now accepting a workspaceID (using what's in localstorage for the current branch as default).


## Deduping

Can still be done as an external management job using a commit dataframes

