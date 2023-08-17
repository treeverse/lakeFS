---
title: Internals
description: How Garbage Collection in lakeFS works
parent: Garbage Collection
grand_parent: How-To
nav_order: 1
has_children: false
redirect_from: 
    - /howto/gc-internals.html
---

# Committed Garbage Collection Internals

{: .warning-title }
> Deprecation notice
>
> This page describes a deprecated feature. Please visit the new [garbage collection documentation](./index.html).


## What gets collected

Because each object in lakeFS may be accessible from multiple branches, it
might not be obvious which objects will be considered garbage and collected.

Garbage collection is configured by specifying the number of days to retain
objects on each branch. If a branch is configured to retain objects for a
given number of days, any object that was accessible from the HEAD of a
branch in that past number of days will be retained.

The garbage collection process proceeds in three main phases:

1. **Discover which commits will retain their objects.**  For every branch,
  the garbage collection job looks at the HEAD of the branch that many days
  ago; every commit at or since that HEAD must be retained.

    ```mermaid
      %%{init: { 'theme': 'base', 'gitGraph': {'rotateCommitLabel': true}} }%%
      gitGraph
        commit id: "2022-02-27 🚮"
        commit id: "2022-03-01 🚮"
        commit id: "2022-03-09"
      branch dev
      checkout main
        commit id: "2022-03-12"
      checkout dev
        commit id: "d: 2022-03-14 🚮"
        commit id: "d: 2022-03-16 🚮"
      checkout main
        commit id: "2022-03-18"
      checkout dev
        commit id: "d: 2022-03-20 🚮"
        commit id: "d: 2022-03-23"
      checkout main
      merge dev
        commit id: "2022-03-26"
    ```

    Continuing the example, branch `main` retains for 21 days and branch `dev`
    for 7. When running GC on 2022-03-31:

    - 7 days ago, on 2022-03-24 the head of branch `dev` was `d:
      2022-03-23`. So, that commit is retained (along with all more recent
      commits on `dev`) but all older commits `d: *` will be collected.
    - 21 days ago, on 2022-03-10, the head of branch `main` was
      `2022-03-09`. So that commit is retained (along with all more recent
      commits on `main`) but commits `2022-02-27` and `2022-03-01` will be
      collected.

1. **Discover which objects need to be garbage collected.** Hold (_only_)
  objects accessible on some retained commits.

    In the example, all objects of commit `2022-03-12`, for instance, are
    retained. This _includes_ objects added in previous commits. However,
    objects added in commit `d: 2022-03-14` which were overwritten or
    deleted in commit `d: 2022-03-20` are not visible in any retained commit
    and will be garbage collected.

1. **Garbage collect those objects by deleting them.** The data of any
  deleted object will no longer be accessible. lakeFS retains all metadata
  about the object, but attempting to read it via the lakeFS API or the S3
  gateway will return HTTP status 410 ("Gone").

## What does _not_ get collected

Some objects will _not_ be collected regardless of configured GC rules:
* Any object that is accessible from any branch's HEAD.
* Objects stored outside the repository's [storage namespace][storage-namespace].
  For example, objects imported using the lakeFS import UI are not collected.
* Uncommitted objects, see [Uncommitted Garbage Collection](./uncommitted.html),

## Performance

Garbage collection reads many commits.  It uses Spark to spread the load of
reading the contents of all of these commits.  For very large jobs running
on very large clusters, you may want to tweak this load.  To do this:

* Add `-c spark.hadoop.lakefs.gc.range.num_partitions=RANGE_PARTITIONS`
  (default 50) to spread the initial load of reading commits across more
  Spark executors.
* Add `-c spark.hadoop.lakefs.gc.address.num_partitions=RANGE_PARTITIONS`
  (default 200) to spread the load of reading all objects included in a
  commit across more Spark executors.

Normally this should not be needed.

## Networking

Garbage collection communicates with the lakeFS server.  Very large
repositories may require increasing a read timeout.  If you run into timeout errors during communication from the Spark job to lakeFS consider increasing these timeouts:

* Add `-c spark.hadoop.lakefs.api.read.timeout_seconds=TIMEOUT_IN_SECONDS`
  (default 10) to allow lakeFS more time to respond to requests.
* Add `-c
  spark.hadoop.lakefs.api.connection.timeout_seconds=TIMEOUT_IN_SECONDS`
  (default 10) to wait longer for lakeFS to accept connections.

[storage-namespace]:  {% link understand/glossary.md %}#storage-namespace
