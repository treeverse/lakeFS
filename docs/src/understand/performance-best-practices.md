---
title: Performance Best Practices
description: This section suggests performance best practices to work with lakeFS.
--- 
# Performance Best Practices

## Overview
Use this guide to achieve the best performance with lakeFS.

## Avoid concurrent commits/merges

Just like in Git, branch history is composed by commits and is linear by nature. 
Concurrent commits/merges on the same branch result in a race. The first operation will finish successfully while the rest will retry.

## Perform meaningful commits

It's a good idea to perform commits that are meaningful in the senese that they represent a logical point in your data's lifecycle. While lakeFS supports arbirartily large commits, avoiding commits with a huge number of objects will result in a more comprehensible commit history.

## Use zero-copy import

To import object into lakeFS, either a single time or regularly, lakeFS offers a [zero-copy import][zero-copy-import] feature.
Use this feature to import a large number of objects to lakeFS, instead of simply copying them into your repository.
This feature will create a reference to the existing objects on your bucket and avoids the copy.

## Read data using the commit ID

In cases where you are only interested in reading committed data:

* Use a commit ID (or a tag ID) in your path (e.g: `lakefs://repo/a1b2c3`).
* Add `@` before the path  `lakefs://repo/main@/path`.

When accessing data using the branch name (e.g. `lakefs://repo/main/path`) lakeFS will also try to fetch uncommitted data, which may result in reduced performance.
For more information, see [how uncommitted data is managed in lakeFS][representing-refs-and-uncommitted-metadata].

## Operate directly on the storage

Sometimes, storage operations can become a bottleneck. For example, when your data pipelines upload many big objects.
In such cases, it can be beneficial to perform only versioning operations on lakeFS, while performing storage reads/writes directly on the object store.
lakeFS offers multiple ways to do that:

* The [`lakectl fs upload --pre-sign`][lakectl-upload] command (or [download][lakectl-download]).
* The lakeFS [Hadoop Filesystem][hadoopfs].
* The [staging API][api-staging] which can be used to add lakeFS references to objects after having written them to the storage.

Accessing the object store directly is a faster way to interact with your data.

## Zero-copy

lakeFS provides a zero-copy mechanism to data. Instead of copying the data, we can check out to a new branch. 
Creating a new branch will take constant time as the new branch points to the same data as its parent.
It will also lower the storage cost.


[hadoopfs]:  /integrations/spark/#lakefs-hadoop-filesystem
[zero-copy-import]:  /howto/import/#zero-copy-import
[lakectl-upload]:  /reference/cli/#lakectl-fs-upload
[lakectl-download]:  /reference/cli/#lakectl-fs-download
[api-staging]:  /reference/api/#operations-objects-stageObject
[representing-refs-and-uncommitted-metadata]:  /understand/how/versioning-internals/#representing-references-and-uncommitted-metadata
