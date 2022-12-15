---
layout: default
title: Performance Best Practices
parent: Understanding lakeFS
description: This section suggests performance best practices to work with lakeFS.
nav_order: 26
has_children: false
--- 
# Performance Best Practices
{: .no_toc }

{% include toc.html %}

## Overview
Use this guide to achieve the best performance with lakeFS.

## Avoid concurrent commits/merges
Just like in Git, branch history is composed by commits and is linear by nature. 
Concurrent commits/merges on the same branch result in a race. The first operation will finish successfully while the rest will retry.

## Perform meaningful commits
It's a good idea to perform commits that are meaningful in the senese that they represent a logical point in your data's lifecycle. While lakeFS supports arbirartily large commits, avoiding commits with a huge number of objects will result in a more comprehensible commit history.

## Use zero-copy import
To import object into lakeFS, either a single time or regularly, lakeFS offers a [zero-copy import](../howto/import.md#zero-copy-import) feature.
Use this feature to import a large number of objects to lakeFS, instead of simply copying them into your repository.
This feature will create a reference to the existing objects on your bucket and avoids the copy.

## Read data using the commit ID
In cases where you are only interested in reading committed data: 
* Use a commit ID (or a tag ID) in your path (e.g: `lakefs://repo/a1b2c3`).
* Add `@` before the path  `lakefs://repo/main@/path`.

When accessing data using the branch name (e.g. `lakefs://repo/main/path`) lakeFS will also try to fetch uncommitted data, which may result in reduced performance.
For more information, see [how uncommitted data is managed in lakeFS](../understand/how/versioning-internals.md#representing-references-and-uncommitted-metadata)

## Operate directly on the storage
Sometimes, storage operations can become a bottleneck. For example, when your data pipelines upload many big objects.
In such cases, it can be beneficial to perform only versioning operations on lakeFS, while performing storage reads/writes directly on the object store.
lakeFS offers multiple ways to do that:
* The [`lakectl upload --direct`](../reference/cli.md#lakectl-fs-upload) command (or [download](../reference/cli.md#lakectl-fs-download)).
* The lakeFS [Hadoop Filesystem](../integrations/spark.md#use-the-lakefs-hadoop-filesystem).
* The [staging API](../reference/api.md#objects/stageObject) which can be used to add lakeFS references to objects after having written them to the storage.

Accessing the object store directly is a faster way to interact with your data.

## Zero-copy
lakeFS provides a zero-copy mechanism to data. Instead of copying the data, we can check out to a new branch. 
Creating a new branch will take constant time as the new branch points to the same data as its parent.
It will also lower the storage cost.
