---
title: Data Structure
parent: Understanding lakeFS
description: Understand the data structure in lakeFS
---

# How Does lakeFS Store Your Data
lakeFS being a data versioning engine, requires the ability to save multiple versions of the same object. As a result, lakeFS stores objects in the object store
in way that allows it to version the data in an efficient way.
This might cause confusion when trying to understand where our data is actually being stored. This page will try to shed a light on this subject.

## lakeFS Repository Namespace Structure
lakeFS stores repository data and metadata under the repository's namespace. The lakeFS repository namespace is a dedicated path under the object store used by lakeFS to manage a repository.
Listing a repository storage namespace in the object store will provide the following output:

```shell
aws s3 ls s3://<storage_namespace>/
                        PRE _lakefs/
                        PRE data/
```

lakeFS stores the actual user data under the `data/` prefix. The `_lakefs/` prefix is used to store commit metadata which includes [range and meta-range](../understand/how/versioning-internals.md) files and internal lakeFS data.
Since lakeFS manages immutable data, objects are not saved using their logical name - these might get overwritten, violating the immutability guarantee. This means that when you upload a csv file called `allstar_games_stats.csv` to branch main, lakeFS will generate a random physical
address under the `data/` prefix and upload it to there.  
Mapping from a path to an object changes as you upload, commit, and merge on lakeFS. When updating an object, lakeFS will create a new physical address for that version preserving other versions of that object.
lakeFS will link between the object's logical address and its physical address - and store that relation under the given commit metadata (range and meta-range)

lakeFS uses its object store immutably i.e. anything uploaded is never changed or overridden (Refer to [GC](../howto/garbage-collection/index.md) for explanation on how and when lakeFS actually deletes data from the storage).  
To find data, lakeFS uses the logical address e.g. `lakefs://my-repo/main/allstar_games_stats.csv`, indicating a repository and branch.
Using the [KV metadata store](../understand/how/versioning-internals.md#representing-references-and-uncommitted-metadata), lakeFS will first try to find any uncommitted version of the object in the given branch. If no uncommitted version exist, it will take the latest committed version from the branch head (which is the top commit of the branch)

1. In the KV metadata store under the current staging token of branch main. This will return any uncommitted changes for the given object
2. Read it from the branch's head meta-range and range (which are saved under the `_lakefs` prefix in the object store. This will return the metadata for the object as it was stored in the latest commit for branch main.  
The physical path returned will be in the form of `s3://<storage_namespace>/data/gp0n1l7d77pn0cke6jjg/cg6p50nd77pn0cke6jk0`. The same object in lakeFS might have several physical addresses, one for each version where it exists.

## Finding an object's location on your object store
One way to determine the physical location of an object is using the `lakectl fs stat` command:

```bash
lakectl fs stat --pre-sign=false lakefs://my-repo/main/allstar_games_stats.csv
Path: allstar_games_stats.csv
Modified Time: 2024-08-02 10:13:33 -0400 EDT
Size: 0 bytes
Human Size: 0 B
Physical Address: s3://niro-test/repos/docs/data/data/geh1jurck6tfom0s1t8g/cqmej33ck6tfom0s1tvg
Checksum: d41d8cd98f00b204e9800998ecf8427e
Content-Type: application/octet-stream
```

lakeFS can show any version of an object. For example: to see an object's physical location on branch `dev` from 3 versions ago, use reference dev~3:

```bash
lakectl fs stat lakefs://my-repo/dev~3/allstar_games_stats.csv
Path: allstar_games_stats.csv
Modified Time: 2024-08-02 10:11:49 -0400 EDT
Size: 916393 bytes
Human Size: 916.4 kB
Physical Address: s3://<storage_namespace>/data/data/geh1jurck6tfom0s1t8g/cqmei9bck6tfom0s1tt0
Checksum: 48e04a4c072acdcf932ee6c43f46ef14
Content-Type: application/octet-stream
```

This can be done using any lakeFS reference type.

To learn more about the internals of lakeFS and how it stores your data, follow [this blog post](https://lakefs.io/blog/where-is-my-data/)
