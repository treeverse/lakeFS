---
layout: default
title: Branching Model
description: This page explains how lakeFS uses a Git-like branching model at its core.
parent: Understanding lakeFS
has_children: false
nav_order: 25
redirect_from:
    - ../branching/
    - ../branching/model.html
---

# Branching Model
{: .no_toc }

At its core, lakeFS uses a Git-like branching model.

{% include toc.html %}

### Repositories

In lakeFS, a _repository_ a logical namespace used to group together objects, branches and commits.
It can be considered the lakeFS analog of a bucket in an object store. Since it has version control qualities, it is also analogous to a repository in Git.

### Branches

Branches are similar in concept to [Git branches](https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging){:target="_blank" .button-clickable}.  
When creating a new branch in lakeFS, we are actually creating a consistent snapshot of the entire repository, which is isolated from other branches and their changes.  
Another way to think of branches is like a very long-lived database transaction, providing us with [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation){: target="_blank" .button-clickable}.

Once we've made the necessary changes to our data within our isolated branch, we can merge it back to the branch we branched from.  
This operation is atomic in lakeFS - readers will either see all our committed changes or none at all.

Isolation and Atomicity are very powerful tools: it allows us to do things that are otherwise extremely hard to get right: replace data in-place,
add or update multiple objects and collections as a single piece, run tests and validations before exposing data to others and more.

### Commits

Commits are immutable "checkpoints", containing an entire snapshot of a repository at a given point in time.
This is again very similar to commits in Git. Each commit contains metadata - who performed it, timestamp, a commit message as well as arbitrary key/value pairs we can choose to add.
Using commits, we can view our Data Lake at a certain point in its history and we are guaranteed that the data we see is exactly is it was at the point of committing it.

In lakeFS, different users can view different branches (or even commits, directly) at the same time on the same repository. there's no "checkout" process that copies data around. All live branches and commits are immediately available at all times.

### Objects

Objects in lakeFS are very similar to those found in S3 (or other object stores, for that matter). lakeFS is agnostic to what these objects contain: Parquet, CSV, ORC and even JPEG or other forms of unstructured data.   

Unlike Git, lakeFS does not care about the contents of an object - if we try to merge two branches that both update the same file, it is up to the user to resolve this conflict.  
This is because lakeFS doesn't assume anything about the structure of the object and so cannot try to merge both changesets into a single object (additionally, this operation makes little sense for machine generated files, and data in general).

The actual data itself is not stored inside lakeFS directly, but rather stored in an underlying object store. lakeFS will manage these writes, and will store a pointer to the object in its metadata database.
