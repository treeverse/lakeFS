---
layout: default
title: What is lakeFS
nav_order: 0
---

# What is lakeFS
{: .no_toc }

**Disclaimer:** This is an early access version intended for collecting feedback and should not be used in production. API and data model are expected to change.
{: .note .pb-3 }

lakeFS is an open source project that empowers your object storage data lake (e.g. S3, GCS, Azure Blob) with ACID guarantees and Git-like collaboration capabilities, such as data quality assurance by data CI/CD, data reproducibility by time travel to previous lake versions, and reducing costs of mistakes by allowing instant revert. 
{: .pb-5 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc .pb-5 }


## Why? What are Object Stores missing when used as a Data Lake?



1. **Isolation:** Object stores make it easy to ingest data into the lake, making it available to others. However, by doing so, they provide no isolation guarantees: readers can access data while it's being written; bulk move/rename operations mean readers will see intermittent state.

   

2. **Consistency:** Object Stores (S3 in particular) are eventually consistent for some operations. This requires workarounds such as EMRFS or s3Guard that help mitigate this to an extent, but don't provide the primitives to allow real, cross-collection consistency: changing many objects together as an atomic operation.
   

3. **Lack of rollback/revert:** By working at the single object level, mistakes become costly. If a retention job accidentally deletes a huge number of objects, reverting such a change is very hard: common object-level versioning makes this theoretically possible, but very costly in both time and money. It makes the feedback loop much longer since there's no real way to experiment with the data without fearing data loss or breaking changes for others.

   

4. **Hard to manage and control:** Teams managing large Data Lakes usually have some conventions and policies around the data they contain. For example, enforcing the use of a certain format, rules around breaking schema changes and automated tests for job output. The mutable nature of Data Lakes means you can run these tests only after the data has already been written. 

   

5. **Lack of reproducibility:** There's no way to view the data as it existed at a certain point in the past. Say I want to make a backwards-compatible change to my code: I'd like to validate this by running the new code on last week's data while making sure I'm getting last week's output. However, since last week others may have made changes to that input. If I get a different result - how can I tell if I broke something, or the data has changed?


## Concepts

To solve these problems, lakeFS exposes an API that is compatible with S3 (with the data itself being stored back to S3), that introduces Git-like semantics on top: commits, branches, merges, etc. So:


1. **Isolation is achieved using branches:** I can create a branch derived from "master", run my jobs on it, experiment, and only once done, I can commit my change and atomically merge it into master. All my changes are applied as one atomic piece, meaning no-one sees any iterminnent state. You can think of it as kind of a very long-lived database transaction.

   

2. **reverting a change (whether it was committed or not) is also an atomic operation.** You can revert uncommitted changes but also point a branch to an older commit back in time. You can even cherry-pick changes (i.e. only revert the stuff I've done under /some/path).

   

3. **Control through CI:** If my changes are made in a branch, I can run automated scripts or manual reviews before merging to master. I can easily detect schema changes, bad output, wrong paths, etc., before they cause a production issue for someone else.

   

4. **Reproducibility:** Different users can simultaneously look at different branches and commits. If I tag a data commit with the commit hash of the job that created it, I can always go back in time and get the exact snapshot of the data that existed along with the code that created it.


## Where does it fit in?

lakeFS sits between the data itself (i.e. an S3 bucket) and the applications and users that consume the data. Think of it as S3 on top of S3:

![lakeFS](assets/img/wrapper.png)

   
## Branching Model

At its core, lakeFS uses a Git-like branching model. The data model contains the following types:

### Repositories

In lakeFS, a repository is a logical namespace used to group together objects, branches and commits. It is the equivalent of a Bucket in S3, and a repositoriy in Git.

### Branches

Branches are similar in concept to [Git branches](https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging){:target="_blank"}.  
When creating a new branch in lakeFS, we are actually creating a consistent snapshot of the entire repository, which is isolated from other branches and their changes.  
Another way to think of branches is like a very long-lived database transaction, providing us with [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation){: target="_blank" }.

Once we've made the necessary changes to our data within our isolated branch, we can merge it back to the branch we branched from.  
This operation is atomic in lakeFS - readers will either see all our committed changes or non at all.

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
Addressing the object in the underlying object store is done using a dedupe ID - objects with the same content will receive the same ID, thus stored only once.

## Next steps

Run lakeFS locally and see how it works for yourself!

Check out the [Quick Start Guide](quickstart.md)

