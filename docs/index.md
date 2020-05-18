---
layout: default
title: What is lakeFS
nav_order: 0
has_children: false
---

# What is lakeFS

lakeFS is a Data Lake Management platform that enables ACID guarantees using Git-like operations on conventional Object Stores (i.e. S3).


## Key Features

* Branch & Commit model provides an intuitive way to isolate and manage changes to data 
* Instant and atomic rollback of changes
* Strongly consistent data operations, as opposed to S3's eventual consistency.
* Transactional nature creates a clear contract between writers and readers (i.e. if a partition exists, it's guaranteed to contain all the data it should contain)
* API Compatibility with S3 means it's (almost) a drop-in replacement.


## Where does it fit in?

lakeFS sits between the data itself (i.e. an S3 bucket) and the applications and users that consume the data. Think of it as S3 on top of S3:

![lakeFS](assets/img/wrapper.png)


## Why? What are Object Stores missing when used as a Data Lake?



1. **Isolation:** Object stores make it easy to ingest data into the lake, making it available to others. However, by doing so, they provide no isolation guarantees: readers can access data while it's being written; bulk move/rename operations mean readers will see intermittent state.

   

2. **Lack of rollback/revert:** By working at the single object level, mistakes become costly. If a retention job accidentally deletes a huge number of objects, reverting such a change is very hard: common object-level versioning makes this theoretically possible, but very costly in both tim and. It makes the feedback loop much longer since there's no real way to experiment with the data without fearing data loss or breaking changes for others.

   

3. **Hard to manage and control:** Teams managing large Data Lakes usually have some conventions and policies around the data they contain. For example, enforcing the use of a certain format, rules around breaking schema changes and automated tests for job output. The mutable nature of Data Lakes means you can run these tests after the data has already been written. 

   

4. **Lack of reproducibility:** There's no way to view the data as it existed at a certain point in the past. Say I want to make a backwards-compatible change to my code: I'd like to validate this by running the new code on last week's data while making sure I'm getting last week's output. However, since last week others may have made changes to that input. If I get a different result - how can I tell if I broke something, or the data has changed?


## Concepts

To solve these problems, lakeFS exposes an API that is compatible with S3 (with the data itself being stored back to S3), that introduces Git-like semantics on top: commits, branches, merges, etc. So:


1. **Isolation is achieved using branches:** I can create a branch derived from "master", run my jobs on it, experiment, and only once done, I can commit my change and atomically merge it into master. All my changes are applied as one atomic piece, meaning no-one sees any iterminnent state. You can think of it as kind of a very long-lived database transaction.

   

2. **reverting a change (whether it was committed or not) is also an atomic operation.** You can revert uncommitted changes but also point a branch to an older commit back in time.You can even cherry-pick changes (i.e. only revert the stuff I've done under /some/path).

   

3. **Control through CI:** If my changes are made in a branch, I can run automated scripts or manual reviews before merging to master. I can easily detect schema changes, bad output, wrong paths, etc., before they cause a production issue for someone else.

   

4. **Reproducibility:** Different users can simultaneously look at different branches and commits. If I tag a data commit with the commit hash of the job that created it, I can always go back in time and get the exact snapshot of the data that existed along with the code that created it.

   
