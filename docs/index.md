---
layout: default
title: What is lakeFS
nav_order: 0
has_children: false
---

# What is lakeFS

lakeFS is a Data Lake Management platform that enables ACID guarantees using Git-like operations on conventional Object Stores (i.e. S3).

### Why? What are Object Stores missing when used as a Data Lake?



1. **Isolation:** Object stores make it easy to ingest data into the lake, making it available to others. However, by doing so, they provide no isolation guarantees: readers can access data while it's being written; bulk move/rename operations mean readers will see intermittent state.

   

2. **Lack of rollback/revert:** By working at the single object level, mistakes become costly. If a retention job accidentally deletes a huge number of objects, reverting such a change is very hard: common object-level versioning makes this theoretically possible, but very costly in both tim and. It makes the feedback loop much longer since there's no real way to experiment with the data without fearing data loss or breaking changes for others.

   

3. **Hard to manage and control:** Teams managing large Data Lakes usually have some conventions and policies around the data they contain. For example, enforcing the use of a certain format, rules around breaking schema changes and automated tests for job output. The mutable nature of Data Lakes means you can run these tests after the data has already been written. 

   

4. **Lack of reproducibility:** There's no way to view the data as it existed at a certain point in the past. Say I want to make a backwards compatible change to my code: I'd like to validate this by running the new code on last week's data while making sure I'm getting last week's output. But since last week others may have made changes to that input. If I get a different result how can I tell if I broke something or the data has changed?



### Concepts

### Key Features

