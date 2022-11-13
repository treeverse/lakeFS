---
layout: default
title: Performance Design Patterns
parent: Understanding lakeFS
description: This section suggests performance best practices to work with lakeFS.
nav_order: 26
has_children: false
--- 
# Performance Design Patterns
{: .no_toc }

{% include toc.html %}

## Overview

When designing applications to access objects and versioning with LakeFS, use our best practices design patterns for achieving the best performance for your application.

## Uncommitted Objects
Commit is linear to the size of the change. As such, it is preferred to not have a lot of uncommitted data on a branch. The more objects in the commit, the commit will be larger and take more time to complete.
Even so, a commit should represent a logic point in time, or an application change to mark so it should have a meaning, a too small commit for example every one file will create unused meta ranges.
The commit size should not be on the edge, smaller or bigger.

## Isolation on the branch level
Having multiple commits on the same branch currently may result in a race on a branch and may need a retry if one will fail.
Same for merging multiple branches to the main branch at once. Therefore, it is suggested to have a single committer on a branch.

## Import data to lakeFS
When importing data to lakefs from external sources one time or regularly. Instead of copying it to the bucket you can use lakeFS import/ingest which will create a reference to the existing objects on your bucket and avoid the copy.

## Access committed data
Accessing committed data with the branch name: `lakefs://repo/main`
will get the latest commit from the main branch. However, it will also try to fetch the uncommitted data from main and will perform more calls to the kv store.
If needed to access only the committed data, use the commit id or tag id if exist: `lakefs://repo/commit-id-1`
It will use only the commit layer and saves calls to the DB, allowing more fast access when listing.
Uncommitted data is saved on the KV DB store and committed data on the object store, therefore it is preferred to work on committed data.

## Direct upload
Writing objects to the object store can be done using the API, lakectl, and the s3 gateway.
Lakectl has the option to upload objects [directly](https://docs.lakefs.io/reference/commands.html#lakectl-fs-upload) to the object store (requires proper credentials).
It is the faster option to upload an object with lakeFS, which will save latency from the gateway to s3.
It will upload the data to the object store and then stage only the metadata to lakeFS.

## Zero-copy
lakeFS provides a zero-copy mechanism to data. Therefore, instead of copying the data, we can check out to a new branch. It will lower the storage cost and minimize expensive operations.





