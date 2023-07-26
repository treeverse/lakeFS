---
layout: default
title: Model 
description: The lakeFS object model blends the object models of Git and of object stores such as S3. Read this page to learn more.
parent: Understanding lakeFS
nav_order: 20
has_children: false
redirect_from: 
  - /reference/object-model.html
  - /understand/branching-model.html
  - /understand/object-model.html
---

# Model

{% include toc_2-3.html %}

lakeFS blends concepts from object stores such as S3 with concepts from Git. This reference
defines the common concepts of lakeFS.

## Objects

lakeFS is an interface to manage objects in an object store.

The actual data itself is not stored inside lakeFS directly but in an [underlying object store](#concepts-unique-to-lakefs).
lakeFS manages pointers and additional metadata about these objects. 
{: .note }

## Version Control

lakeFS is spearheading version control semantics for data. Most of these concepts will be familiar to Git users:

### Repository

In lakeFS, a _repository_ is a set of related objects (or collections of objects). In many cases, these represent tables of [various formats](https://lakefs.io/hudi-iceberg-and-delta-lake-data-lake-table-formats-compared/){:target="_blank"} for tabular data, semi-structured data such as JSON or log files - or a set of unstructured objects such as images, videos, sensor data, etc.

lakeFS represents repositories as a logical namespace used to group together objects, branches, and commits - analogous to a repository in Git.

lakeFS repository naming requirements are as follows: 

- Start with a lower case letter or number
- Contain only lower case letters, numbers and hyphens
- Be between 3 and 63 characters long

### Commits

Using commits, you can view a [repository](#repository) at a certain point in its history and you're guaranteed that the data you see is exactly as it was at the point of committing it.

These commits are immutable "checkpoints" containing all contents of a repository at a given point in the repository's history.

Each commit contains metadata - the committer, timestamp, a commit message, as well as arbitrary key/value pairs you can choose to add.


  **Identifying Commits**<br/><br/>
  A commit is identified by its _commit ID_, a digest of all contents of the commit. <br/>
  Commit IDs are by nature long, so you may use a unique prefix to abbreviate them. A commit may also be identified by using a textual definition, called a _ref_. <br/><br/>
  Examples of refs include tags, branch names, and expressions.
{: .note }


### Branches

Branches in lakeFS allow users to create their own "isolated" view of the repository.

Changes on one branch do not appear on other branches. Users can take changes from one branch and apply it to another by [merging](#merge) them.

Under the hood, branches are simply a pointer to a [commit](#commits) along with a set of uncommitted changes.


### Tags

Tags are a way to give a meaningful name to a specific commit. 
Using tags allow users to reference specific releases, experiments or versions by using a human friendly name.

Example tags:

* `v2.3` to mark a release.
* `dev:jane-before-v2.3-merge` to mark Jane's private temporary point.

### History

The _history_ of the branch is the list of commits from the branch tip through the first
parent of each commit. Histories go back in time.

### Merge

_Merging_ is the way to integrate changes from a branch into another branch.
The result of a merge is a new commit, with the destination as the first parent and the source as the second.

To learn more about how merging works in lakeFS, see the [merge reference](../understand/how/merge.md)
{: .note }


### Ref expressions

lakeFS also supports _expressions_ for creating a ref. These are similar to [revisions in
Git](https://git-scm.com/docs/gitrevisions#_specifying_revisions); indeed all `~` and `^`
examples at the end of that section will work unchanged in lakeFS.

* A branch or a tag are ref expressions.
* If `<ref>` is a ref expression, then:
  + `<ref>^` is a ref expression referring to its first parent.
  + `<ref>^N` is a ref expression referring to its N'th parent; in particular `<ref>^1` is the
    same as `<ref>^`.
  + `<ref>~` is a ref expression referring to its first parent; in particular `<ref>~` is the
    same as `<ref>^` and `<ref>~`.
  + `<ref>~N` is a ref expression referring to its N'th parent, always traversing to the first
    parent.  So `<ref>~N` is the same as `<ref>^^...^` with N consecutive carets `^`.


## Concepts unique to lakeFS
The _underlying storage_ is a location in an object store where lakeFS keeps your objects and some immutable metadata.

When creating a lakeFS repository, you assign it with a _storage namespace_. The repository's
storage namespace is a location in the underlying storage where data for this repository
will be stored.

We sometimes refer to underlying storage as _physical_. The path used to store the contents of an object is then termed a _physical path_.
Once lakeFS saves an object in the underlying storage it is never modified, except to remove it
entirely during some cleanups.

A lot of what lakeFS does is to manage how lakeFS paths translate to _physical paths_ on the
object store. This mapping is generally **not** straightforward. Importantly (and contrary to
many object stores), lakeFS may map multiple paths to the same object on backing storage, and
always does this for objects that are unchanged across versions.

### `lakefs` protocol URIs

lakeFS uses a specific format for path URIs. The URI `lakefs://<REPO>/<REF>/<KEY>` is a path
to objects in the given repo and ref expression under key. This is used both for path
prefixes and for full paths. In similar fashion, `lakefs://<REPO>/<REF>` identifies the
repository at a ref expression, and `lakefs://<REPO>` identifes a repo.
