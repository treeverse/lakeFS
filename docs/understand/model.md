---
layout: default
title: Model 
description: The lakeFS object model blends the object models of Git and of object stores such as S3. Read this page to learn more.
parent: Understanding lakeFS
nav_order: 20
has_children: false
redirect_from: ["../reference/object-model.html","../understand/branching-model.html","../understand/object-model.html"]
---

# Model
{: .no_toc }

{% include toc.html %}

lakeFS blends concepts from object stores such as S3 with concepts from Git. This reference
defines the common concepts of lakeFS.

## Objects

lakeFS is an object store and borrows concepts from S3.

An _object store_ links objects to paths. An _object_ holds:

* Some _contents_, with unlimited size and format.
* Some _metadata_, including
  + _size_ in bytes,
  + the _creation time_, a timestamp with seconds resolution,
  + a _checksum_ string which uniquely identifies the contents,
  + some _user metadata_, a small map of strings to strings.

Similarly to many object stores, lakeFS objects are immutable and never rewritten. They can
be entirely replaced or deleted, but not modified.

The actual data itself is not stored inside lakeFS directly but in an [underlying object store](#concepts-unique-to-lakefs).
lakeFS will manage these writes and store a pointer to the object in its metadata database.
{: .note }

## Version Control

lakeFS borrows its concepts for version control from Git.

### Repository

In lakeFS, a _repository_ is a logical namespace used to group together objects, branches, and commits.
It can be considered the lakeFS analog of a bucket in an object store. Since it has version control qualities, it's also analogous to a repository in Git.

### Commits
Commits are immutable "checkpoints" containing an entire snapshot of a repository at a given point in time.
This is very similar to commits in Git. Each commit contains metadata - the committer, timestamp, a commit message, as well as arbitrary key/value pairs you can choose to add.
Using commits, you can view your Data Lake at a certain point in its history and you're guaranteed that the data you see is exactly is it was at the point of committing it.

Every repository has exactly one _initial commit_ with no parents. A commit with more than one parent is
a _merge commit_. Currently lakeFS only supports merge commits with two parents.

#### Identifying commits
{: .no_toc }

A commit is identified by its _commit ID_, a digest of all contents of the commit. Commit IDs are by nature long,
so you may use a unique prefix to abbreviate them. A commit may also be identified by using a textual definition,
called a _ref_. Examples of refs include tags, branch names, and expressions.

### Branches
_Branches_ are similar in concept to [Git branches](https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging){:target="_blank"}.
It is a mutable pointer to a commit and its staging area (see [below](#staging-area)). A branch in lakeFS is a consistent snapshot of the entire repository,
which is isolated from other branches and their changes.

Example branches:

* `main`, the trunk.
* `staging`, maybe ahead of `main`.
* `dev:joe-bugfix-1234` for Joe to fix issue 1234.

### Staging Area

The _staging area_ is where your changes appear before they are committed.
Unlike Git, every branch in lakeFS has its own staging area.
Uncommitted changes are visible to all users with read permissions, for example using the _Uncommitted Changes_ tab in the UI.

When you commit these changes, a new commit is added to the commit history, and the changes disappear from the staging area.
This operation is atomic: readers will either see all your committed changes or none at all.


### Tags

A _tag_ is an immutable pointer to a single commit. Tags have readable names. Since tags
are commits, a repository can be read from any tag. Example tags:

* `v2.3` to mark a release.
* `dev:jane-before-v2.3-merge` to mark Jane's private temporary point.


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

### History

The _history_ of the branch is the list of commits from the branch tip through the first
parent of each commit. Histories go back in time.

### Merge

_Merging_ is the way to integrate changes from a branch into another branch.
The result of a merge is a new commit, with the destination as the first parent and the source as the second.

Unlike Git, lakeFS doesn't apply a diff algorithm while merging.
This is because lakeFS is used for unstructured data, where it makes little sense to merge multiple changes into a single object.
{: .note }

#### How it works
{: .no_toc }

To merge a _merge source_ (a commit) into a _merge destination_ (another commit), lakeFS first
finds the [merge base](https://git-scm.com/docs/git-merge-base#_description) the nearest common parent of the two commits.
It can now perform a _three-way merge_, by examining the presence and identity of files in each commit. In the table
below, "A", "B" and "C" are possible file contents, "X" is a missing file, and "conflict"
(which only appears as a result) is a merge failure.

| **In base** | **In source** | **In destination** | **Result** | **Comment**                                    |
| :---:       | :---:         | :---:              | :---:      | :---                                           |
| A           | A             | A                  | A          | Unchanged file                                 |
| A           | B             | B                  | B          | Files changed on both sides in same way        |
| A           | B             | C                  | conflict   | Files changed on both sides differently        |
| A           | A             | B                  | B          | File changed only on one branch                |
| A           | B             | A                  | B          | File changed only on one branch                |
| A           | X             | X                  | X          | Files deleted on both sides                    |
| A           | B             | X                  | conflict   | File changed on one side, deleted on the other |
| A           | X             | B                  | conflict   | File changed on one side, deleted on the other |
| A           | A             | X                  | X          | File deleted on one side                       |
| A           | X             | A                  | X          | File deleted on one side                       |

The API and lakectl allow passing an optional `strategy` flag with the following values: 
- dest-wins - in case of a conflict, merge will pick the destination object.
- source-wins - in case of a conflict, merge will pick the source object.
If the strategy is set, it will affect all the objects in the merge, there is currently no way to treat each conflict differently.

As a format-agnostic system, lakeFS currently merges by complete files. Format-specific and
other user-defined merge strategies for handling conflicts are on the roadmap.

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
