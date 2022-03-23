---
layout: default
title: Object Model 
description: The lakeFS object model blends the object models of git and of object stores such as S3.  Here are the explicit definitions.
parent: Understanding lakeFS
nav_order: 22
has_children: false
redirect_from: ../reference/object_model.html
---

# Object Model
{: .no_toc }

{% include toc.html %}

## Introduction

lakeFS blends concepts from object stores such as S3 with concepts from Git.  This reference
defines the common concepts of lakeFS.  Every concept appearing in _italic text_ is its
definition.

## lakeFS the object store

lakeFS is an object store, and borrows concepts from S3.

An _object store_ links objects to paths.  An _object_ holds:

* Some _contents_, with unlimited size and format.
* Some _metadata_, including
  + _size_ in bytes
  + the _creation time_, a timestamp with seconds resolution
  + a _checksum_ string which uniquely identifies the contents
  + some _user metadata_, a small map of strings to strings.

Similarly to many object stores, lakeFS objects are immutable and never rewritten.  They can
be entirely replaced or deleted, but not modified.

A _path_ is a readable string, typically decoded as UTF-8.  lakeFS maps paths to their objects
according to specific rules.  lakeFS paths use the `lakefs` protocol, described below.

## lakeFS the version control system

lakeFS borrows its concepts for version control from Git.

### Repository

A _repository_ is a collection of objects with common history tracking.  lakeFS manages
versions of the repository, identified by their commits.  A _commit_ is a collection of object
metadata and data, including especially all paths and the object contents and metadata at that
commit.  Commits have their own _commit metadata_, which includes a textual comment and
additional user metadata.

### Commits

Commits are organized into a _history_ using their parent commits.  Every repository has
exactly one _initial commit_ with no parents.  <span style="font-size: smaller">Note that a
Git repository may have multiple initial commits.</span> A commit with more than one parent is
a _merge commit_.  <span style="font-size: smaller">Currently lakeFS only supports merge
commits with two parents.</span>

### Identifying commits

A commit is identified by its _commit ID_, a digest of all contents of the commit.  Commit IDs
are by nature long, so a unique prefix may be used to abbreviate them (but note that short
prefixes can become non-unique as the repository grows, so prefer to avoid abbreviating when
storing commits for the long term).  A commit may also be identified by using a textual
definition, called a _ref_.  Examples of refs include tags, branches, and expressions.  The
state of the repository at any commit is always readable.

#### Tags

A _tag_ is an immutable pointer to a single commit.  Tags have readable names.  Because tags
are commits, a repository can be read from any tag.  Example tags:

* `v2.3` to mark a release
* `dev:jane-before-v2.3-merge` to mark Jane's private temporary point.

#### Branches

A _branch_ is a mutable pointer to a commit and its staging area.  Repositories are readable
from any branch, but they are also **writable** to a branch.  The _staging area_ associated
with a branch is mutable storage where objects can be created, updated or deleted.  These
objects are readable when reading from the branch.  To **create** a commit from a branch, all
files from the staging area are merged into the contents of the current branch, creating the
new set of objects.  The parent of the commit is the previous branch tip, and the new branch
tip is set to this new commit.  Example branches:

* `main`, the trunk
* `staging`, maybe ahead of `main`
* `dev:joe-bugfix-1234` for Joe to fix issue 1234.

#### Ref expressions

lakeFS also supports _expressions_ for creating a ref.  These are similar to [revisions in
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
parent of each commit.  Histories go back in time.

The other way to create a commit is to merge an existing commit onto a branch.  To _merge_ a
source commit into a branch, lakeFS finds the _best_ common ancestor of that source commit and
the branch tip, called the "base".  Then it performs a [3-way merge](#three-way-merge).  The
"best" ancestor is exactly that defined in the documentation for
[git-merge-base](https://git-scm.com/docs/git-merge-base#_description).  The result of a merge
is a new commit, with the destination as the first parent and the source as the second.  Thus
the previous tip of the merge destination is part of the history of the merged object.

### Three way merge

To merge a _merge source_ (a commit) into a _merge destination_ (another commit), lakeFS first
finds the _merge base_, the nearest common parent of the two commits.  It can now perform a
three-way merge, by examining the presence and identity of files in each commit.  In the table
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

As a format-agnostic system, lakeFS currently merges by complete files.  Format-specific and
other user-defined merge strategies for handling conflicts are on the roadmap.

## Concepts unique to lakeFS

_Underlying storage_ is the area on some other object store that lakeFS uses to store object
contents and some of its metadata.  We sometimes refer to underlying storage as _physical_.
The path used to store the contents of an object is then termed a _physical path_.  The object
itself on underlying storage is never modified, except to remove it entirely during some
cleanups.

When creating a lakeFS repository, you assign it with a _storage namespace_. The repository's
storage namespace is the prefix in the underlying storage where data for this repository
will be stored.

A lot of what lakeFS does is to manage how lakeFS paths translate to _physical paths_ on the
object store.  This mapping is generally **not** straightforward.  Importantly (and unlike
many object stores), lakeFS may map multiple paths to the same object on backing storage, and
always does this for objects that are unchanged across versions.

### `lakefs` protocol URIs

lakeFS uses a specific format for path URIs.  The URI `lakefs://<REPO>/<REF>/<KEY>` is a path
to objects in the given repo and ref expression under key.  This is used both for path
prefixes and for full paths.  In similar fashion, `lakefs://<REPO>/<REF>` identifies the
repository at a ref expression, and `lakefs://<REPO>` identifes a repo.
