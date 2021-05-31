---
layout: default
title: Object Model 
description: The lakeFS object model blends the object models of git and of object stores such as S3.  Here are the explicit definitions.
parent: Reference
nav_order: 70
has_children: false
---

# Commands (CLI) Reference
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

## Introduction

lakeFS blends concepts from object stores such as S3 with concepts from Git.  This reference
defines the common concepts of lakeFS.  Every concept appearing in _italic text_ is its
definition.

## lakeFS the object store

lakeFS is an object store, and borrows concepts from S3.

An _object store_ links objects to paths.  An _object_ holds:

* Some _contents_, with unlimited size and format.
* Some _metadata_, including
  + _size_ (in bytes)
  + _creation time_
  + a _checksum_ string which uniquely identifies the contents
  + some _user metadata_, a small map of strings to strings.

In lakeFS objects are immutable and never rewritten.

A _path_ is a readable string, typically decoded as UTF-8.  lakeFS maps paths to their objects
according to specific rules.  Importantly (and unlike many object stores), lakeFS may map
multiple paths to the same object on backing storage.  lakeFS paths use the `lakefs` protocol,
described below.

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
storing commits for the long term).  A commit may also be identified by using a tag or a
branch.  A repository can be read from any commit.

A _tag_ is an immutable pointer to a single commit.  Tags have readable names.  Because tags
are commits, a repository can be read from any tag.

A _branch_ is a mutable pointer to a commit and its staging area.  Repositories are readable
from any branch, but they are also **writable** to a branch.  The _staging area_ associated
with a branch is mutable storage where objects can be created, updated or deleted.  These
objects are readable when reading from the branch.  To **create** a commit from a branch, all
files from the staging area are merged into the contents of the current branch, creating the
new set of objects.  The parent of the commit is the previous branch tip, and the new branch
tip is set to this new commit.

The _history_ of the branch is the list of commits from the branch tip through the first
parent of each commit.  Histories go back in time.

The other way to create a commit is to merge an existing commit onto a branch.  To _merge_ a
source commit into a branch, lakeFS finds the closest common ancestor of that source commit
and the branch tip, called the "base".  Then it performs a 3-way merge.

## Concepts unique to lakeFS

_Underlying storage_ is the area on some other object store that lakeFS uses to store object
contents and some of its metadata.  We sometimes refer to underlying storage as _physical_.
The path used to store the contents of an object is then termed a _physical path_.

A lot of what lakeFS does is to manage how lakeFS paths translate to _physical paths_ on the
object store.  This mapping is generally **not** straightforward.

### `lakefs` protocol URIs

lakeFS uses a specific format for path URIs.  The URI `lakefs://<REPO>/<REF>/<KEY>` is a path
to objects in the given repo and ref under key.  This is used both for path prefixes and for
full paths.  In similar fashion, `lakefs://<REPO>/<REF>` identifies a ref, and
`lakefs://<REPO>` identifes a repo.
