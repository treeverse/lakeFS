---
layout: default
title: lakeFS Glossary
description: Glossary of all terms related to lakeFS technical internals and the architecture
nav_order: 52
has_children: false
---

# Glossary
This page has definition and explanations of all terms related to lakeFS technical internals and the architecture.

{% include toc.html %}

### Auditing
Data auditing is data assessment to ensure its accuracy, security, and efficacy for specific usage. It also involves assessing data quality through its lifecycle and understanding the impact of poor quality data on the organization's performance and revenue. Ensuring data reproducibility, auditability, and governance is one of the key concerns of data engineers today. lakeFS commit history helps the data teams to keep track of all changes to the data, supporting data auditing.

### Branch

Branches are similar in concept to Git branches.
When creating a new branch, we are actually creating a consistent snapshot of the entire repository, which is isolated from other branches and their changes.

A branch is a mutable pointer to a commit and uncommitted changes in its staging area (i.e., mutable storage where users can create, update, and delete objects). When a user creates a commit from a branch, all the files from the staging area will be merged into the contents of the current branch, generating a new set of objects. The pointer is updated to reference the new set of objects. The new branch tip is set to the latest commit and the previous branch tip serves as the commit's parent. 

Just like in git, a branch spans a repository. Learn more about the [lakeFS branching model](./understand/branching-model.md).

### Collection
A collection, roughly speaking, is a set of data. Collections may be structured or unstructured; a structured collection is often referred to as a table.

### Commit
A commit is a point-in-time immutable snapshot of a branch. It's a collection of object metadata and data, including paths and the object contents and metadata. Commits have their own commit metadata. Every repository has one initial commit with no parent commits. If a commit has more than one parent, it is a merge commit. lakeFS supports only merge commits with two parents.

### Cross-collection Consistency
It is unfortunate that the word 'consistency' has multiple meanings, at least four of them according to Martin Kleppmann. Consistency in the context of lakeFS and data versioning is, the guarantee that operations in a transaction are performed accurately, correctly and most important, atomically. 

A repository (and thus a branch) in lakeFS, can span multiple tables or collections. By providing branch, commit, merge and revert operations atomically on a branch, lakeFS achieves consistency guarantees across different logical collections. That is, data versioning is consistent across multiple collections within a repository.

It is sometimes referred as multi-table transactions. That is, lakeFS offers transactional guarantees across multiple tables.

<!---Learn more about cross-collection consistency here (link to CCC blog) -->

### Data Lifecycle Management
In data-intensive applications, data should be managed through its entire lifecycle similar to how teams manage code. By doing so, we could leverage the best practices and tools from application lifecycle management (like CI/CD operations) and apply them to data. lakeFS offers data lifecycle management via [isolated data development environments](./use_cases/iso_env.md) instead of shared buckets.

### Data Pipeline Reproducibility
Reproducibility in data pipelines is the ability to repeat a process. An example of this is recreating an issue that occurred in the production pipeline. Reproducibility allows for the controlled manufacture of an error to debug and troubleshoot it at a later point in time. Reproducing a data pipeline issue is a challenge that most data engineers face on a daily basis. Learn more about how lakeFS supports data pipeline [reproducibility](./use_cases/reproducibility.md). Other use cases include running ad-hoc queries (useful for data science), review, and backfill.

### Data Quality Testing
This term describes ways to test data for its accuracy, completeness, consistency, timeliness, validity, and integrity. lakeFS hooks can be used to implement and run data quality tests before promoting staging data into production. 

### Data Versioning
To version data means creating a unique point-in-time reference for data that can be accessed later. This reference can take the form of a query, an ID, or also commonly, a DateTime identifier. Data versioning may also include saving an entire copy of the data under a new name or file path every time you want to create a version of it. More advanced versioning solutions like lakeFS perform versioning through zero-copy data operations. lakeFS also optimizes storage usage between versions and exposes special operations to manage them.

### Git-like Operations
lakeFS allows teams to treat their data lake as a Git repository.   Git is used for code versioning, whereas lakeFS is used for data versioning.  lakeFS provides Git-like operations such as branch, commit, merge and revert.

### Graveler
Graveler is the core versioning engine of lakeFS. It handles versioning by translating lakeFS addresses to the actual stored objects. See the [versioning internals section](./understand/versioning-internals.md) to learn how lakeFS stores metadata.

### Hooks
lakeFS hooks allow you to automate and ensure that a given set of checks and validations happens before important lifecycle events. They are similar conceptually to [Git Hooks](https://git-scm.com/docs/githooks), but in contrast, they run remotely on a server. Currently, lakeFS allows executing hooks when two types of events occur: pre-commit events that run before a commit is acknowledged and pre-merge events that trigger right before a merge operation. 

### Isolated Data Snapshot
Creating a branch in lakeFS provides an isolated environment containing a snapshot of your repository. While working on your branch in isolation, all other data users will be looking at the repository's main branch. So they won't see your changes, and you also won't see the changes applied to the main branch. All of this happens without any data duplication but metadata management.

### Main Branch
Every Git repository has the main branch (unless you take explicit steps to remove it) and it plays a key role in the software development process. In most projects, it represents the source of truth - all the code that works has been tested and is ready to be pushed to production. Similarly, main branch in lakeFS could be used as the single source of truth. For example, the live production data can be on the main branch. 

### Metadata Management
Where there is data, there is also metadata. lakeFS uses metadata to define schema, data types, data versions, relations to other datasets, etc. This helps to improve discoverability and manageability. lakeFS performs data versioning through metadata operations. 

### Merge
lakeFS merge command, similar to git merge functionality, allows you to merge data branches. Once you commit data, you can review it and then merge the committed data into the target branch. A merge generates a commit on the target branch with all your changes. lakeFS guarantees atomic merges that are fast, given they don’t involve copying data. 

### Repository
A repository is a collection of objects with common history tracking. lakeFS manages versions of the repository identified by their commits. 

### Rollback
A rollback is an atomic operation reversing the effects of a previous commit. If a developer introduces a new code version to production and discovers that it has a critical bug, they can simply roll back to the previous version. In lakeFS, a rollback is an atomic action that prevents the data consumers from receiving low-quality data until the issue is resolved. Learn more about how lakeFS supports the [rollback](./use_cases/rollback.md) operation.

### Storage Namespace
The storage namespace is a location in the underlying storage dedicated to a specific repository.
lakeFS uses it to store the repository's objects and some of its metadata.

### Underlying Storage
The underlying storage is a location in some object store where lakeFS keeps your objects and some metadata.

### Tag
A tag is an immutable pointer to a single commit. Tags have readable names. Because tags are commits, a repository can be read from any tag. Example tags:

- `v2.3` to mark a release 
- `dev:jane-before-v2.3-merge` to mark Jane’s private temporary point.

