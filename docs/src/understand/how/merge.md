---
title: Merge
description: Using lakeFS, you can merge different commits and references into a branch. The purpose of this document is to explain how to use this feature.
---

# Merges in lakeFS

The merge operation in lakeFS is similar to Git. It incorporates changes from a _merge source_ (a commit/reference) into a _merge destination_ (a **branch**). 

## How does it work?

lakeFS first finds the [merge base](https://git-scm.com/docs/git-merge-base#_description): the nearest common ancestor of the two commits.
It can now perform a _three-way merge_, by examining the presence and identity of files in each commit. In the table
below, "A", "B" and "C" are possible file contents, "X" is a missing file, and "conflict"
(which only appears as a result) is a merge failure.

| **In base** | **In source** | **In destination** | **Result** | **Comment**                                    |
|:-----------:|:-------------:|:------------------:|:----------:|:-----------------------------------------------|
|      A      |       A       |         A          |     A      | Unchanged file                                 |
|      A      |       B       |         B          |     B      | Files changed on both sides in same way        |
|      A      |       B       |         C          |  conflict  | Files changed on both sides differently        |
|      A      |       A       |         B          |     B      | File changed only on one branch                |
|      A      |       B       |         A          |     B      | File changed only on one branch                |
|      A      |       X       |         X          |     X      | Files deleted on both sides                    |
|      A      |       B       |         X          |  conflict  | File changed on one side, deleted on the other |
|      A      |       X       |         B          |  conflict  | File changed on one side, deleted on the other |
|      A      |       A       |         X          |     X      | File deleted on one side                       |
|      A      |       X       |         A          |     X      | File deleted on one side                       |

## Merge Strategies

The [API](../../reference/api.md) and [`lakectl`][lakectl-merge] allow passing an optional `strategy` flag with the following values:

### `source-wins`

In case of a conflict, merge will pick the source objects.

!!! example
    ```bash
    lakectl merge lakefs://example-repo/validated-data lakefs://example-repo/production --strategy source-wins
    ```

When a merge conflict arises, the conflicting objects in the `validated-data` branch will be chosen to end up in `production`.

### `dest-wins`

In case of a conflict, merge will pick the destination objects.

!!! example
    ```bash
    lakectl merge lakefs://example-repo/validated-data lakefs://example-repo/production --strategy dest-wins
    ```

When a merge conflict arises, the conflicting objects in the `production` branch will be chosen to end up in `validated-data`. The `production` branch will not be affected by object changes from `validated-data` conflicting objects.

The strategy will affect all conflicting objects in the merge if it is set. Currently it is not possible to treat conflicts individually.

As a format-agnostic system, lakeFS currently merges by complete files. Format-specific and
other user-defined merge strategies for handling conflicts are on the roadmap.

## Async Merge (Enterprise) {#async-merge}

!!! info
    Available in **lakeFS Cloud** and **lakeFS Enterprise**

lakeFS Enterprise supports asynchronous merge operations for improved scalability.

### Overview

In lakeFS Enterprise, merge operations execute asynchronously:

1. The API returns immediately with a task ID
2. The merge executes in the background
3. Clients poll for completion status
4. Results (or errors) are available once the operation completes

### lakectl and UI behavior

When using lakeFS Enterprise:

- `lakectl merge` uses async merge by default
- The lakeFS UI uses async merge by default
- Both handle polling automatically—the experience is seamless

### API usage

For direct API access, use the experimental async endpoints:

- `POST /repositories/{repo}/refs/{source}/merge/{dest}/async` - Start async merge
- `GET /repositories/{repo}/refs/{source}/merge/{dest}/async/{id}/status` - Poll status

[lakectl-merge]:  ../../reference/cli.md#lakectl-merge
