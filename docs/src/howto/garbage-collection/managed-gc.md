---
title: Managed Garbage Collection
description: Reduce the operational overhead of running garbage collection manually.
status: enterprise
---

# Managed Garbage Collection

!!! info "lakeFS Cloud"

!!! note
    Managed GC is only available for [lakeFS Cloud](../../cloud/index.md). If you are using the self-managed lakeFS, garbage collection is [available to run manually](gc.md).

## Benefits of using managed GC

* The quick and safe way to delete your unnecessary objects
* No operational overhead
* SLA for when your objects are deleted
* Support from the Treeverse team

## How it works

Similarly to the self-managed lakeFS, managed GC uses [garbage collection rules](gc.md) to determine which objects to delete.
However, it uses our super-fast and efficient engine to detect stale objects and branches (depends on your configuration) and prioritize them for deletion.

## Setting up

Enable managed GC through the lakeFS Cloud onboarding setup wizard.
This will create additional cloud resources for us to use and have access to delete those objects.
