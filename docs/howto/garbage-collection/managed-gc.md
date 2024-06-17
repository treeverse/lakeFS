---
title: Managed Garbage Collection
description: Reduce the operational overhead of running garbage collection manually.
parent: Garbage Collection
nav_order: 5
grand_parent: How-To
redirect_from:
  - /cloud/managed-gc.html
---

# Managed Garbage Collection
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

{: .note}
> Managed GC is only available for [lakeFS Cloud]({% link cloud/index.md %}). If you are using the self-managed lakeFS, garbage collection is [available to run manually]({% link howto/garbage-collection/index.md %}).

## Benefits of using managed GC
* The quick and safe way to delete your unnecessary objects
* No operational overhead
* SLA for when your objects are deleted
* Support from the Treeverse team

## How it works
Similarly to the self-managed lakeFS, managed GC uses [garbage collection rules]({% link howto/garbage-collection/index.md %}) to determine which objects to delete.
However, it uses our super-fast and efficient engine to detect stale objects and branches (depends on your configuration) and prioritize them for deletion.

## Setting up
Enable managed GC through the lakeFS Cloud onboarding setup wizard.
This will create additional cloud resources for us to use and have access to delete those objects.
