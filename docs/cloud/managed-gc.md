---
layout: default
title: Managed Garbage Collection
description: Managed Garbage Collection is a managed garbage collection solution, maintained by Treeverse and operating within strict SLA, it reduces the operational overhead of maintaining a garbage collection manually.
parent: lakeFS Cloud
has_children: false
---

# Managed Garbage Collection
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

{: .note}
> Managed Garbage Collection is only available for [lakeFS Cloud](../cloud/). [Garbage collection](../howto/garbage-collection-index.md) is available to run manually when using self-managed lakeFS.

## The benefits of using managed GC onlakeFS cloud are:
* Advanced Engine to detect and delete objects quickly and safely
* Managed Solution
* Garbage Collection SLA
* Support

## How does it work?
lakeFS Cloud Managed Garbage Collection is using the same configuration of [garbage collection rules](../howto/garbage-collection-index.md) that are used in the self-managed version of lakeFS, but, we run it on our infrastructure, with a super-fast and efficient engine to detect stale objects and branches (depends on your configuration) and prioritize them for deletion.

## Setting up Managed Garbage Collection
lakeFS Cloud Onboarding Setup Wizard contains a toggle to enable Managed GC. This will create additional cloud resources for us to use and have access to delete those objects.