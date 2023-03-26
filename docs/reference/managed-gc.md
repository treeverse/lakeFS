---
layout: default
title: Managed Garbage Collection
description: This section covers managed garbage collection for lakeFS Cloud.
parent: Reference
nav_order: 65
has_children: false
---


# Managed Garbage Collection
{: .d-inline-block }
Cloud
{: .label .label-green }


{: .note}
> Managed Garbage Collection is only available for [lakeFS Cloud](../cloud.md).
>
> Using the Open Source? Read more on [garbage-collection](../howto/garbage-collection-index.md). 

{% include toc.html %}

***What are the differences between lakeFS Garbage Collection and lakeFS Cloud Managed Garbage Collection?***
* Advanced Engine to detect and delete objects quickly and safely
* Managed Solution
* Garbage Collection SLA
* Support

## How does it work?
lakeFS Cloud Managed Garbage Collection is using the same configuration of [garbage collection rules](../howto/garbage-collection-index.md) that are used in OSS, but, we run it on our infrastructure, with a super-fast and efficient engine to detect stale objects and branches (depends on your configuration) and prioritize them for deletion.

## How do I get started?
lakeFS Cloud Onboarding Setup Wizard already contains a toggle to enable Managed GC, which will create additional cloud resources for us to use and have access to delete those objects, once this configuration is set, and the garbage collection rules are in place, you can sit back and relax, we'll do the rest.
