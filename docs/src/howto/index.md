---
title: How-To
description: How to perform various tasks in lakeFS
---

# lakeFS - How To

<img src="../assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>

## Installation and upgrades

* Step-by-step instructions for deploying and configuring lakeFS on [AWS](deploy/aws.md), [GCP](deploy/gcp.md), [Azure](deploy/azure.md), and [on-premises](deploy/onprem.md).

* Details on [how to upgrade lakeFS](deploy/upgrade.md)

## Getting data in and out of lakeFS

* [Import](import.md) and [Export Data](export.md) from lakeFS
* [Copy data](copying.md) to/from lakeFS
* [Using external Data Catalogs](catalog_exports.md) with data stored on lakeFS
* [Migrating](migrate-away.md) away from lakeFS
* Working with lakeFS data [locally](local-checkouts.md)

## Actions and Hooks in lakeFS

* Use [Actions and Hooks](hooks/index.md) as part of your workflow to validate data, enforce constraints, and do more when events occur.

## Branch Protection

* [Branch Protection](protect-branches.md) prevents commits directly to a branch. This is a good way to enforce good practice and make sure that changes to important branches are only done by a merge.

## Pull Requests

* Improve collaboration over data with [Pull Requests](pull-requests.md).

## lakeFS Sizing Guide

* This [comprehensive guide](sizing-guide.md) details all you need to know to correctly size and test your lakeFS deployment for production use at scale, including:

  * [System Requirements](sizing-guide.md#system-requirements)
  * [Scaling factors](sizing-guide.md#scaling-factors)
  * [Benchmarks](sizing-guide.md#benchmarks)
  * [Important metrics](sizing-guide.md#important-metrics)
  * [Reference architectures](sizing-guide.md#reference-architectures)

## Garbage Collection

* lakeFS will keep all of your objects forever, unless you tell it otherwise. Use [Garbage Collection](garbage-collection/gc.md) (GC) to remove objects from the underlying storage.
    If you want GC to happen automatically then you can use [Managed Garbage Collection](garbage-collection/managed-gc.md) which is available as part of lakeFS Cloud.

## Private Link

* [Private Link](private-link.md) enables lakeFS Cloud to interact with your infrastructure using private networking.

## Unity Delta Sharing

* lakeFS [Unity Delta Sharing](unity-delta-sharing.md) provides a read-only experience from Unity Catalog for lakeFS customers.
