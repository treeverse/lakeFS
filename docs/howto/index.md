---
title: How-To
description: How to perform various tasks in lakeFS
nav_order: 5
has_children: true
has_toc: true
---

# lakeFS - How To

<img src="/assets/img/docs_logo.png" alt="lakeFS Docs" width=200 style="float: right; margin: 0 0 10px 10px;"/>


## Installation and upgrades

* Step-by-step instructions for deploying and configuring lakeFS on [AWS](/howto/deploy/aws.html), [GCP](/howto/deploy/gcp.html), [Azure](/howto/deploy/azure.html), and [on-premises](/howto/deploy/onprem.html). 

* Details on [how to upgrade lakeFS](/howto/deploy/upgrade.html)

## Getting data in and out of lakeFS

* [Import](/howto/import.html) and [Export Data](/howto/export.html) from lakeFS
* [Copy data](/howto/copying.html) to/from lakeFS
* [Migrating](/howto/migrate-away.html) away from lakeFS

## Actions and Hooks in lakeFS

* Use [Actions and Hooks](/howto/hooks/) as part of your workflow to validate data, enforce constraints, and do more when events occur.

## Branch Protection

* [Branch Protection](/howto/protect-branches.html) prevents commits directly to a branch. This is a good way to enforce good practice and make sure that changes to important branches are only done by a merge.

## lakeFS Sizing Guide

* This [comprehensive guide](/howto/sizing-guide.html) details all you need to know to correctly size and test your lakeFS deployment for production use at scale, including: 

    * [System Requirements](/howto/sizing-guide.html#system-requirements)
    * [Scaling factors](/howto/sizing-guide.html#scaling-factors)
    * [Benchmarks](/howto/sizing-guide.html#benchmarks)
    * [Important metrics](/howto/sizing-guide.html#important-metrics)
    * [Reference architectures](/howto/sizing-guide.html#reference-architectures)

## Garbage Collection

* lakeFS will keep all of your objects forever, unless you tell it otherwise. Use [Garbage Collection](/howto/garbage-collection/) (GC) to remove objects from the underlying storage. This can be useful in situations including cost reduction (use less storage) and privacy policies.
