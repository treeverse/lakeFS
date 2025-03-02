---
title: Multi-storage Backend Support
description: How to manage data across multiple storage systems with lakeFS 
parent: How-To
---

# Multi-storage Backend Support

Note about license and a CTA to contact us.

Available from which lakeFS Enterprise version?

Validate on multiple s3-compatible storages but should work for other combinations. 

{% include toc.html %}

## Use Cases 

## Configuring Multiple Storage Backends

### Configuration format 

### Upgrading from single to multi-store
* The backward_compatible flag
* default to it when storage id is empty

### Common Configuration Errors & Fixes

Example: Blockstore ID conflicts, missing backward_compatible, unsupported configs in OSS.

## Managing repositories with multiple storage backends 

constraint: a single repo is associated with a single backend.

### Creating a Repository
Behavior for single vs. multi-blockstore installations.
API, CLI, and UI changes.

### Viewing Repository Details
How to check a repo’s storage backend in UI/API.

## Listing connected storages 

## Adding or removing a storage backend
We use the configurations for it. go change configurations and restart the server.

#### Add

#### Remove

* Before removing a connected storage, you need to make sure that you delete the repositories created on it. otherwise it will lead
  undefined behaviour.
  We use the configurations for it. go change configurations and restart the server.

