---
title: Multi-storage Backend Support
description: How to manage data across multiple storage systems with lakeFS 
parent: How-To
---

# Multi-storage Backend Support

lakeFS Enterprise
{: .label .label-purple }

{: .note}
> Multi-storage backend support is only available for licensed [lakeFS Enterprise]({% link enterprise/index.md %}) customers.
> Contact us to get started!   

Multi-storage backend support in lakeFS Enterprise enables seamless data management across multiple storage systems — 
on-premises, across public clouds, or in hybrid environments. This feature makes lakeFS a unified data management platform
for all organizational data assets, which is especially critical in AI/ML workflows that rely on diverse datasets stored
in multiple locations.

With a multi-store setup, lakeFS can connect to and manage any combination of supported storage systems, including AWS S3,
Azure Blob, Google Cloud Storage, other S3-compatible storage, and even local storage. This enables **unified data access** 
using the [lakefs protocol URI](../understand/model.md#lakefs-protocol-uris) while maintaining **centralized access control**
and governance across all backends.

ADD a diagram

Multi-storage backends support is available from version X of lakeFS Enterprise.

{% include toc.html %}

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


TODO: 
* add to https://docs.lakefs.io/understand/architecture.html#object-storage for msb discoverability 