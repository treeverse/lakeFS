---
title: Multi-storage Backend Support
description: How to manage data across multiple storage systems with lakeFS 
parent: How-To
---

# Multi-storage Backend Support

lakeFS Enterprise
{: .label .label-purple }

{: .note}
> Multi-storage backend support is only available for licensed [lakeFS Enterprise]({% link enterprise/index.md %}) customers.<br>
> Contact us to get started!   

{% include toc.html %}

## What is Multi-storage Backend Support? 

lakeFS multi-storage backend support enables seamless data management across multiple storage systems — 
on-premises, across public clouds, or hybrid environments. This capability makes lakeFS a unified data management platform
for all organizational data assets, which is especially critical in AI/ML environments that rely on diverse datasets stored
in multiple locations.

With a multi-store setup, lakeFS can connect to and manage any combination of supported storage systems, including AWS S3,
Azure Blob, Google Cloud Storage, other S3-compatible storage, and even local storages. 

**ADD a diagram**

{: .note}
> Multi-storage backends support is available from version X of lakeFS Enterprise.

## Use Cases

1. **Unified Data Access and Versioning**:
  * Access data across multiple storage backends using a single, consistent [URI format](../understand/model.md#lakefs-protocol-uris).
  * Maintain version control across different storage providers for consistency and reproducibility.

2. **Centralized Access Control & Governance**:
   * Access permissions and policies can be centrally managed across all connected storage systems using lakeFS [RBAC](../security/rbac.md).
   * Compliance and security controls remain consistent, regardless of where the data is stored.

3. **Distributed Data Management**:
   * Eliminate data silos and enable seamless cross-cloud collaboration.
   * Ideal for AI/ML environments where datasets are distributed across multiple storage locations.
   
## Configuration

To configure your lakeFS server to connect to multiple storage backends, define them under the `blockstores` section in 
your server configurations. The `blockstores.stores` field is an array of storage backends, each with its own configuration.  

{: .warning}
> Multi-store configuration is incompatible with [single-store configuration](../reference/configuration.md/#blockstore), 
> which is used in lakeFS open-source and unlicensed Enterprise setups. Ensure that only one configuration type is used.

### Example Configuration

```yaml
blockstores:
  signing:
    secret_key: "some_secret"  # Required for encryption and HMAC signing
  stores:
    - id: "s3-prod"
      backward_compatible: true
      description: "AWS S3 storage for production data"
      type: "s3"
      s3:
        region: us-east-1
    - id: "minio-research"
      description: "MinIO storage for research data"
      type: "s3"
      s3:
        force_path_style: true
        endpoint: 'http://minio-raw-data.local'
        discover_bucket_region: false
        credentials:
          access_key_id: "minioadmin"
          secret_access_key: "minioadmin"
    - id: "azure-prod"
      description: "Azure Blob storage for analytics"
      type: "azure"
      azure:
        storage_account: "my-prod-account"
        storage_access_key: "EXAMPLE45551FSAsVVCXCF"
```  

This example configuration defines connect three storage backends to lakeFS:
* S3 (s3-prod) – Primary production storage, marked as backward compatible which means it was previously used in a single-store setup.
* MinIO (minio-research) – Used for research data, configured with explicit credentials.
* Azure Blob Storage (azure-prod) – Stores analytics data. 

#### Key Considerations

* Unique Blockstore IDs: Each storage backend must have a unique id.
* Persistence of Blockstore IDs: Once defined, an id must not change.
* S3 Authentication Handling:
    * All standard S3 authentication methods are supported.
    * If static credentials are provided, lakeFS will use them. Otherwise, it will fall back to the AWS credentials chain.
    * If multiple storages of type `s3` are used, static credentials are required for all but one.

### Upgrading from Single to Multi-Store

When upgrading from a single storage backend to a multi-store setup, follow these guidelines:
* Use the new `blockstores` structure, replacing the existing `blockstore` configuration.
* Define all previously available [single-blockstore settings](../reference/configuration.md#blockstore) under their respective storage backends.
* The `signing.secret_key` remains a required global setting.
* Set `backward_compatible: true` for the existing storage backend to ensure:
  * Existing repositories continue using the original storage backend.
  * Newly created repositories default to this backend unless explicitly assigned a different one, to ensure a non-breaking upgrade process. 

### Common Configuration Errors & Fixes

| Issue                                                               | Cause | Solution                                                 |
|---------------------------------------------------------------------|-------|----------------------------------------------------------|
| Blockstore ID conflicts                                             | Duplicate `id` values in `stores` | Ensure each storage backend has a unique ID              |
| Missing `backward_compatible`                                       | Upgrade from single to multi-store without setting the flag | Add `backward_compatible: true` for the existing storage |
| Unsupported configurations in OSS or unlicensed Enterprise accounts | Using multi-store features in an unsupported setup | Contact us to start using the feature                    |


## Creating repositories  

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
* Understand the status storage combinations of non self-managed s3 compatible 
* local storage in intro - clarify
* contact us - what main? add to cta in begining and in problems table 
* Consider calling the feature - Distributed data management

* Make sure configuration reference has the new configurations
  Configuration Parameters
- **`signing.secret_key`** – A required, cryptographically secure string used for encryption and signing storage-related API operations.
- **`stores`** – A list of configured storage backends, each defined by:
    - **`id`** – A unique identifier for the storage backend.
    - **`backward_compatible`** – Enables backward compatibility for upgrading from a single-store setup. Defaults to `false`.
    - **`description`** – A short description of the backend’s purpose.
    - **`type`** – The storage provider type (`s3`, `azure`, `gcs`, or `local`).
    - **Provider-specific settings** – Each backend type requires different parameters (see below).
