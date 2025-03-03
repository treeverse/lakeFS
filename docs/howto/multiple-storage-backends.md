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

1. **Distributed Data Management**:
   * Eliminate data silos and enable seamless cross-cloud collaboration.
   * Maintain version control across different storage providers for consistency and reproducibility.
   * Ideal for AI/ML environments where datasets are distributed across multiple storage locations.

2. **Unified Data Access and Versioning**:
  * Access data across multiple storage backends using a single, consistent [URI format](../understand/model.md#lakefs-protocol-uris).

3. **Centralized Access Control & Governance**:
   * Access permissions and policies can be centrally managed across all connected storage systems using lakeFS [RBAC](../security/rbac.md).
   * Compliance and security controls remain consistent, regardless of where the data is stored.
   
## Configuration

To configure your lakeFS server to connect to multiple storage backends, define them under the `blockstores` section in 
your server configurations. The `blockstores.stores` field is an array of storage backends, each with its own configuration.  

{: .note}
> If you're upgrading from a single-store setup, refer to the [upgrade guidelines](#upgrading-from-single-to-multi-store)
> to ensure a smooth transition.

### Example Configurations

<div class="tabs">
  <ul>
    <li><a href="#on-prem">On-prem</a></li>
    <li><a href="#multi-cloud">Multi-cloud</a></li>
    <li><a href="#hybrid">Hybrid</a></li>
  </ul>
  
  <div markdown="1" id="on-prem">

This example setup configures lakeFS to manage data across two separate MinIO instances.

```yaml
blockstores:
    signing:
      secret_key: "some-secret"
    stores:
        - id: "minio-prod"
          description: "Primary on-prem MinIO storage for production data"
          type: "s3"
          s3:
            force_path_style: true
            endpoint: 'http://minio-prod.local'
            discover_bucket_region: false
            credentials:
              access_key_id: "prod_access_key"
              secret_access_key: "prod_secret_key"
        - id: "minio-backup"
          description: "Backup MinIO storage for disaster recovery"
          type: "s3"
          s3:
            force_path_style: true
            endpoint: 'http://minio-backup.local'
            discover_bucket_region: false
            credentials:
              access_key_id: "backup_access_key"
              secret_access_key: "backup_secret_key"
```

  </div>

  <div markdown="2" id="multi-cloud">

This example setup configures lakeFS to manage data across two public cloud providers: AWS and Azure.

```yaml
blockstores:
    signing:
      secret_key: "some-secret"
    stores:
        - id: "s3-prod"
          description: "AWS S3 storage for production data"
          type: "s3"
          s3:
            region: "us-east-1"
        - id: "azure-analytics"
          description: "Azure Blob storage for analytics data"
          type: "azure"
          azure:
            storage_account: "analytics-account"
            storage_access_key: "EXAMPLE45551FSAsVVCXCF"

```
  </div>

  <div markdown="3" id="hybrid">

This hybrid setup allows lakeFS to manage data across both cloud and on-prem storages.
```yaml
blockstores:
    signing:
      secret_key: "some-secret"
    stores:
        - id: "s3-archive"
          description: "AWS S3 storage for long-term archival"
          type: "s3"
          s3:
            region: "us-west-2"
        - id: "minio-fast-access"
          description: "On-prem MinIO for high-performance workloads"
          type: "s3"
          s3:
            force_path_style: true
            endpoint: 'http://minio.local'
            discover_bucket_region: false
            credentials:
              access_key_id: "minio_access_key"
              secret_access_key: "minio_secret_key"

```
  </div>
</div>

### Key Considerations

* Unique Blockstore IDs: Each storage backend must have a unique id.
* Persistence of Blockstore IDs: Once defined, an id must not change.
* S3 Authentication Handling:
    * All standard S3 authentication methods are supported.
    * If static credentials are provided, lakeFS will use them. Otherwise, it will fall back to the AWS credentials chain. 
      This means that for setups with multiple storages of type `s3`, static credentials are required for all but one.

### Upgrading from Single to Multi-Store

When upgrading from a single storage backend to a multi-store setup, follow these guidelines:
* Use the new `blockstores` structure, **replacing** the existing `blockstore` configuration. Note that `blockstore` and `blockstores` 
  configurations are mutually exclusive - lakeFS does not support both simultaneously. 
* Define all previously available [single-blockstore settings](../reference/configuration.md#blockstore) under their respective storage backends.
* The `signing.secret_key` remains a required global setting.
* Set `backward_compatible: true` for the existing storage backend to ensure:
  * Existing repositories continue using the original storage backend.
  * Newly created repositories default to this backend unless explicitly assigned a different one, to ensure a non-breaking upgrade process. 

### Adding or Removing a Storage Backend

To add a storage backend, update the server configuration with the new storage entry and restart the server.

To remove a storage backend:
* Delete all repositories associated with the storage backend.
* Remove the storage entry from the configuration.
* Restart the server.

{: .warning}
> Repositories linked to a removed storage backend will result in unexpected behavior. Ensure all necessary cleanup is done before removal.

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


TODO: 
* add to https://docs.lakefs.io/understand/architecture.html#object-storage for msb discoverability
* Understand the status storage combinations of non self-managed s3 compatible 
* local storage in intro - clarify
* contact us - what main? add to cta in begining and in problems table 
* Consider calling the feature - Distributed data management
* reference from enterprise docs page and other places?

* Make sure configuration reference has the new configurations
  Configuration Parameters
- **`signing.secret_key`** – A required, cryptographically secure string used for encryption and signing storage-related API operations.
- **`stores`** – A list of configured storage backends, each defined by:
    - **`id`** – A unique identifier for the storage backend.
    - **`backward_compatible`** – Enables backward compatibility for upgrading from a single-store setup. Defaults to `false`.
    - **`description`** – A short description of the backend’s purpose.
    - **`type`** – The storage provider type (`s3`, `azure`, `gcs`, or `local`).
    - **Provider-specific settings** – Each backend type requires different parameters (see below).
