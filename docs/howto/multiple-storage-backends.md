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

3. **Distributed Data Consolidation**:
   * Eliminate data silos and enable seamless cross-cloud collaboration.
   * Ideal for AI/ML environments where datasets are distributed across multiple storage locations.
   
## How to configure multiple storage backends

To configure your lakeFS server to connect to multiple storage backends, define them under the `blockstores` section in 
your server configuration.

### Configuration Structure

```yaml
blockstores:
    signing:
      secret_key: "<random_secret>"  # Required. A cryptographically secure random string for encryption and HMAC signing in storage-related APIs.

    stores:
        - id: "<storage_id>" # Unique storage id 
          backward_compatible: <true|false>  # Optional. Set to `true` for upgrading from a single-store setup.
          description: "<description_of_storage>"  
          type: "<storage_type>"  # Supported types: s3, azure, gcs, local, etc.

          # Storage-specific configuration
          s3:  
              force_path_style: <true|false>  
              endpoint: "<storage_endpoint>"  
              discover_bucket_region: <true|false>  
              credentials:
                access_key_id: "<access_key>"
                secret_access_key: "<secret_key>"

          azure:  
              storage_account: "<account_name>"  
              storage_access_key: "<access_key>"

          gcs:  
              credentials_file: "<path_to_service_account.json>"
              bucket_project_id: "<project_id>"

          local:  
              path: "<local_storage_path>"
```  

### Configuration Parameters

- **`signing.secret_key`** – A required, cryptographically secure string used for encryption and signing storage-related API operations.
- **`stores`** – A list of configured storage backends, each defined by:
  - **`id`** – A unique identifier for the storage backend.
  - **`backward_compatible`** – Enables backward compatibility for upgrading from a single-store setup. Defaults to `false`.
  - **`description`** – A short description of the backend’s purpose.
  - **`type`** – The storage provider type (`s3`, `azure`, `gcs`, or `local`).
  - **Provider-specific settings** – Each backend type requires different parameters (see below).

#### Example Configurations

##### **S3-Compatible Backends (AWS S3, MinIO, Ceph, etc.)**

```yaml
- id: "minio-main"
  description: "MinIO backend for production data"
  type: "s3"
  s3:
      force_path_style: true
      endpoint: "http://minio-main.local"
      discover_bucket_region: false
      credentials:
        access_key_id: "<main_access_key>"
        secret_access_key: "<main_secret_key>"
```

##### **Azure Blob Storage**

```yaml
- id: "azure-prod"
  description: "Azure storage for analytics"
  type: "azure"
  azure:
      storage_account: "myaccount"
      storage_access_key: "<access_key>"
```

##### **Google Cloud Storage (GCS)**

```yaml
- id: "gcs-research"
  description: "Google Cloud Storage for research data"
  type: "gcs"
  gcs:
      credentials_file: "/path/to/service-account.json"
      bucket_project_id: "my-gcp-project"
```

##### **Local Storage**

```yaml
- id: "local-dev"
  description: "Local storage for development"
  type: "local"
  local:
      path: "/data/lakefs-storage"
```

---

### Upgrading from Single to Multi-Store
- Set `backward_compatible: true` for the existing store to ensure a smooth transition.
- When `backward_compatible` is enabled, repositories created before the upgrade default to the existing store and repos 
created without specifying a storage id default to it. 

### **Common Configuration Errors & Fixes**
| Issue | Cause | Solution |
|-------|-------|---------|
| Blockstore ID conflicts | Duplicate `id` values in `stores` | Ensure each storage backend has a unique ID |
| Missing `backward_compatible` | Upgrade from single to multi-store without setting the flag | Add `backward_compatible: true` for the existing storage |
| Unsupported configurations in OSS | Enterprise-only features used in OSS version | Verify feature availability in the [lakeFS Enterprise documentation]({% link enterprise/index.md %}) |


### Upgrading from single to multi-store
* 
* The backward_compatible flag
* default to it when storage id is empty


### Common Configuration Errors & Fixes

Example: Blockstore ID conflicts, missing backward_compatible, unsupported configs in OSS.

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
* Make sure configuration reference has the new configurations
* Understand the status storage combinations of non self-managed s3 compatible 
* local storage in intro - clarify 