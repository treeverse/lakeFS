# Next Generation Metastore

### Overview

Deliver manageability of metastore information alongside the object-store data as part of lakeFS.

### Goals

- Metastore entities are versioned: using the same commit and branching abilities that lakeFS provides
- Metastore entities are versioned together with the data: an atomic merge operation should contain both data mutations as well as metastore mutations.
- Metastore entities are diff-able: can view the changes made to the metadata between commits
- Maintain some compatibility with existing HMS, or at least a narrow set of its versions and API
- Extend functionality to also support next-generation table formats in addition to hive-style tables (Notably, Iceberg and Delta Lake). The extent of this support is still TBD.

### Non-Goals

- Support storing metadata inside existing Hive Metastore implementations
- Provide any significant change in performance or throughput of metadata operations
- Provide compatibility with other specific versions of Hive or Hive Metastore


### Discovery

Metastore implementation provides a reference implementation we can test and explore. It still needs to be discovered how to extend functionality to enable versioning.

Finding out more about the following items will help us evaluate our design options:

1. Passing lakeFS's reference/branch information from the metastore client. Explore ways to pass repository/branch/ref information from the client to our metastore.
2. Side by side - very similar to the current work on side-by-side work with lakeFS and the current object-storage, the user like to run the same application with minimum changes, while enabling some of the data to live inside lakeFS. What needs to change or requirements in order to have the same in the metadata level? Do we need to sync information, manage part of the data and enable mapping on the client side? Can we replace the client side implementation? do we need to spin different endpoints to serve different repositories, branches in lakeFS for parts of the metadata? fallback to existing metastore?
3. Data model diff/merge - describe the level of entities we use for diff/merge. The operation we need to support in order to resolve conflict. How it effects our import/export path?

### Design Diagram

TBD