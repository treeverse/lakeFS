# Next Generation Metastore

### Overview

Deliver manageability of metastore information alongside the object-store data as part of lakeFS.

### Goals

- Metastore entities are versioned: using the same commit and branching abilities that lakeFS provides
- Metastore entities are versioned together with the data: an atomic merge operation should contain both data mutations as well as metastore mutations.
- Metastore entities are diff-able: can view the changes made to the metadata between commits
- Maintain some compatibility with existing HMS, or at least a narrow set of its versions and API
- Extend functionality to also support next-generation table formats in addition to hive-style tables (Notably, Iceberg and Delta Lake) - long term goal.


### Non-Goals

- Support storing metadata inside existing Hive Metastore implementations
- Provide any significant change in performance or throughput of metadata operations
- Provide compatibility with other specific versions of Hive or Hive Metastore


### Design Diagram

[lakeFS Metastore](diagrams/metastore.png)

In this diagram:
1. Spark/Hive/Trino - data and metadata clients
1. lakeFS Metastore Thrift Proxy - act as the client's metastore by implementing the Hive Metastore Thrift protocol
1. lakeFS - Include the new following components:
   - Additional operations on OpenAPI that will 
   - Metadata catalog that will wrap Graveler to serve metadata like we do for the object store.

The diagram show how Spark/Hive/Trino connects to lakeFS's metastore proxy (which acts as Hive Metastore).
The request to the metastore proxy will invoke the equivalent operation on lakeFS's OpenAPI.
Inside lakeFS API implementation will use the metastore catalog, that is implemented by Graveler as the storage engine.


### Data Model

Using Gravler we can implement a Metastore catalog for all the metadata properties.
Using the same ordered key-value exposed by Graveler we can model the entities used by Hive Metastore:

- Databases
- Tables
- Partitions
- Indices
- Functions
- Columns (TODO: how do we store column statistics?)
- Constraints
- Serdes

Mapping the above model into our key/value and compare/merge strategy by a table level can be implemented as first step. Compare and merge changes in database/table level, which means that any changes under the table will consider a table modification and the user will handle to resolve a conflcit in case two diffrent nested information would have changed.
One of the discover items is to select the best way we can model the information in order to have a better way to diff/merge changes in metastore data.  The key format we will use will effect how the underlaying ranges will be split, the cost of lookup of nested information and etc.
In order to keep a reference to a state of the above model, the data we will keep depends on they we store.
For example in case we like to store database information in our key/value storage:

```
db/db1 -> {name, description, locationUri, parameters, privileges, ownerName, ownerType}
db/db2 -> {name, description, locationUri, parameters, privileges, ownerName, ownerType}
...
```

Different keys holds different value format

```
db/db1/table/tbl1 -> {tableName,dbName,owner,createTime,lastAccessTime,retention,sd,partitionKeys,...}
db/db1/table/tbl2 -> {tableName,dbName,owner,createTime,lastAccessTime,retention,sd,partitionKeys,...}
db/db2/table/tbl1 -> {tableName,dbName,owner,createTime,lastAccessTime,retention,sd,partitionKeys,...}
...
```

The data model that Hive Metastore provides enables more than the above. We will implement the missing entities as required by the data tools.


### Thrift API

Implementing the Hive Metastore 2.3.x will be the starting point of lakeFS's Metastore.
Spark works with Hive Metastore version 2.3.9 (https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html), but can also work with version 3.x.
Trino can work with Hive 2 and 3, while having specific capabilities enabled for version 3.
Must of the Hive Metastore Thrift interface kept backward compatibility v2.3.x and v3.x interfaces are almost identical, where version 3 adds Catalog and Workload management metadata.

Code generation should be considered. Having generated bride that will transfer each Thrift API call to our OpenAPI. And/or generate code for each entity described in the Thrift interface can reduce the code in order serialize, map and filter the data model.

Thrift API communicate over a socket, the first step will enable non-secure communication over an additional port. A secure communication can be handled by a external TCP load-balancer first, later the proxy can supply this capability out of the box.


Hooks - The Thrift interface include a set of API ends with '_environment_context', which enables the caller passing additional information to the hooks. The hooks mechanism in Hive Metastore is a Java interface implementation that the metastore load and calls on specific operations. We will not support this mechanism at first, but we can have a reference implementation of what to expect in case of adding it to our hooks mechanism.


### Discovery

Metastore implementation provides a reference implementation we can test and explore. It still needs to be discovered how to extend functionality to enable versioning.

Finding out more about the following items will help us evaluate our design options:

1. Passing lakeFS's reference/branch information from the metastore client. Explore ways to pass repository/branch/ref information from the client to our metastore.
1. Side by side - very similar to the current work on side-by-side work with lakeFS and the current object-storage, the user like to run the same application with minimum changes, while enabling some of the data to live inside lakeFS. What needs to change or requirements in order to have the same in the metadata level? Do we need to sync information, manage part of the data and enable mapping on the client side? Can we replace the client side implementation? do we need to spin different endpoints to serve different repositories, branches in lakeFS for parts of the metadata? fallback to existing metastore?
1. Data model diff/merge - describe the level of entities we use for diff/merge. The operation we need to support in order to resolve conflict. How it effects our import/export path?
1. Authentication with lakeFS - currently lakeFS secure all access by authenticating each request. What will be with metadata?
1. Data model - best way to model the metadata information into Graveler to enable 3-way diff / merge that will work in optimal way. Enable diff and merge with context, for example, when a column is added we need to know on which table and merge can identify two new partitions to the same table without a conflict.

