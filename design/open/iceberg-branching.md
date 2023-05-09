# Iceberg support: branching & isolation

## Problem Description

Iceberg stores metadata files that represent a snapshot of a given Iceberg table.
Those metadata files save the location of the data hard-coded.
This is problematic with lakeFS, since we cannot work on the same files with different branches.

For example when working on branch `main`, the metadata files will be written with the property `"location" : "s3a://example-repo/main/db/table1"`.
This means the data is located in `"s3a://example-repo/main/db/table1"`.
However, when branching out from main to `feature-branch`, the metadata files still point to the main branch.
With this we lose the capability of isolation.


## Goals

Enable working in isolation with an Iceberg table. Create a branch in lakeFS, and operate on the table on the branch.
At first stage: support iceberg without being catalog agnostic - meaning work with only lakeFS catalog and not be compatible with other catalogs.
At second stage: being catalog agnostic. Users will be able to configure both lakeFS catalog and other catalog together.
- We will start by supporting a limited set of catalogs, for example Glue, Snowflake and Tabular.

## Non Goals

- Support merge operation.
- Enable lakeFS operations using a dedicated SQL syntax.


## Proposed Design: Dedicated lakeFS catalog

The catalog stores current metadata pointer for Iceberg tables.
With lakeFS, this pointer needs to be versioned as well.
In order to prevent from adding versioning capabilities to existing catalogs, we'll extend a catalog that can utilize lakeFS to keep the pointer.
Therefore, we'll extend the Hadoop catalog to work with lakeFS
- Every operation of the catalog happens with the scope of a reference in lakeFS. This will be extracted from the table path.
- Write location on metadata files to be relative: the repo and brach will not be written.
- The catalog will know to use the metadata files from the relevant references
  
Implement LakeFSFileIO:
[FileIO](https://iceberg.apache.org/javadoc/master/org/apache/iceberg/io/FileIO.html) is the primary interface between the core Iceberg library and underlying storage (read more [here](https://tabular.io/blog/iceberg-fileio/#:~:text=FileIO%20is%20the%20primary%20interface,how%20straightforward%20the%20interface%20is.)). In practice, it is just two operations: open for read and open for write.
We'll Extend hadoop fileIO to work with lakeFS, and to be compatible with lakeFS catalog.
As with the catalog, the reference in lakeFS will be extracted from the table path.

This design will require users to only have a single writer per branch. This is because the Hadoop catalog uses a rename operation which is not atomic in lakeFS. 
We think it's a reasonable limitation to start with. We may be able to overcome this limitation in the future, using the `IfNoneMatch` flag in the lakeFS API.

## Alternatives Considered

### Implementation of lakeFS FileIO alone
In order to enable the users to choose their preferred catalog, we considered implementing only lakeFS FileIO.
As mentioned above, this will require the catalog to be able to version metadata pointers.
This will be very hard and won't enable us to fail fast.


## Usage

Iceberg operations:

The user will need to set the catalog implementation to be the lakeFS catalog. For example, if working in Spark:
`spark.sql.catalog.lakefs.catalog-impl=io.lakefs.iceberg.LakeFSCatalog`
The user will also need to configure a Hadoop FileSystem that can interact with objects on lakeFS, like the S3AFileSystem or LakeFSFileSystem.

Every operation on iceberg table (writing, reading, compaction, etc.) will be performed on the table in lakeFS.
The tables will be defined in iceberg. 
Meaning, the user will need to specify the full path of the table, containing the repo and any reference in lakeFS. 

For example:

`SELECT * FROM example-repo.main.table1;`


lakeFS:
lakeFS operations will stay as is.
Interacting with lakeFS will be through lakeFS clients, not through Iceberg.
Branch, diff and commit operations should work out of the box (since the locations in the metadata files are relative).
Diff will not show changes done to the table (like in delta diff).

## Packaging

We will publish this catalog to Maven central.
To start using Iceberg with lakeFS, the user will need to add the lakeFS catalog as a dependency.

## Open Questions
- Can this catalog become part of the Iceberg source code?
- Migration: how to move my existing tables to lakeFS?
- Iceberg diff: how to view table changes in lakeFS.
- Merge: how to allow merging between branches?
- Catalog agnosticity: how can we use the benefits of other catalogs with lakeFS?
- Add files operation to iceberg 
