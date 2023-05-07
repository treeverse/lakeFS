# Iceberg support: branching & isolation

## Problem Description

Iceberg stores metadata files that represent a snapshot of a given Iceberg table.
Those metadata files save the locataion of the data hard-coded.
This is problematic with lakeFS, since we cannot work on the same files with different branches.

For example when working on branch `main`, the metadata files will be written with the property `"location" : "s3a://example-repo/main/db/table1"`.
This means the data is located in `"s3a://example-repo/main/db/table1"`.
However, when branching out from main to `feature-branch`, the metadata files still point to the main branch.
With this we lose the capability of isolation.


## Goals

Enable isolation with iceberg table. Create branch from existing one, and writing the data and metadata to the relevant branch.
At first stage: support iceberg without being catalog agnostic - meaning work with only lakeFS catalog and not be compatible with other catalogs.
At second stage: being catalog agnostic. Users will be able to configure both lakeFS catalog and other catalog together.
- The catalogs that will be supported are Glue, Snoflake and Tabular.

## Non Goals

- Support merge operation.
- Enable lakeFS operations using a dedicated SQL syntax.


## Proposed Design

Implement lakeFS catalog:

The catalog stores current metadata pointer for Iceberg tables.
With lakeFS, this pointer needs to be versioned as well.
In order to prevent from adding versioning capabilities to existing catalogs, we'll extend a catalog that can utilize lakeFS to keep the pointer.
Therefor, we'll extend hadoop catalog to work with lakeFS
- Every operation of the catalog happens with the scope of a reference in lakeFS.
- Write location on metadata files to be relative: the repo and brach will not be written.
- The catalog will know to use the metadata files from the relevant references
  
Implement lakeFSIO:
FileIO is the primary interface between the core Iceberg library and underlying storage. In practice, it is just two operations: open for read and open for write.
We'll Extend hadoop fileIO to work with lakeFS, and to be compatible with lakeFS catalog. 


## Alternatives Considered

### Implementation of lakeFS FileIO alone
In order to enable the user to choose his preferred catalog, we considered implementing only lakeFS FileIO.
This attemp failed since, as mentioned above, it will require the catalog to be able to version metadata pointers.
This will be very hard and won't enable us to fail fast.


## Usage

Iceberg operations:
Users will need to configure lakeFS catalog, with the relevant hadoop configuration.
Every operation on iceberg table (writing, reading, compaction, etc.) will be performed on the table in lakeFS.
Meaning, the user will need to specify the full path of the table, containing the repo and reference in lakeFS.

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
- Migration: how to use move my existing tables to lakeFS?
- Iceberg diff: how to view table changes in lakeFS.
- Merge: how to allow merging between branches?
- Catalog agnosticity: how can we use the benefits of other catalogs with lakeFS?
