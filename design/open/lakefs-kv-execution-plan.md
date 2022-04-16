# lakeFS on KV - Execution Plan

Key/value data storage package (kv) that will supply key/value access to data to replace the current storage and locking done by Postgres.
High level overview of the interface described in https://github.com/treeverse/lakeFS/blob/ed55edc2c24b24ed4e6fcccad1ae95844d38b9ad/design/open/metadata_kv/index.md. The package will support multiple back-end implementations.

## Transform from using db to kv package

High level API described in the kv proposal, we should consider taking the DynamoDB API to address the way we communicate `set-if` functionality while marshal the same value without fetching data first.
Key/value format can be done using ProtoBuf / JSON - discussion and information will be part of a design document.
Each data currently stored using 'db' in a table will be migrated to a key value format. This is the first migration that will be supported on postgres. When all data is migrated from the current tables to kv, the migration will support data format changes inside the key/value.
Migrating from postgres to alternative implementation will be supported by dump/restore functionality after the first step of the migration is compelted and all the data is using the kv format.
Key will be based on the identity and lookup properties of the data.
Value will encode the data with additional version information to enable future data migration.

## Per package changes

For the following packages we should handle the move from 'db' package to 'kv'.
During development, allow an internal configuration flag to switch between using kv and db implementation.
Any move transition should support migration from old Postgres data to new Postgres data format.
When a complete data model / functionality completes the transition we can exclude the 'option' to swtich from/to and just use the new format.

- Map the use of transactions in order to provide a valid solution using the kv interface
- Map any database locking to specific solution using the kv implementation
- Map models from db to key/value key and value format (ProtoBuf?)
- Map secondary index used - ex: lookup user by id and email

    pkg/auth
        - metadata information: installation id, version and etc
        - authorization information: users, group, policy, credentials and etc
    pkg/actions
        - actions information: runs, hooks, status
    pkg/gateway/multiparts
        - tracking gateway multipart requests

    pkg/graveler/ref
        - crud: repository, branch, commits, tags (+log)
        - branch level locking
        - lock free commit
    pkg/graveler/retention
        - uses graveler/ref to iterate over branches and commit log
    pkg/graveler/staging
        - staged k/v storage: key/identity/value used to get/set/drop/drop by prefix/list

    pkg/api
        - calls migrate as part of setup lakefs
    pkg/catalog
        - initialize storage with db and lock db
    pkg/diagnostics
        - runs list of queries to collect information on the user's environment

## Global changes

- Transition from db to kv should include a configuration flag that enables the use of 'kv' vs the current 'db' implementation.
  As complete features move to use kv, and migration plan is implemented, we can remove the flag check for the feature and just enable the new functionality.
- Migrate from current DDL to kv will be supported only for Postgres and dropped when all features works using kv.
- Basic dump/restore functionality should be supported for the k/v implementation, how do we migrate the information - discovery

## Testing

- Migration - running the same tests on data that was generated with previous db/model
- Functionality performance - set of tests that will check that we didn't degrade
- Migration from old format performance - can't take 24h to switch to new version (downtime?)
- New locking mechanism functionality

## Milestone

- Implement adapter k/v for Postgres
- Feature flag (internal) used for kv development
- Each milestone will include migrate db that will use the new kv format. will support only the part we decided as feature complete and will no longer be controlled by the feature flag.
- For each step: implement the new models to k/v and migrate code
- Implement adapter k/v for other implementations
