# lakeFS on KV - Execution Plan

Key/value data storage package (kv) that will supply key/value access to data to replace the current storage and locking done by Postgres.
High level overview of the interface described in https://github.com/treeverse/lakeFS/blob/ed55edc2c24b24ed4e6fcccad1ae95844d38b9ad/design/open/metadata_kv/index.md. The package will support multiple back-end implementations.

## Transform from using db to kv package

High level API described in the kv proposal, we should consider taking the DynamoDB API to address the way we communicate `set-if` functionality while marshal the same value without fetching data first.
Key/value format can be done using ProtoBuf / JSON - discussion and information will be part of a design document.
Each data currently stored using 'db' in a table will be migrated to a key value format. This is the first migration that will be supported on postgres. When all data is migrated from the current tables to kv, the migration will support data format changes inside the key/value.
Migrating from postgres to alternative implementation will not be part of this implementation as it will require to dump/restore of all data stored into the kv, not just ref store.
Key will be based on the identity and lookup properties of the data.
Value will encode the data with additional version information to enable future data migration.

## Per package changes

For the following packages we should handle the move from 'db' package to 'kv'.
During development, allow an internal configuration flag to switch between using kv and db implementation.
Upgrade from db to kv based version that is part of the milestone, we will include migration from old Postgres data to new Postgres data format.
When a feature is complete (using kv, dump/restore, migrate) as part of a milestone, can be only part of the model, we can exclude the feature flag and keep the new kv functionality without a way to go back.

The folloiwng steps will be required for each pacakge that uses the 'db' layer:

- Map the use of transactions to a kv solution
- Map database locking to the proposal kv solution using set-if
- Map table data to key/value
- Map secondary index if needed - ex: lookup user by id and email

    pkg/auth
        - metadata information: installation id, version and etc
        - authorization information: users, group, policy, credentials and etc
    pkg/actions
        - actions information: runs, hooks, status
    pkg/gateway/multipart
        - tracking gateway multipart requests

    pkg/graveler/ref
        - crud: repository, branch, commits, tags (+log)
        - branch level locking
        - lock free commit
    pkg/graveler/retention
        - uses graveler/ref to iterate over branches and commit log
    pkg/graveler/staging
        - staged k/v storage: key/identity/value used to get/set/drop/drop by prefix/list (as described in the kv proposal)

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
- Dump and restore functionality implementation as part of ref-dump.

## Testing

- Migration - upgrade from db to kv version and verify commit log, branch staging environment, authorization and actions data integrity.
- Functionality - verify that all data kept on kv works with the current functionality (CRUD).
- Ref-store dump/restore state test that old data is available and not corrupted.
- Functionality performance - set of tests that will check that we didn't degrade.
- Migration from old format performance - can't take 24h to switch to new version (downtime?).
- New locking mechanism functionality. Multiple commits. Read and write while commit. Commit after a failed commit.

## Milestone

- Implement adapter k/v for Postgres. Unit test. Performance test.
- Implement authorization, actions and multi part using kv. Include feature flag to control which implementation is active and migrate information to move from db to kv version. Unit test.
- Reimplement diagnostics similar functionality over kv. Current implementation perform several queries over the database to report information that may help us diagnose issues. We should apply the same queries or implementation specific queries that will help identify issues in the underlying storage.
- Implement graveler staging over kv, using kv to manage branch locking for commit and merge. Feature flag. Migrate information from db to kv for graveler (commit log, staging, ref-dump and restore). Unit test. Performance test.
- Integration testing. Remove old implementation.
