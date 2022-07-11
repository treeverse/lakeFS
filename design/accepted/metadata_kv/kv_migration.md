# KV Migration (export import phase 1) Design

## Definitions
1. KV Migration - The transition from schema based postgres implementation to key value store based postgres implementation
2. DB Migration - The process of migrating postgres data using the postgres versioning schema
3. KV Migration Version - “Schema” version for which the KV Migration will occur
4. KV Enabled Flag - Development flag to enable the use of the KV Store and to signal migration to KV is required
5. DB Import - The process of creating the initial lakeFS DB via a 'file' which was previously exported. Our plan is
   to create a preliminary implementation of the lakeFS DB export / import feature (specifically DB Import) as part of the
   KV development and use it in the KV Migration process.


## Abstract
DB migration mechanism relies on postgres versioning schema.
KV Migration flow is a transitory mechanism, which will be needed only when upgrading lakeFS to the KV Migration Version. For efficiency and re-use we would like to create a preliminary version of the export-import feature to implement the KV Migration flow.

## Considerations
Although we cannot use the DB Migration flow to handle the KV migration, it must be incorporated into it and executed in the context of the DB migration.
DB migration must be performed up to the latest version before the KV Migration Version and only then KV Migration can happen
KV Migration Version will be a consecutive version succeeding the last DB Migration version.  
During development we will advance the KV Migration Version on any change in the DB schema until we reach feature complete.
Once we release lakeFS with KV store, the KV Migration Version will be fixed to the latest DB schema version.  
Migration check for KV should be enabled only when the KV Enabled flag was passed. While the flag is disabled migration will
behave as usual, not taking into account KV migration.


## Migration Mechanism
1. Migrate up command will perform DB migration up to the latest version
2. Check if current version equals the KV Migration Version and perform KV Migration
3. KV Migration will consist of 2 main steps: Migrate, Import
4. Migrate:
   1. Each migrating package will implement a 'Migrate' method which will implement its migration logic
   2. Each migrating package will read the data from its tables and create the appropriate KV models to support its functionality.
   3. Data will be saved in a format that can be read by the package's KV implementation
5. Import:
   1. Read the package's data directly to the DB in KV format
6. Upon successful import - update DB migration version in DB
7. Drop old DB tables - support an optional flag for testing purposes

## Failures:
1. In event of failure on any of the KV migration steps, before the DB migration version is updated, the migration will be considered as failed
2. Upon re-run of migrate up, the KV Migration will start from scratch, disregarding any previous changes
3. In the event of failure after updating the DB migration version and before/during cleanup (tables drop), an event will be issued to the user, requiring manual intervention