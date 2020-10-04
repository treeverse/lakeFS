BEGIN;
ALTER TABLE catalog_entries
    ALTER COLUMN min_commit DROP DEFAULT;
drop view if exists catalog_entries_v;
CREATE VIEW catalog_entries_v AS
SELECT e.branch_id,
       e.path,
       e.physical_address,
       e.creation_date,
       e.size,
       e.checksum,
       e.metadata,
       e.min_commit,
       e.max_commit,
       (e.min_commit < catalog_max_commit_id()) AS is_committed,
       (e.max_commit < catalog_max_commit_id()) AS is_deleted,
       (e.max_commit = 0) AS is_tombstone,
       e.ctid AS entry_ctid
FROM catalog_entries e;
update catalog_entries set min_commit = catalog_max_commit_id()  where min_commit = 0;

COMMIT;
