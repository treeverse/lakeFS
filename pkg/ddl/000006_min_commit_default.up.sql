BEGIN;

ALTER TABLE catalog_entries
    ALTER COLUMN min_commit DROP DEFAULT;

DROP VIEW IF EXISTS catalog_entries_v;
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

UPDATE catalog_entries SET min_commit = catalog_max_commit_id() WHERE min_commit = 0;

COMMIT;
