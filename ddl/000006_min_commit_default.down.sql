BEGIN;
ALTER TABLE catalog_entries
    ALTER COLUMN min_commit SET DEFAULT 0;
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
       (e.min_commit <> 0) AS is_committed,
       (e.max_commit < catalog_max_commit_id()) AS is_deleted,
       ((e.max_commit < e.min_commit) OR (e.max_commit = 0)) AS is_tombstone,
       e.ctid AS entry_ctid,
       CASE e.min_commit
           WHEN 0 THEN catalog_max_commit_id()
           ELSE e.min_commit
           END AS commit_weight
FROM catalog_entries e;
update catalog_entries set min_commit = 0  where min_commit = catalog_max_commit_id();
COMMIT;