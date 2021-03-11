BEGIN;
ALTER TABLE graveler_commits
RENAME COLUMN tree_id TO meta_range_id;
COMMIT;