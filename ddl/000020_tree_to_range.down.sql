BEGIN;
ALTER TABLE graveler_commits
RENAME COLUMN meta_range_id TO tree_id;
COMMIT;