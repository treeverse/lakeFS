BEGIN;
ALTER TABLE graveler_commits
RENAME COLUMN range_id TO tree_id;
COMMIT;