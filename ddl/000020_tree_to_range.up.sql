BEGIN;
ALTER TABLE graveler_commits
RENAME COLUMN tree_id TO range_id;
COMMIT;