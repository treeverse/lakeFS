BEGIN;
DROP INDEX graveler_commits_idx_first_parent;
DROP FUNCTION first_parent;
DROP INDEX graveler_branches_idx_commit_id;
COMMIT;
