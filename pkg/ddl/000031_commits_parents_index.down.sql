BEGIN;
DROP INDEX graveler_commits_idx_parents;
DROP INDEX graveler_branches_idx_commit_id;
COMMIT;
