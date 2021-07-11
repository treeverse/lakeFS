BEGIN;
CREATE INDEX graveler_commits_idx_parents ON graveler_commits(parents);
CREATE INDEX graveler_branches_idx_commit_id ON graveler_branches(commit_id);
COMMIT;
