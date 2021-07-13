BEGIN;

CREATE INDEX graveler_branches_idx_commit_id ON graveler_branches(commit_id);

-- function to return the first parent:
-- this takes into consideration commits created before fixing the parent switch bug (https://github.com/treeverse/lakeFS/issues/1730)
CREATE OR REPLACE FUNCTION first_parent(parents text[], version int)
RETURNS text AS
    $$
    DECLARE
        parent_switch_commit_version int;
    BEGIN
        parent_switch_commit_version := 1;
        RETURN CASE WHEN version < parent_switch_commit_version THEN parents[array_length(parents, 1)] ELSE parents[1] END;
    END
    $$
LANGUAGE plpgsql IMMUTABLE;

CREATE INDEX graveler_commits_idx_first_parent ON graveler_commits(first_parent(parents, version));

COMMIT;

