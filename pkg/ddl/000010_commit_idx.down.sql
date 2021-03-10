BEGIN;
DROP INDEX IF EXISTS entries_uncommitted_branch_min_commit;
DROP INDEX IF EXISTS entries_branch_min_commit;
CREATE INDEX entries_uncommitted_branch_min_commit
    ON catalog_entries USING btree
        (branch_id ASC NULLS LAST, min_commit ASC NULLS LAST)
    WHERE min_commit = '1000000000000000000'::bigint;
COMMIT;
