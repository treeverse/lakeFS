BEGIN;
ALTER TABLE catalog_entries DROP CONSTRAINT IF EXISTS catalog_entries_pk;
DROP INDEX IF EXISTS catalog_fki_entries_branches_fk;
ALTER TABLE catalog_entries DROP CONSTRAINT IF EXISTS catalog_entries_branches_fk;
CREATE UNIQUE INDEX catalog_entries_uidx
    ON catalog_entries USING btree
        (branch_id ASC NULLS LAST, path COLLATE pg_catalog."C" ASC NULLS LAST, min_commit DESC NULLS FIRST)
    INCLUDE(max_commit);
CREATE INDEX catalog_commits_lineage_idx
    ON catalog_commits USING btree
        (merge_type ASC NULLS LAST, branch_id ASC NULLS LAST, commit_id DESC NULLS LAST)
    INCLUDE(lineage_commits, merge_source_branch, merge_source_commit)
    WHERE merge_type = ANY (ARRAY['from_parent'::catalog_merge_type, 'from_child'::catalog_merge_type]);
COMMIT;
