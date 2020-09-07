alter table catalog_entries drop constraint if exists catalog_entries_pk;
drop index if exists catalog_fki_entries_branches_fk;
alter table catalog_entries drop constraint if exists catalog_entries_branches_fk;
CREATE UNIQUE INDEX catalog_entries_uidx
    ON catalog_entries USING btree
        (branch_id ASC NULLS LAST, path COLLATE pg_catalog."C" ASC NULLS LAST, min_commit DESC NULLS FIRST)
    INCLUDE(max_commit);
CREATE INDEX commits_lineage_idx
    ON catalog_commits USING btree
        (merge_type ASC NULLS LAST, branch_id ASC NULLS LAST, commit_id DESC NULLS LAST)
    INCLUDE(lineage_commits, merge_source_branch, merge_source_commit)
    WHERE merge_type = ANY (ARRAY['from_parent'::catalog_merge_type, 'from_child'::catalog_merge_type]);