drop index catalog_entries_uidx;
drop index commits_lineage_idx;
ALTER TABLE ONLY catalog_entries
    ADD CONSTRAINT catalog_entries_branches_fk FOREIGN KEY (branch_id) REFERENCES catalog_branches(id) ON DELETE CASCADE NOT VALID;
CREATE INDEX catalog_fki_entries_branches_fk ON catalog_entries USING btree (branch_id);
ALTER TABLE ONLY catalog_entries
    ADD CONSTRAINT catalog_entries_pk PRIMARY KEY (path, branch_id, min_commit) INCLUDE (max_commit);
