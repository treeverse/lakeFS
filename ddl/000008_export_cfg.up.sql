BEGIN;

CREATE TABLE IF NOT EXISTS catalog_branches_export (
    branch_id integer PRIMARY KEY,
    export_path varchar NOT NULL,
    export_status_path varchar NOT NULL,
    last_keys_in_prefix_regexp varchar NOT NULL
);

ALTER TABLE catalog_branches_export
    ADD CONSTRAINT branches_export_branches_fk
        FOREIGN KEY (branch_id) REFERENCES catalog_branches(id)
	ON DELETE CASCADE;
END;
