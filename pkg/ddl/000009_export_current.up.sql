BEGIN;

CREATE TYPE catalog_branch_export_status AS ENUM (
    'in-progress',
    'exported-successfully',
    'export-failed'
);

CREATE TABLE IF NOT EXISTS catalog_branches_export_state (
    branch_id integer PRIMARY KEY,
    current_ref VARCHAR,	-- If NULL, nothing currently exported (will export everything)
    state catalog_branch_export_status,
    error_message TEXT		-- If status='export-failed'
);

ALTER TABLE catalog_branches_export_state
    ADD CONSTRAINT branches_export_state_branches_fk
    FOREIGN KEY (branch_id) REFERENCES catalog_branches(id)
    -- Does *not* reference catalog_branches_export - state is independent of configuration,
    -- e.g. when configuration is changed.
    ON DELETE CASCADE;

ALTER TABLE catalog_branches_export_state
    ADD CONSTRAINT catalog_branches_export_error_on_failure
    CHECK ((state = 'export-failed') = (error_message IS NOT NULL));

-- BUG(ariels): reset export state when catalog_branches_export changes it physical address.

END;
