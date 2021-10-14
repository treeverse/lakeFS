BEGIN;

ALTER TABLE auth_users
    ADD COLUMN IF NOT EXISTS friendly_name TEXT,
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT '';


COMMENT ON COLUMN auth_users.friendly_name IS 'If set, a shorter name for user than "display_name"';

END;
