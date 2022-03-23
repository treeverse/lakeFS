BEGIN;

ALTER TABLE auth_users
    ADD COLUMN IF NOT EXISTS email TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS password bytea ;

CREATE UNIQUE INDEX email_unique_idx on auth_users (LOWER(email));

COMMIT;
