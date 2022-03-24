BEGIN;
DROP INDEX email_unique_idx ;
ALTER TABLE auth_users
    DROP COLUMN IF EXISTS email,
    DROP COLUMN IF EXISTS password;
COMMIT ;
