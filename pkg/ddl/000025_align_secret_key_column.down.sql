BEGIN;
ALTER TABLE auth_credentials
    RENAME COLUMN secret_access_key TO access_secret_key;
COMMIT;