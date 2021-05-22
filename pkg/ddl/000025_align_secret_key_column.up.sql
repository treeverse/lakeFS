BEGIN;
ALTER TABLE auth_credentials
    RENAME COLUMN access_secret_key TO secret_access_key;
COMMIT;