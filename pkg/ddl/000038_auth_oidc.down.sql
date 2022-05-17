BEGIN;
ALTER TABLE auth_users
    DROP COLUMN IF EXISTS oidc_openid;
COMMIT ;
