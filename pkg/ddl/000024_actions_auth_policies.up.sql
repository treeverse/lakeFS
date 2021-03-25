BEGIN;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["ci:Read*"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE display_name = 'RepoManagementReadAll' AND NOT statement @> '[{"Action": ["ci:Read*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["ci:*"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE display_name = 'RepoManagementFullAccess' AND NOT statement @> '[{"Action": ["ci:*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

COMMIT;
