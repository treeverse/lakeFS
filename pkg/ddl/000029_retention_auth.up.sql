BEGIN;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["retention:Get*"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE display_name = 'RepoManagementReadAll' AND NOT statement @> '[{"Action": ["retention:Get*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["retention:*"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE display_name = 'RepoManagementFullAccess' AND NOT statement @> '[{"Action": ["retention:*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

COMMIT;
