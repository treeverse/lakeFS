BEGIN;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["fs:ReadConfig"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE display_name = 'RepoManagementReadAll' AND NOT statement @> '[{"Action": ["fs:ReadConfig"], "Effect": "allow", "Resource": "*"}]'::jsonb;

UPDATE auth_policies
SET statement = statement || '[{"Action": ["fs:ReadConfig"], "Effect": "allow", "Resource": "*"}]'::jsonb
WHERE display_name = 'RepoManagementFullAccess' AND NOT statement @> '[{"Action": ["fs:ReadConfig"], "Effect": "allow", "Resource": "*"}]'::jsonb;

COMMIT;
