BEGIN;

UPDATE auth_policies
SET statement = statement - (SELECT ordinality-1 AS index
                             FROM auth_policies CROSS JOIN jsonb_array_elements(statement) WITH ORDINALITY
                             WHERE display_name = 'RepoManagementReadAll' AND value = '{"Action": ["branches:Get*"], "Effect": "allow", "Resource": "*"}')::int
WHERE display_name = 'RepoManagementReadAll' AND statement @> '[{"Action": ["branches:Get*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

UPDATE auth_policies
SET statement = statement - (SELECT ordinality-1 AS index
                             FROM auth_policies CROSS JOIN jsonb_array_elements(statement) WITH ORDINALITY
                             WHERE display_name = 'RepoManagementFullAccess' AND value = '{"Action": ["branches:*"], "Effect": "allow", "Resource": "*"}')::int
WHERE display_name = 'RepoManagementFullAccess' AND statement @> '[{"Action": ["branches:*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

COMMIT;
