BEGIN;

UPDATE auth_policies
SET statement = statement - (SELECT ordinality-1 AS index
        FROM auth_policies CROSS JOIN jsonb_array_elements(statement) WITH ORDINALITY
        WHERE display_name = 'RepoManagementReadAll' AND value = '{"Action": ["ci:Read*"], "Effect": "allow", "Resource": "*"}')::int
    WHERE display_name = 'RepoManagementReadAll' AND statement @> '[{"Action": ["ci:Read*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

UPDATE auth_policies
SET statement = statement - (SELECT ordinality-1 AS index
        FROM auth_policies CROSS JOIN jsonb_array_elements(statement) WITH ORDINALITY
        WHERE display_name = 'RepoManagementFullAccess' AND value = '{"Action": ["ci:*"], "Effect": "allow", "Resource": "*"}')::int
    WHERE display_name = 'RepoManagementFullAccess' AND statement @> '[{"Action": ["ci:*"], "Effect": "allow", "Resource": "*"}]'::jsonb;

COMMIT;
