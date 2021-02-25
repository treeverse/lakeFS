BEGIN;

update auth_policies
set statement = statement - (select ordinality-1 as index
        from public.auth_policies cross join jsonb_array_elements(statement) with ordinality
        where display_name = 'RepoManagementReadAll' and value = '{"Action": ["ci:Read*"], "Effect": "allow", "Resource": "*"}')::int
    where display_name = 'RepoManagementReadAll' AND statement @> '[{"Action": ["ci:Read*"], "Effect": "allow", "Resource": "*"}]'::jsonb ;

update auth_policies
set statement = statement - (select ordinality-1 as index
        from public.auth_policies cross join jsonb_array_elements(statement) with ordinality
        where display_name = 'RepoManagementFullAccess' and value = '{"Action": ["ci:*"], "Effect": "allow", "Resource": "*"}')::int
    where display_name = 'RepoManagementFullAccess' AND statement @> '[{"Action": ["ci:*"], "Effect": "allow", "Resource": "*"}]'::jsonb ;

COMMIT;
