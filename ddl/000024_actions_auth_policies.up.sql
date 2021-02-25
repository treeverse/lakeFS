BEGIN;

UPDATE auth_policies SET statement=jsonb_set(statement,'{0,"Action"}','["ci:*"]', false) WHERE display_name='RepoManagementFullAccess';

UPDATE auth_policies SET statement=jsonb_set(statement,'{0,"Action"}','["ci:Read*"]', false) WHERE display_name='RepoManagementReadAll';

COMMIT;
