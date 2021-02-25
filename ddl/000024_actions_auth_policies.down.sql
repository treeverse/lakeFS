BEGIN;

UPDATE auth_policies SET statement=jsonb_set(statement,'{0,"Action"}','["retention:*"]', false) WHERE display_name='RepoManagementFullAccess';

UPDATE auth_policies SET statement=jsonb_set(statement,'{0,"Action"}','["retention:Get*"]', false) WHERE display_name='RepoManagementReadAll';

COMMIT;
