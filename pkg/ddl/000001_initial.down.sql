BEGIN;

DROP TABLE IF EXISTS auth_credentials;
DROP TABLE IF EXISTS auth_group_policies;
DROP TABLE IF EXISTS auth_user_policies;
DROP TABLE IF EXISTS auth_user_groups;
DROP TABLE IF EXISTS auth_policies;
DROP TABLE IF EXISTS auth_groups;
DROP TABLE IF EXISTS auth_users;
DROP TABLE IF EXISTS auth_installation_metadata;
DROP TABLE IF EXISTS catalog_repositories_config;
DROP TABLE IF EXISTS catalog_object_dedup;
DROP TABLE IF EXISTS catalog_multipart_uploads;
DROP VIEW IF EXISTS catalog_entries_v;
DROP TABLE IF EXISTS catalog_entries;
DROP TABLE IF EXISTS catalog_commits;
DROP TABLE IF EXISTS catalog_branches CASCADE ;
DROP TYPE IF EXISTS catalog_merge_type;
DROP TYPE IF EXISTS catalog_commit_status;
DROP TABLE IF EXISTS catalog_repositories;
DROP SEQUENCE IF EXISTS catalog_repositories_id_seq;
DROP SEQUENCE IF EXISTS catalog_branches_id_seq;
DROP SEQUENCE IF EXISTS catalog_commit_id_seq;
DROP FUNCTION IF EXISTS catalog_max_commit_id;

COMMIT;
