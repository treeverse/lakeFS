BEGIN;

-- auth schema, containing information about lakeFS authentication and authorization
CREATE TABLE IF NOT EXISTS auth_users (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,

    CONSTRAINT auth_users_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS auth_groups (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,

    CONSTRAINT auth_groups_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS auth_policies (
    id serial NOT NULL PRIMARY KEY,
    created_at timestamptz NOT NULL,
    display_name text NOT NULL,
    statement jsonb NOT NULL,
    CONSTRAINT auth_policies_unique_display_name UNIQUE (display_name)
);


CREATE TABLE IF NOT EXISTS auth_user_groups (
    user_id  integer REFERENCES auth_users (id) ON DELETE CASCADE NOT NULL,
    group_id integer REFERENCES auth_groups (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (user_id, group_id)
);
CREATE INDEX idx_auth_user_groups_user_id ON auth_user_groups (user_id); -- list groups by user
CREATE INDEX idx_auth_user_groups_group_id ON auth_user_groups (group_id); -- list users by group


CREATE TABLE IF NOT EXISTS auth_user_policies (
    user_id integer REFERENCES auth_users (id) ON DELETE CASCADE NOT NULL,
    policy_id integer REFERENCES auth_policies (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (user_id, policy_id)
);
CREATE INDEX idx_auth_user_policies_user_id ON auth_user_policies (user_id); -- list policies by user


CREATE TABLE IF NOT EXISTS auth_group_policies (
    group_id integer REFERENCES auth_groups (id) ON DELETE CASCADE NOT NULL,
    policy_id integer REFERENCES auth_policies (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (group_id, policy_id)
);
CREATE INDEX idx_auth_group_policies_group_id ON auth_group_policies (group_id); -- list policies by group


CREATE TABLE IF NOT EXISTS auth_credentials (
    access_key_id varchar(20) NOT NULL PRIMARY KEY,
    access_secret_key bytea NOT NULL,
    issued_date timestamptz NOT NULL,
    user_id integer REFERENCES auth_users (id) ON DELETE CASCADE
);
CREATE INDEX idx_auth_credentials_user_id ON auth_credentials (user_id); -- list credentials per user

CREATE TABLE IF NOT EXISTS auth_installation_metadata (
    key_name text NOT NULL PRIMARY KEY,
    key_value text NOT NULL
);

-- catalog: containing information about lakeFS metadata

CREATE FUNCTION catalog_max_commit_id() RETURNS bigint
    LANGUAGE sql IMMUTABLE COST 1
AS $$ select 1000000000000000000::bigint $$;

CREATE TYPE catalog_commit_status AS ENUM (
    'not_committed',
    'committed',
    'deleted'
);

CREATE TYPE catalog_merge_type AS ENUM (
    'none',
    'from_father',
    'from_son',
    'non_direct'
);

CREATE SEQUENCE catalog_branches_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE catalog_branches (
    repository_id integer NOT NULL,
    id integer DEFAULT nextval('catalog_branches_id_seq'::regclass) NOT NULL,
    name character varying(64) NOT NULL,
    lineage bigint[] DEFAULT array[]::bigint[]
);

CREATE SEQUENCE catalog_commit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 10;

CREATE TABLE catalog_commits (
    branch_id           bigint                                 NOT NULL,
    commit_id           bigint                                 NOT NULL,
    previous_commit_id  bigint                                 NOT NULL,
    committer           character varying,
    message             character varying,
    creation_date       timestamp with time zone DEFAULT now() NOT NULL,
    metadata            jsonb,
    merge_source_branch bigint,
    merge_source_commit bigint,
    merge_type          catalog_merge_type               DEFAULT 'none'::catalog_merge_type,
    lineage_commits     bigint[]                 DEFAULT array []::bigint[]
);

CREATE TABLE catalog_entries (
    branch_id bigint NOT NULL,
    path character varying COLLATE "C" NOT NULL,
    physical_address character varying,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    size bigint NOT NULL,
    checksum character varying(64) NOT NULL,
    metadata jsonb,
    min_commit bigint DEFAULT 0 NOT NULL,
    max_commit bigint DEFAULT catalog_max_commit_id() NOT NULL,
    -- If set, entry has expired.  Requests to retrieve may return "410 Gone".
    is_expired BOOLEAN DEFAULT false NOT NULL
);
ALTER TABLE ONLY catalog_entries ALTER COLUMN path SET STATISTICS 10000;

CREATE VIEW catalog_entries_v AS
 SELECT e.branch_id,
    e.path,
    e.physical_address,
    e.creation_date,
    e.size,
    e.checksum,
    e.metadata,
    e.min_commit,
    e.max_commit,
    (e.min_commit <> 0) AS is_committed,
    (e.max_commit < catalog_max_commit_id()) AS is_deleted,
    ((e.max_commit < e.min_commit) OR (e.max_commit = 0)) AS is_tombstone,
    e.ctid AS entry_ctid,
        CASE e.min_commit
            WHEN 0 THEN catalog_max_commit_id()
            ELSE e.min_commit
        END AS commit_weight
   FROM catalog_entries e;

CREATE TABLE catalog_multipart_uploads (
    repository_id integer NOT NULL,
    upload_id character varying NOT NULL,
    path character varying COLLATE "C" NOT NULL,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    physical_address character varying
);

CREATE TABLE catalog_object_dedup (
    repository_id integer NOT NULL,
    dedup_id bytea NOT NULL,
    physical_address character varying NOT NULL
);

CREATE TABLE catalog_repositories (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    storage_namespace character varying NOT NULL,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    default_branch integer NOT NULL
);

CREATE SEQUENCE catalog_repositories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS catalog_repositories_config (
    repository_id integer     NOT NULL,
    key           varchar     NOT NULL,
    value         jsonb       NOT NULL,
    description   varchar,
    created_at    timestamptz NOT NULL,

    PRIMARY KEY (repository_id, key)
);

ALTER TABLE ONLY catalog_branches
    ADD CONSTRAINT catalog_branches_pk PRIMARY KEY (id);

ALTER TABLE ONLY catalog_commits
    ADD CONSTRAINT catalog_commits_pk PRIMARY KEY (branch_id, commit_id);

ALTER TABLE ONLY catalog_entries
    ADD CONSTRAINT catalog_entries_pk PRIMARY KEY (path, branch_id, min_commit) INCLUDE (max_commit);

ALTER TABLE ONLY catalog_multipart_uploads
    ADD CONSTRAINT catalog_multipart_uploads_pk PRIMARY KEY (upload_id);

ALTER TABLE ONLY catalog_object_dedup
    ADD CONSTRAINT catalog_object_dedup_pk PRIMARY KEY (repository_id, dedup_id);

ALTER TABLE ONLY catalog_repositories
    ADD CONSTRAINT catalog_repositories_pk PRIMARY KEY (id);

ALTER TABLE ONLY catalog_repositories_config
    ADD CONSTRAINT catalog_repositories_config_fk FOREIGN KEY (repository_id) REFERENCES catalog_repositories(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX catalog_branches_repository_name_uindex ON catalog_branches USING btree (name, repository_id);

CREATE INDEX catalog_fki_branch_repository_fk ON catalog_branches USING btree (repository_id);

CREATE INDEX catalog_fki_entries_branches_fk ON catalog_entries USING btree (branch_id);

CREATE INDEX catalog_fki_repositories_branches_id_fk ON catalog_repositories USING btree (default_branch);

CREATE UNIQUE INDEX catalog_repositories_name_uindex ON catalog_repositories USING btree (name);

ALTER TABLE ONLY catalog_branches
    ADD CONSTRAINT branch_repository_fk FOREIGN KEY (repository_id) REFERENCES catalog_repositories(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE ONLY catalog_commits
    ADD CONSTRAINT catalog_commits_branches_repository_id_fk FOREIGN KEY (branch_id) REFERENCES catalog_branches(id) ON DELETE CASCADE;

ALTER TABLE ONLY catalog_entries
    ADD CONSTRAINT catalog_entries_branches_fk FOREIGN KEY (branch_id) REFERENCES catalog_branches(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE ONLY catalog_multipart_uploads
    ADD CONSTRAINT catalog_multipart_uploads_repository_id_fk FOREIGN KEY (repository_id) REFERENCES catalog_repositories(id) ON DELETE CASCADE;

ALTER TABLE ONLY catalog_object_dedup
    ADD CONSTRAINT catalog_object_dedup_repository_id_fk FOREIGN KEY (repository_id) REFERENCES catalog_repositories(id) ON DELETE CASCADE;

ALTER TABLE ONLY catalog_repositories
    ADD CONSTRAINT catalog_repositories_branches_id_fk FOREIGN KEY (default_branch) REFERENCES catalog_branches(id) DEFERRABLE INITIALLY DEFERRED NOT VALID;

COMMIT;
