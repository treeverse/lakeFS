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

CREATE TABLE IF NOT EXISTS auth_account_metadata (
    key_name text NOT NULL PRIMARY KEY,
    key_value text NOT NULL
);

-- index schema, containing information about lakeFS filesystem data
CREATE TABLE IF NOT EXISTS index_repositories(
                             id                varchar(64) NOT NULL PRIMARY KEY,
                             storage_namespace varchar     NOT NULL,
                             creation_date     timestamptz NOT NULL,
                             default_branch    varchar     NOT NULL
);


CREATE TABLE IF NOT EXISTS index_objects
(
    repository_id    varchar(64) REFERENCES index_repositories (id) NOT NULL,
    object_address   varchar(64)                              NOT NULL,
    checksum         varchar(64)                              NOT NULL,
    size             bigint                                   NOT NULL CHECK (size >= 0),
    physical_address varchar(64)                              NOT NULL,
    metadata         json                                     NOT NULL,

    PRIMARY KEY (repository_id, object_address)
);

CREATE TABLE IF NOT EXISTS index_object_dedup
(
    repository_id    varchar(64) REFERENCES index_repositories (id) NOT NULL,
    dedup_id         bytea                                    NOT NULL,
    physical_address varchar                                  NOT NULL,

    PRIMARY KEY (repository_id, dedup_id)
);

CREATE TABLE IF NOT EXISTS index_entries
(
    repository_id  varchar(64) REFERENCES index_repositories (id) NOT NULL,
    parent_address varchar(64)                              NOT NULL,
    name           varchar COLLATE "C"                      NOT NULL,
    address        varchar(64)                              NOT NULL,
    type varchar(24) NOT NULL CHECK (type in ('object', 'tree')),
    creation_date timestamptz NOT NULL,
    size bigint NOT NULL CHECK(size >= 0),
    checksum varchar(64) NOT NULL,
    object_count integer,

    PRIMARY KEY (repository_id, parent_address, name)
);


CREATE TABLE IF NOT EXISTS index_commits
(
    repository_id varchar(64) REFERENCES index_repositories (id) NOT NULL,
    address       varchar(64)                              NOT NULL,
    tree          varchar(64)                              NOT NULL,
    committer     varchar                                  NOT NULL,
    message       varchar                                  NOT NULL,
    creation_date timestamptz                              NOT NULL,
    parents       json                                     NOT NULL,
    metadata      json,

    PRIMARY KEY (repository_id, address)
);


CREATE TABLE IF NOT EXISTS index_branches
(
    repository_id  varchar(64) REFERENCES index_repositories (id) NOT NULL,
    id             varchar                                  NOT NULL,
    commit_id      varchar(64)                              NOT NULL,
    commit_root    varchar(64)                              NOT NULL,
    FOREIGN KEY (repository_id, commit_id) REFERENCES index_commits (repository_id, address),
    PRIMARY KEY (repository_id, id)
);


CREATE TABLE IF NOT EXISTS index_workspace_entries
(
    repository_id       varchar(64) REFERENCES index_repositories (id),
    branch_id           varchar NOT NULL,
    parent_path         varchar COLLATE "C" NOT NULL,
    path                varchar COLLATE "C" NOT NULL,

    -- entry fields
    entry_name          varchar COLLATE "C",
    entry_address       varchar(64),
    entry_type          varchar(24) CHECK (entry_type in ('object', 'tree')),
    entry_creation_date timestamptz,
    entry_size          bigint,
    entry_checksum      varchar(64),

    -- alternatively, tombstone
    tombstone           boolean NOT NULL,

    FOREIGN KEY (repository_id, branch_id) REFERENCES index_branches (repository_id, id),
    PRIMARY KEY (repository_id, branch_id, path)
);

CREATE INDEX IF NOT EXISTS idx_index_workspace_entries_parent_path ON index_workspace_entries (repository_id, branch_id, parent_path);


CREATE TABLE IF NOT EXISTS index_multipart_uploads
(
    repository_id    varchar(64) REFERENCES index_repositories (id) NOT NULL,
    upload_id        varchar                                  NOT NULL,
    path             varchar COLLATE "C"                      NOT NULL,
    creation_date    timestamptz                              NOT NULL,
    physical_address varchar                                  NOT NULL,
    PRIMARY KEY (repository_id, upload_id)
);

