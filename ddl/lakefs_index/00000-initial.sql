-- index schema, containing information about lakeFS filesystem data
CREATE TABLE repositories (
    id varchar(64) NOT NULL PRIMARY KEY,
    storage_namespace varchar NOT NULL,
    creation_date timestamptz NOT NULL,
    default_branch varchar NOT NULL
);


CREATE TABLE objects (
    repository_id varchar(64) REFERENCES repositories(id) NOT NULL,
    address varchar(64) NOT NULL,
    checksum varchar(64) NOT NULL,
    size bigint NOT NULL CHECK(size >= 0),
    blocks json NOT NULL,
    metadata json NOT NULL,

    PRIMARY KEY (repository_id, address)
);


CREATE TABLE roots (
    repository_id varchar(64) REFERENCES repositories(id) NOT NULL,
    address varchar(64) NOT NULL,
    creation_date timestamptz NOT NULL,
    size bigint NOT NULL CHECK(size >= 0),

    PRIMARY KEY (repository_id, address)
);


CREATE TABLE entries (
    repository_id varchar(64) REFERENCES repositories(id) NOT NULL,
    parent_address varchar(64) NOT NULL,
    name varchar NOT NULL,
    address varchar(64) NOT NULL,
    type varchar(24) NOT NULL CHECK (type in ('object', 'tree')),
    creation_date timestamptz NOT NULL,
    size bigint NOT NULL CHECK(size >= 0),
    checksum varchar(64) NOT NULL,

    PRIMARY KEY (repository_id, parent_address, name)
);


CREATE TABLE commits (
    repository_id varchar(64) REFERENCES repositories(id) NOT NULL,
    address varchar(64) NOT NULL,
    tree varchar(64) NOT NULL,
    committer varchar NOT NULL,
    message varchar NOT NULL,
    creation_date timestamptz NOT NULL,
    parents json NOT NULL,
    metadata json,

    FOREIGN KEY (repository_id, tree) REFERENCES roots (repository_id, address),
    PRIMARY KEY (repository_id, address)
);


CREATE TABLE branches (
    repository_id varchar(64) REFERENCES repositories(id) NOT NULL,
    id varchar NOT NULL,
    commit_id varchar(64) NOT NULL,
    commit_root varchar(64) NOT NULL,

    workspace_root varchar(64) NOT NULL, -- will be removed as it doesn't really fit the postgres model

    FOREIGN KEY (repository_id, commit_id) REFERENCES commits (repository_id, address),
    PRIMARY KEY (repository_id, id)
);


CREATE TABLE workspace_entries (
    repository_id varchar(64) REFERENCES repositories(id),
    branch_id varchar  NOT NULL,
    parent_path varchar  NOT NULL,
    path varchar  NOT NULL,

    -- entry fields
    entry_name varchar,
    entry_address varchar(64),
    entry_type varchar(24) CHECK (entry_type in ('object', 'tree')),
    entry_creation_date timestamptz,
    entry_size bigint,
    entry_checksum varchar(64),

    -- alternatively, tombstone
    tombstone boolean NOT NULL,

    FOREIGN KEY (repository_id, branch_id) REFERENCES branches (repository_id, id),
    PRIMARY KEY (repository_id, branch_id, path)
);

CREATE INDEX idx_workspace_entries_parent_path ON workspace_entries (repository_id, branch_id, parent_path);


CREATE TABLE multipart_uploads (
    repository_id varchar(64) REFERENCES repositories(id)  NOT NULL,
    id varchar(128)  NOT NULL,
    path varchar NOT NULL,
    creation_date timestamptz  NOT NULL,

    PRIMARY KEY (repository_id, id)
);


CREATE TABLE multipart_upload_parts (
    repository_id varchar(64) REFERENCES repositories(id) NOT NULL,
    upload_id varchar NOT NULL,
    part_number integer NOT NULL,
    checksum varchar(64)  NOT NULL,
    creation_date timestamptz NOT NULL,
    size bigint NOT NULL CHECK(size >= 0),
    blocks json NOT NULL,

    FOREIGN KEY (repository_id, upload_id) REFERENCES multipart_uploads (repository_id, id),
    PRIMARY KEY (repository_id, upload_id, part_number)
);
