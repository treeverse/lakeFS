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
    PRIMARY KEY (repository_id, branch_id, path, entry_type)
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
    upload_id varchar(128)  NOT NULL,
    part_number integer NOT NULL,
    checksum varchar(64)  NOT NULL,
    creation_date timestamptz NOT NULL,
    size bigint NOT NULL CHECK(size >= 0),
    blocks json NOT NULL,

    FOREIGN KEY (repository_id, upload_id) REFERENCES multipart_uploads (repository_id, id),
    PRIMARY KEY (repository_id, upload_id, part_number)
);

CREATE OR REPLACE VIEW entries_with_path AS
WITH RECURSIVE cte AS (
    SELECT entries.parent_address AS root_address,
           array((SELECT id FROM branches WHERE commit_root = entries.parent_address)) AS branches,
           '' AS parent_path, entries.*
    FROM entries JOIN branches ON entries.parent_address = branches.commit_root
    WHERE branches.repository_id = entries.repository_id
    UNION ALL
    SELECT cte_1.root_address,
           cte_1.branches,
           cte_1.parent_path || cte_1.name,
           entries.*
    FROM cte cte_1
             JOIN entries ON entries.parent_address = cte_1.address
)
SELECT cte.* FROM cte;

CREATE OR REPLACE VIEW combined_workspace AS
SELECT COALESCE(wse.repository_id, ewp.repository_id) as repository_id,
       COALESCE(wse.branch_id, ewp.branch) AS branch_id,
       COALESCE(wse.parent_path, ewp.parent_path) AS parent_path,
       COALESCE(wse.path, ewp.parent_path || ewp.name) AS path,
       COALESCE(wse.entry_name, ewp.name) AS name,
       COALESCE(wse.entry_type, ewp.type) AS entry_type,
       COALESCE(wse.entry_size, ewp.size) AS size,
       COALESCE(wse.entry_creation_date, ewp.creation_date) AS creation_date,
       COALESCE(wse.entry_checksum, ewp.checksum) AS checksum,
       COALESCE(wse.entry_address, ewp.address) AS address,
       CASE WHEN ewp.name IS NULL THEN 'ADDED'
            WHEN wse.tombstone THEN 'DELETED'
            WHEN wse.entry_checksum <> ewp.checksum AND wse.entry_type = 'object' THEN 'CHANGED'
           END AS diff_type,
       tombstone
FROM workspace_entries wse
         FULL OUTER JOIN (SELECT unnest(branches) AS branch, *
                          FROM entries_with_path) ewp
                         ON wse.branch_id= ewp.branch AND wse.parent_path = ewp.parent_path AND
                            wse.entry_name = ewp.name AND wse.repository_id = ewp.repository_id;


CREATE OR REPLACE VIEW workspace_diff AS SELECT repository_id, branch_id, path as object_path, diff_type,
                                                CASE WHEN diff_type = 'ADDED' OR diff_type = 'DELETED' THEN (SELECT min(path) from combined_workspace cw2 where cw.path like cw2.path || '%' and cw2.diff_type = cw.diff_type)
                                                     ELSE path END AS diff_path FROM combined_workspace cw WHERE entry_type='object' AND diff_type IS NOT NULL
