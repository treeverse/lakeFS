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

CREATE OR REPLACE FUNCTION tree_from_root(repository_id varchar, root_address varchar)
    RETURNS TABLE (parent_path varchar, repository_id varchar, parent_address varchar, name varchar, address varchar, type varchar, creation_date timestamp with time zone, size bigint, checksum varchar)
AS
$body$
WITH RECURSIVE cte AS (
    SELECT '' AS parent_path, e.*
    FROM entries e
    WHERE e.repository_id = $1 AND parent_address = $2
    UNION ALL
    SELECT cte_1.parent_path || cte_1.name, entries.*
    FROM cte cte_1 JOIN entries ON entries.parent_address = cte_1.address
)
SELECT cte.* FROM cte
$body$ language sql;

CREATE OR REPLACE FUNCTION combined_ws_fn(repository_id varchar, branch_id varchar, only_diff bool default false)
    RETURNS TABLE (repository_id varchar, branch_id varchar, parent_path varchar, path varchar,
                   name varchar, entry_type varchar, size bigint, creation_date timestamp with time zone, checksum varchar, address varchar, diff_type varchar, tombstone bool) AS
$BODY$
SELECT $1,
       $2,
       COALESCE(wse.parent_path, tfr.parent_path)           AS parent_path,
       COALESCE(wse.path, tfr.parent_path || tfr.name)      AS path,
       COALESCE(wse.entry_name, tfr.name)                   AS name,
       COALESCE(wse.entry_type, tfr.type)                   AS entry_type,
       COALESCE(wse.entry_size, tfr.size)                   AS size,
       COALESCE(wse.entry_creation_date, tfr.creation_date) AS creation_date,
       COALESCE(wse.entry_checksum, tfr.checksum)           AS checksum,
       COALESCE(wse.entry_address, tfr.address)             AS address,
       CASE WHEN tfr.name IS NULL THEN 'ADDED'
            WHEN wse.tombstone THEN 'DELETED'
            WHEN wse.entry_checksum <> tfr.checksum AND wse.entry_type = 'object' THEN 'CHANGED'
       END AS diff_type,
       tombstone
FROM (SELECT * FROM workspace_entries WHERE workspace_entries.repository_id = $1 AND workspace_entries.branch_id = $2) wse
         FULL JOIN tree_from_root($1, (SELECT commit_root FROM branches WHERE branches.repository_id = $1 AND id = $2)) tfr
                         ON wse.parent_path = tfr.parent_path AND wse.entry_name = tfr.name AND wse.repository_id = tfr.repository_id
WHERE (NOT $3 OR wse.path IS NOT NULL)
$BODY$ language sql;

-- CREATE OR REPLACE FUNCTION ws_diff_fn(repository_id varchar, branch_id varchar)
--     RETURNS TABLE (repository_id varchar, branch_id varchar, object_path varchar, diff_type varchar, diff_path varchar) AS
-- $BODY$
-- SELECT cw.repository_id, cw.branch_id, path as object_path, diff_type,
--     CASE WHEN diff_type = 'ADDED' OR diff_type = 'DELETED'
--             THEN (SELECT min(path) from combined_ws_fn($1, $2) cw2 where cw.path like cw2.path || '%' and cw2.diff_type = cw.diff_type)
--         ELSE path END AS diff_path
-- FROM combined_ws_fn($1, $2) cw WHERE entry_type='object' AND diff_type IS NOT NULL
-- $BODY$ language plpgsql;
--
-- CREATE OR REPLACE FUNCTION tree_dif(repository_id varchar, branch_id varchar)
-- RETURNS TABLE (r varchar) AS
-- $BODY$recur
-- WITH RECURSIVE cte AS(
--     SELECT 0 AS depth, * FROM combined_ws_fn($1, $2, 0)
--     UNION ALL
--     SELECT cte_1.depth, cws.* FROM combined_ws_fn($1, $2, depth + 1) cws JOIN cte cte_1 ON cte_1.path = cws.parent_path
-- )
-- SELECT * FROM cte
--$BODY$ language sql;

CREATE OR REPLACE FUNCTION ws_diff_fn(repository_id varchar, branch_id varchar)
    RETURNS TABLE (repository_id varchar, branch_id varchar, parent_path varchar, path varchar,
                   name varchar, entry_type varchar, size bigint, creation_date timestamp with time zone, checksum varchar, address varchar, diff_type varchar, tombstone bool) AS
$BODY$
SELECT wse.repository_id, wse.branch_id, wse.parent_path AS parent_path, wse.path, wse.entry_name, wse.entry_type, wse.entry_size, wse.entry_creation_date,
       wse.entry_checksum, wse.entry_address,
       CASE WHEN tfr.name IS NULL THEN 'ADDED'
            WHEN wse.tombstone THEN 'DELETED'
            WHEN wse.entry_checksum <> tfr.checksum AND wse.entry_type = 'object' THEN 'CHANGED'
       END AS diff_type,
       tombstone
FROM workspace_entries wse
         LEFT JOIN tree_from_root($1, (SELECT commit_root FROM branches WHERE branches.repository_id = $1 AND id = $2)) tfr
                         ON wse.parent_path = tfr.parent_path AND wse.entry_name = tfr.name AND wse.repository_id = tfr.repository_id
         WHERE wse.repository_id = $1 AND wse.branch_id = $2
$BODY$ language sql;

CREATE OR REPLACE FUNCTION ws_diff_fn_improved(repository_id varchar, branch_id varchar)
RETURNS TABLE(path varchar, diff_type varchar,  entry_type varchar) AS
    $BODY$
        SELECT d1.path, d1.diff_type, d1.entry_type FROM combined_ws_fn($1, $2, true) d1 JOIN combined_ws_fn($1, $2, true) d2
            ON d1.parent_path = d2.path AND (d1.diff_type = 'CHANGED' OR d2.diff_type IS NULL OR d2.diff_type <> d1.diff_type)
    $BODY$ language sql;