CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE TYPE commit_status AS ENUM (
    'not_committed',
    'committed',
    'deleted'
);

CREATE TYPE merge_type AS ENUM (
    'none',
    'from_father',
    'from_son',
    'non_direct'
);

CREATE FUNCTION get_next_marker(min_dir text, delim text) RETURNS text
    LANGUAGE sql
    AS $$select
case when strpos(min_dir,delim) > 0 then left(min_dir,strpos(min_dir,delim)) || chr(254)
								else min_dir end$$;

CREATE SEQUENCE branches_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE branches (
    repository_id integer NOT NULL,
    id integer DEFAULT nextval('branches_id_seq'::regclass) NOT NULL,
    name character varying(64) NOT NULL,
    next_commit integer DEFAULT 1 NOT NULL
);

CREATE TABLE commits (
    branch_id integer NOT NULL,
    commit_id integer NOT NULL,
    committer character varying,
    message character varying,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb,
    merge_source_branch integer,
    merge_source_commit integer,
    merge_type merge_type DEFAULT 'none'::merge_type NOT NULL,
    CONSTRAINT merge_check CHECK ((((merge_type = 'none'::merge_type) AND (merge_source_branch IS NULL) AND (merge_source_commit IS NULL)) OR ((merge_type <> 'none'::merge_type) AND (merge_source_branch IS NOT NULL) AND (merge_source_commit IS NOT NULL))))
);

CREATE TABLE entries (
    branch_id integer NOT NULL,
    path character varying NOT NULL,
    physical_address character varying(64),
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    size bigint NOT NULL,
    checksum character varying(64) NOT NULL,
    metadata jsonb,
    min_commit integer DEFAULT 0 NOT NULL,
    max_commit integer DEFAULT ('01111111111111111111111111111111'::"bit")::integer NOT NULL
);

CREATE VIEW dir_list_v AS
 WITH RECURSIVE dir_list AS (
         SELECT get_next_marker(min((entries.path)::text), '/'::text) AS marker
           FROM entries
          WHERE (entries.branch_id = 1)
        UNION ALL
         SELECT ( SELECT get_next_marker(min((entries.path)::text), '/'::text) AS marker
                   FROM entries
                  WHERE (((entries.path)::text > dir_list_1.marker) AND (entries.branch_id = 1))) AS marker
           FROM dir_list dir_list_1
          WHERE (dir_list_1.marker IS NOT NULL)
        )
 SELECT dir_list.marker
   FROM dir_list
  WHERE (dir_list.marker IS NOT NULL);

CREATE VIEW entries_v AS
 SELECT entries.branch_id,
    entries.path,
    entries.physical_address,
    entries.creation_date,
    entries.size,
    entries.checksum,
    entries.metadata,
    entries.min_commit,
    entries.max_commit,
    (entries.min_commit <> 0) AS is_committed,
    (entries.max_commit <> ('01111111111111111111111111111111'::"bit")::integer) AS is_deleted,
    ((entries.max_commit < entries.min_commit) OR (entries.max_commit = 0)) AS is_tombstone
   FROM entries;

CREATE TABLE lineage (
    branch_id integer NOT NULL,
    precedence integer NOT NULL,
    ancestor_branch integer NOT NULL,
    effective_commit integer NOT NULL,
    min_commit integer DEFAULT 0 NOT NULL,
    max_commit integer DEFAULT 2147483647 NOT NULL
);

CREATE VIEW lineage_v AS
 SELECT lineage.branch_id,
    false AS main_branch,
    lineage.precedence,
    lineage.ancestor_branch,
    lineage.effective_commit,
    lineage.min_commit,
    lineage.max_commit,
    (lineage.max_commit = ('01111111111111111111111111111111'::"bit")::integer) AS active_lineage
   FROM lineage
UNION ALL
 SELECT branches.id AS branch_id,
    true AS main_branch,
    0 AS precedence,
    branches.id AS ancestor_branch,
    (branches.next_commit - 1) AS effective_commit,
    0 AS min_commit,
    ('01111111111111111111111111111111'::"bit")::integer AS max_commit,
    true AS active_lineage
   FROM branches;

CREATE VIEW entries_lineage_full_v AS
 SELECT l.branch_id AS displayed_branch,
    e.branch_id AS source_branch,
    e.path,
    e.min_commit,
        CASE
            WHEN l.main_branch THEN e.max_commit
            WHEN (e.max_commit <= l.effective_commit) THEN e.max_commit
            ELSE ('01111111111111111111111111111111'::"bit")::integer
        END AS max_commit,
    e.physical_address,
    e.creation_date,
    e.size,
    e.checksum,
    e.metadata,
    l.precedence,
    row_number() OVER (PARTITION BY l.branch_id, e.path ORDER BY l.precedence,
        CASE
            WHEN (l.main_branch AND (e.min_commit = 0)) THEN ('01111111111111111111111111111111'::"bit")::integer
            ELSE e.min_commit
        END DESC) AS rank,
    l.min_commit AS branch_min_commit,
    l.max_commit AS branch_max_commit,
    e.is_committed,
        CASE
            WHEN l.main_branch THEN e.is_deleted
            ELSE (e.max_commit <= l.effective_commit)
        END AS is_deleted,
    l.active_lineage,
    l.effective_commit,
    e.is_tombstone
   FROM (entries_v e
     JOIN lineage_v l ON ((l.ancestor_branch = e.branch_id)))
  WHERE ((l.main_branch OR ((e.min_commit <= l.effective_commit) AND e.is_committed)) AND l.active_lineage);

CREATE VIEW entries_lineage_committed_v AS
 SELECT t.displayed_branch,
    t.source_branch,
    t.path,
    t.min_commit,
    t.max_commit,
    t.physical_address,
    t.creation_date,
    t.size,
    t.checksum,
    t.metadata,
    t.precedence,
    t.row_no AS rank,
    t.branch_min_commit,
    t.branch_max_commit,
    t.is_committed,
    t.is_deleted,
    t.active_lineage,
    t.effective_commit,
    t.is_tombstone
   FROM ( SELECT ef.displayed_branch,
            ef.source_branch,
            ef.path,
            ef.min_commit,
            ef.max_commit,
            ef.physical_address,
            ef.creation_date,
            ef.size,
            ef.checksum,
            ef.metadata,
            ef.precedence,
            row_number() OVER (PARTITION BY ef.displayed_branch, ef.path ORDER BY ef.rank) AS row_no,
            ef.rank,
            ef.branch_min_commit,
            ef.branch_max_commit,
            ef.is_committed,
            ef.is_deleted,
            ef.active_lineage,
            ef.effective_commit,
            ef.is_tombstone
           FROM entries_lineage_full_v ef
          WHERE ef.is_committed) t
  WHERE (t.row_no = 1);

CREATE VIEW entries_lineage_v AS
 SELECT t.displayed_branch,
    t.source_branch,
    t.path,
    t.min_commit,
    t.max_commit,
    t.physical_address,
    t.creation_date,
    t.size,
    t.checksum,
    t.metadata,
    t.precedence,
    t.rank,
    t.branch_min_commit,
    t.branch_max_commit,
    t.is_committed,
    t.is_deleted,
    t.active_lineage,
    t.effective_commit,
    t.is_tombstone
   FROM entries_lineage_full_v t
  WHERE (t.rank = 1);

CREATE TABLE multipart_uploads (
    repository_id integer NOT NULL,
    upload_id character varying NOT NULL,
    path character varying NOT NULL,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    physical_address character varying
);

CREATE TABLE object_dedup (
    repository_id integer NOT NULL,
    dedup_id bytea NOT NULL,
    physical_address character varying(64) NOT NULL
);

CREATE TABLE repositories (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    storage_namespace character varying NOT NULL,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    default_branch integer NOT NULL,
    deleted boolean DEFAULT false NOT NULL
);

CREATE SEQUENCE repositories_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE VIEW top_committed_entries_v AS
 SELECT t.branch_id,
    t.path,
    t.physical_address,
    t.creation_date,
    t.size,
    t.checksum,
    t.metadata,
    t.min_commit,
    t.max_commit,
    t.is_committed,
    t.is_deleted,
    t.is_tombstone,
    t.rank
   FROM ( SELECT e.branch_id,
            e.path,
            e.physical_address,
            e.creation_date,
            e.size,
            e.checksum,
            e.metadata,
            e.min_commit,
            e.max_commit,
            e.is_committed,
            e.is_deleted,
            e.is_tombstone,
            row_number() OVER (PARTITION BY e.branch_id, e.path ORDER BY
                CASE
                    WHEN (e.min_commit = 0) THEN ('01111111111111111111111111111111'::"bit")::integer
                    ELSE e.min_commit
                END DESC) AS rank
           FROM entries_v e
          WHERE e.is_committed) t
  WHERE (t.rank = 1);

ALTER TABLE ONLY branches
    ADD CONSTRAINT branches_pk PRIMARY KEY (id);

ALTER TABLE ONLY commits
    ADD CONSTRAINT commits_pkey PRIMARY KEY (branch_id, commit_id);

ALTER TABLE ONLY entries
    ADD CONSTRAINT entries_pk PRIMARY KEY (branch_id, path, min_commit) INCLUDE (max_commit);

ALTER TABLE ONLY lineage
    ADD CONSTRAINT lineage_pk PRIMARY KEY (branch_id, ancestor_branch, min_commit);

ALTER TABLE ONLY lineage
    ADD CONSTRAINT lineage_precedence_uindex UNIQUE (branch_id, precedence, min_commit);

ALTER TABLE ONLY multipart_uploads
    ADD CONSTRAINT multipart_uploads_pkey PRIMARY KEY (upload_id);

ALTER TABLE ONLY object_dedup
    ADD CONSTRAINT object_dedup_pkey PRIMARY KEY (repository_id, dedup_id);

ALTER TABLE ONLY repositories
    ADD CONSTRAINT repositories_pk PRIMARY KEY (id);

CREATE UNIQUE INDEX branches_repository_name_uindex ON branches USING btree (name, repository_id);

CREATE INDEX entries_path_trgm ON entries USING gin (path public.gin_trgm_ops);

CREATE INDEX fki_branch_repository_fk ON branches USING btree (repository_id);

CREATE INDEX fki_entries_branch_const ON entries USING btree (branch_id);

CREATE INDEX fki_repositories_branches_id_fkey ON repositories USING btree (default_branch);

CREATE UNIQUE INDEX repositories_name_uindex ON repositories USING btree (name);

ALTER TABLE ONLY branches
    ADD CONSTRAINT branch_repository_fk FOREIGN KEY (repository_id) REFERENCES repositories(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE ONLY commits
    ADD CONSTRAINT commits_branches_repository_id_fk FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE;

ALTER TABLE ONLY entries
    ADD CONSTRAINT entries_branch_const FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE;

ALTER TABLE ONLY lineage
    ADD CONSTRAINT lineage_branches_repository_id_fk FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE;

ALTER TABLE ONLY lineage
    ADD CONSTRAINT lineage_branches_repository_id_fk_2 FOREIGN KEY (ancestor_branch) REFERENCES branches(id) ON DELETE CASCADE;

ALTER TABLE ONLY multipart_uploads
    ADD CONSTRAINT multipart_uploads_repository_id_fkey FOREIGN KEY (repository_id) REFERENCES repositories(id) ON DELETE CASCADE;

ALTER TABLE ONLY object_dedup
    ADD CONSTRAINT object_dedup_repository_id_fkey FOREIGN KEY (repository_id) REFERENCES repositories(id) ON DELETE CASCADE;

ALTER TABLE ONLY repositories
    ADD CONSTRAINT repositories_branches_id_fkey FOREIGN KEY (default_branch) REFERENCES branches(id) DEFERRABLE INITIALLY DEFERRED NOT VALID;

