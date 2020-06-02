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
    metadata json,
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
    metadata json,
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
    (entries.max_commit <> 2147483647) AS is_deleted,
    (entries.max_commit < entries.min_commit) AS is_tumbstone
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
    (lineage.max_commit = 2147483647) AS active_lineage
   FROM lineage
UNION ALL
 SELECT branches.id AS branch_id,
    true AS main_branch,
    0 AS precedence,
    branches.id AS ancestor_branch,
    2147483647 AS effective_commit,
    0 AS min_commit,
    2147483647 AS max_commit,
    true AS active_lineage
   FROM branches;

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
    t.is_tumbstone
   FROM ( SELECT l.branch_id AS displayed_branch,
            e.branch_id AS source_branch,
            e.path,
            e.min_commit,
            e.max_commit,
            e.physical_address,
            e.creation_date,
            e.size,
            e.checksum,
            e.metadata,
            l.precedence,
            rank() OVER (PARTITION BY l.branch_id, e.path ORDER BY l.precedence,
                CASE
                    WHEN (l.main_branch AND (e.min_commit = 0)) THEN 2147483647
                    ELSE e.min_commit
                END DESC) AS rank,
            l.min_commit AS branch_min_commit,
            l.max_commit AS branch_max_commit,
            e.is_deleted,
            e.is_committed,
            e.is_tumbstone,
            l.active_lineage
           FROM (entries_v e
             JOIN lineage_v l ON ((l.ancestor_branch = e.branch_id)))
          WHERE ((l.main_branch OR (e.min_commit <= l.effective_commit)) AND (l.max_commit = 2147483647))) t
  WHERE (t.rank = 1);

CREATE VIEW entries_lineage_active_v AS
 SELECT entries_lineage_v.displayed_branch,
    entries_lineage_v.source_branch,
    entries_lineage_v.path,
    entries_lineage_v.min_commit,
    entries_lineage_v.max_commit,
    entries_lineage_v.physical_address,
    entries_lineage_v.creation_date,
    entries_lineage_v.size,
    entries_lineage_v.checksum,
    entries_lineage_v.metadata,
    entries_lineage_v.precedence,
    entries_lineage_v.rank,
    entries_lineage_v.branch_min_commit,
    entries_lineage_v.branch_max_commit,
    entries_lineage_v.is_committed,
    entries_lineage_v.is_deleted,
    entries_lineage_v.is_tumbstone
   FROM entries_lineage_v
  WHERE ((NOT entries_lineage_v.is_deleted) AND entries_lineage_v.active_lineage);

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

ALTER TABLE ONLY branches
    ADD CONSTRAINT branches_pk PRIMARY KEY (id);

ALTER TABLE ONLY commits
    ADD CONSTRAINT commits_pkey PRIMARY KEY (branch_id, commit_id);

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

CREATE UNIQUE INDEX entries_path_index ON entries USING btree (branch_id, path, min_commit) INCLUDE (max_commit);

CREATE INDEX entries_path_trgm ON entries USING gin (path public.gin_trgm_ops);

CREATE INDEX fki_entries_branch_const ON entries USING btree (branch_id);

CREATE INDEX fki_repositories_branches_id_fkey ON repositories USING btree (default_branch);

CREATE UNIQUE INDEX repositories_name_uindex ON repositories USING btree (name);

ALTER TABLE ONLY branches
    ADD CONSTRAINT branches_repository_id_fkey FOREIGN KEY (repository_id) REFERENCES repositories(id) DEFERRABLE;

ALTER TABLE ONLY commits
    ADD CONSTRAINT commits_branches_repository_id_fk FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE CASCADE;

ALTER TABLE ONLY entries
    ADD CONSTRAINT entries_branch_const FOREIGN KEY (branch_id) REFERENCES branches(id);

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

