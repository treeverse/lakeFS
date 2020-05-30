CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;

CREATE SEQUENCE IF NOT EXISTS branches_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


CREATE TABLE IF NOT EXISTS branches (
                          repository_id integer NOT NULL,
                          id integer DEFAULT nextval('branches_id_seq'::regclass) NOT NULL,
                          name character varying(64) NOT NULL,
                          next_commit integer DEFAULT 1 NOT NULL
);



CREATE TABLE IF NOT EXISTS commits (
                         branch_id integer NOT NULL,
                         commit_number integer NOT NULL,
                         committer character varying,
                         message character varying,
                         creation_date timestamptz with time zone DEFAULT now() NOT NULL,
                         metadata json,
                         source_branch integer
);



CREATE TABLE IF NOT EXISTS entries (
                         branch_id integer NOT NULL,
                         key character varying NOT NULL,
                         physical_address character varying(64),
                         creation_date timestamptz with time zone DEFAULT now() NOT NULL,
                         size bigint NOT NULL,
                         checksum character varying(64) NOT NULL,
                         metadata json,
                         is_staged boolean DEFAULT false,
                         min_commit integer DEFAULT 0 NOT NULL,
                         max_commit integer DEFAULT ('01111111111111111111111111111111'::"bit")::integer NOT NULL,
                         CONSTRAINT entries_staged_constraint CHECK (((is_staged IS NULL) OR (min_commit = 0)))
);



CREATE TABLE IF NOT EXISTS lineage (
                         branch_id integer NOT NULL,
                         precedence integer NOT NULL,
                         ancestor_branch integer NOT NULL,
                         effective_commit integer NOT NULL,
                         min_commit integer,
                         max_commit integer
);



CREATE VIEW lineage_v AS
SELECT lineage.branch_id,
       false AS main_branch,
       lineage.precedence,
       lineage.ancestor_branch,
       lineage.effective_commit,
       lineage.min_commit,
       lineage.max_commit
FROM lineage
UNION ALL
SELECT branches.id AS branch_id,
       true AS main_branch,
       0 AS precedence,
       branches.id AS ancestor_branch,
       1000000000 AS effective_commit,
       0 AS min_commit,
       1000000000 AS max_commit
FROM branches;



CREATE VIEW entries_lineage_v AS
SELECT t.displayed_branch,
       t.source_branch,
       t.key,
       t.min_commit,
       t.max_commit,
       t.precedence,
       t.rank,
       t.branch_min_commit,
       t.branch_max_commit
FROM ( SELECT l.branch_id AS displayed_branch,
              e.branch_id AS source_branch,
              e.key,
              e.min_commit,
              e.max_commit,
              l.precedence,
              rank() OVER (PARTITION BY l.branch_id, e.key ORDER BY l.precedence,
                  CASE
                      WHEN (l.main_branch AND (e.min_commit IS NULL)) THEN 1000000000
                      ELSE COALESCE(e.min_commit, 0)
                      END DESC) AS rank,
              l.min_commit AS branch_min_commit,
              l.max_commit AS branch_max_commit
       FROM (entries e
                JOIN lineage_v l ON ((l.ancestor_branch = e.branch_id)))
       WHERE ((l.main_branch OR (e.min_commit <= l.effective_commit)) AND (l.max_commit IS NULL))) t
WHERE (t.rank = 1);



CREATE VIEW entries_lineage_active_v AS
SELECT entries_lineage_v.displayed_branch,
       entries_lineage_v.source_branch,
       entries_lineage_v.key,
       entries_lineage_v.min_commit,
       entries_lineage_v.max_commit,
       entries_lineage_v.precedence,
       entries_lineage_v.rank,
       entries_lineage_v.branch_min_commit,
       entries_lineage_v.branch_max_commit
FROM entries_lineage_v
WHERE ((entries_lineage_v.max_commit IS NULL) AND (entries_lineage_v.branch_max_commit IS NULL));



CREATE TABLE IF NOT EXISTS multipart_uploads (
                                   repository_id integer NOT NULL,
                                   upload_id character varying NOT NULL,
                                   path character varying NOT NULL,
                                   creation_date timestamptz with time zone DEFAULT now() NOT NULL,
                                   object_name bytea,
                                   physical_address character varying
);



CREATE TABLE IF NOT EXISTS object_dedup (
                              repository_id integer NOT NULL,
                              dedup_id bytea NOT NULL,
                              physical_address character varying(64) NOT NULL,
                              size integer NOT NULL,
                              number_of_parts smallint DEFAULT 1
);



CREATE SEQUENCE IF NOT EXISTS repositories_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



CREATE TABLE IF NOT EXISTS repositories (
                              id integer DEFAULT nextval('repositories_id_seq'::regclass) NOT NULL,
                              name character varying(64) NOT NULL,
                              storage_namespace character varying NOT NULL,
                              creation_date timestamptz with time zone DEFAULT now() NOT NULL,
                              default_branch integer DEFAULT 1 NOT NULL,
                              deleted boolean DEFAULT FALSE NOT NULL
);



ALTER TABLE ONLY branches
    ADD CONSTRAINT branches_pk PRIMARY KEY (id);



ALTER TABLE ONLY commits
    ADD CONSTRAINT commits_pkey PRIMARY KEY (branch_id, commit_number);



ALTER TABLE ONLY multipart_uploads
    ADD CONSTRAINT multipart_uploads_pkey PRIMARY KEY (upload_id);



ALTER TABLE ONLY object_dedup
    ADD CONSTRAINT object_dedup_pkey PRIMARY KEY (repository_id, dedup_id);



ALTER TABLE ONLY repositories
    ADD CONSTRAINT repositories_pk PRIMARY KEY (id);



CREATE UNIQUE INDEX branches_repository_name_uindex ON branches USING btree (name, repository_id);



CREATE INDEX entries_key_index ON entries USING btree (branch_id, key) INCLUDE (min_commit, max_commit);



CREATE INDEX IF NOT EXISTS  entries_key_trgm ON entries USING gin (key public.gin_trgm_ops);



CREATE INDEX fki_entries_branch_const ON entries USING btree (branch_id);



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



