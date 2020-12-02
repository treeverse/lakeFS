BEGIN;

CREATE TABLE catalog_multipart_uploads (
        repository_id integer NOT NULL,
        upload_id character varying NOT NULL,
        path character varying COLLATE "C" NOT NULL,
        creation_date timestamp with time zone DEFAULT now() NOT NULL,
        physical_address character varying
);

ALTER TABLE ONLY catalog_multipart_uploads
    ADD CONSTRAINT catalog_multipart_uploads_pk PRIMARY KEY (upload_id);

ALTER TABLE ONLY catalog_multipart_uploads
    ADD CONSTRAINT catalog_multipart_uploads_repository_id_fk FOREIGN KEY (repository_id) REFERENCES catalog_repositories(id) ON DELETE CASCADE;

DROP TABLE IF EXISTS gateway_multiparts;

COMMIT;
