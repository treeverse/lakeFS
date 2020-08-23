-- lakeFS block tables
BEGIN;

CREATE TABLE IF NOT EXISTS block_multipart_uploads
(
    storage_namespace character varying NOT NULL,
    upload_id character varying NOT NULL
);

CREATE TABLE IF NOT EXISTS block_multipart_uploads_parts
(
    storage_namespace character varying NOT NULL,
    upload_id character varying NOT NULL,
    number integer NOT NULL,
    etag character varying NOT NULL
);

ALTER TABLE ONLY block_multipart_uploads
    DROP CONSTRAINT IF EXISTS block_multipart_uploads_pk;

ALTER TABLE ONLY block_multipart_uploads
    ADD CONSTRAINT block_multipart_uploads_pk PRIMARY KEY (upload_id);

ALTER TABLE ONLY block_multipart_uploads
    DROP CONSTRAINT IF EXISTS block_multipart_uploads_storage_namespace_fk;

ALTER TABLE ONLY block_multipart_uploads_parts
    DROP CONSTRAINT IF EXISTS block_multipart_uploads_parts_pk;

ALTER TABLE ONLY block_multipart_uploads_parts
    ADD CONSTRAINT block_multipart_uploads_parts_pk PRIMARY KEY (upload_id, number);

ALTER TABLE ONLY block_multipart_uploads_parts
    DROP CONSTRAINT IF EXISTS block_multipart_uploads_parts_upload_id_fk;

ALTER TABLE ONLY block_multipart_uploads_parts
    ADD CONSTRAINT block_multipart_uploads_parts_upload_id_fk FOREIGN KEY (upload_id) REFERENCES block_multipart_uploads(upload_id) ON DELETE CASCADE;

COMMIT;