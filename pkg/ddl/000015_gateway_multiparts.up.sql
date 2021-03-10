BEGIN;

CREATE TABLE IF NOT EXISTS gateway_multiparts (
    upload_id character varying NOT NULL,
    path character varying COLLATE "C" NOT NULL,
    creation_date timestamp with time zone DEFAULT now() NOT NULL,
    physical_address character varying
);

ALTER TABLE ONLY gateway_multiparts
    ADD CONSTRAINT gateway_multiparts_pk PRIMARY KEY (upload_id);

DROP TABLE IF EXISTS catalog_multipart_uploads;

COMMIT;