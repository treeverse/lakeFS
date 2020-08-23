-- block lakeFS
BEGIN;

DROP TABLE IF EXISTS block_multipart_uploads_parts;
DROP TABLE IF EXISTS block_multipart_uploads;

COMMIT;