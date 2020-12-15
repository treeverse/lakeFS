BEGIN;
ALTER TABLE kv_staging RENAME TO graveler_staging_kv;
ALTER TABLE graveler_staging_kv ALTER COLUMN identity DROP NOT NULL;
COMMIT;
