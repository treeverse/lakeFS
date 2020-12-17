BEGIN;
ALTER TABLE graveler_staging_kv ALTER COLUMN identity SET NOT NULL;
ALTER TABLE graveler_staging_kv RENAME TO kv_staging;
COMMIT;
