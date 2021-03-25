ALTER TABLE catalog_object_dedup
    DROP CONSTRAINT IF EXISTS catalog_object_dedup_pk,
    DROP COLUMN IF EXISTS deleting,
    -- May fail if currently deleting a recreated file.
    ADD CONSTRAINT catalog_object_dedup_pk PRIMARY KEY (repository_id, dedup_id);
