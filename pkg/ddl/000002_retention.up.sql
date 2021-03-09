BEGIN;

ALTER TABLE catalog_object_dedup
    DROP CONSTRAINT IF EXISTS catalog_object_dedup_pk,
    ADD COLUMN IF NOT EXISTS deleting boolean DEFAULT false NOT NULL,
    ADD CONSTRAINT catalog_object_dedup_pk PRIMARY KEY (repository_id, dedup_id, deleting);

COMMENT ON COLUMN catalog_object_dedup.deleting IS $$
Set before deleting an object to indicate it is going to be
removed by an AWS S3 batch operation and cannot be used for new
dedupes.
$$;

END;
