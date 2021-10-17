ALTER TABLE gateway_multiparts
    DROP COLUMN IF EXISTS metadata,
    DROP COLUMN IF EXISTS content_type;
