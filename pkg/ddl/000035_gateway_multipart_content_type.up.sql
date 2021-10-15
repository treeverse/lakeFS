ALTER TABLE gateway_multiparts
    ADD COLUMN IF NOT EXISTS metadata jsonb,
    ADD COLUMN IF NOT EXISTS content_type TEXT;
