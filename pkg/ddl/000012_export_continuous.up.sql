BEGIN;

ALTER TABLE catalog_branches_export ADD COLUMN IF NOT EXISTS continuous BOOLEAN;
UPDATE catalog_branches_export SET continuous=false;
ALTER TABLE catalog_branches_export ALTER COLUMN continuous SET NOT NULL;

END;
