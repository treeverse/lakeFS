BEGIN;
ALTER TYPE catalog_merge_type RENAME VALUE 'from_parent' TO 'from_father';
ALTER TYPE catalog_merge_type RENAME VALUE 'from_child' TO 'from_son';
COMMIT;