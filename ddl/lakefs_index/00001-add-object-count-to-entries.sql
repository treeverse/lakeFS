-- alter table lakefs_index.entries drop column object_count;
-- alter table lakefs_index.roots drop column object_count;
ALTER TABLE entries ADD COLUMN object_count int not null DEFAULT 0;
ALTER TABLE roots ADD COLUMN object_count int not null DEFAULT 0;