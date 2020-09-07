alter table catalog_entries drop constraint catalog_entries_pk if exists

drop index catalog_fki_entries_branches_fk if exists