BEGIN;
CREATE TABLE IF NOT EXISTS staging_entries
(
    staging_token      varchar                                not null,
    path               varchar                                not null,
    address            varchar                                not null,
    last_modified_date timestamp with time zone default now() not null,
    size               bigint                                 not null,
    checksum           varchar(64)                            not null,
    metadata           jsonb
);

CREATE UNIQUE index staging_entries_uidx
    on staging_entries (staging_token asc, path asc);
COMMIT;
