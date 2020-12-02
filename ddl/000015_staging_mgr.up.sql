BEGIN;
CREATE TABLE IF NOT EXISTS staging_entries
(
    staging_token      varchar                                not null,
    path               varchar                                not null,
    address            varchar                                not null,
    last_modified_date timestamp with time zone default now() not null,
    size               bigint                                 not null,
    e_tag              varchar(64)                            not null,
    metadata           jsonb
) PARTITION BY HASH (staging_token);
CREATE TABLE IF NOT EXISTS staging_entries_p0 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE IF NOT EXISTS staging_entries_p1 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 1);
CREATE TABLE IF NOT EXISTS staging_entries_p2 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 2);
CREATE TABLE IF NOT EXISTS staging_entries_p3 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 3);
CREATE TABLE IF NOT EXISTS staging_entries_p4 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 4);
CREATE TABLE IF NOT EXISTS staging_entries_p5 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 5);
CREATE TABLE IF NOT EXISTS staging_entries_p6 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 6);
CREATE TABLE IF NOT EXISTS staging_entries_p7 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 7);
CREATE TABLE IF NOT EXISTS staging_entries_p8 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 8);
CREATE TABLE IF NOT EXISTS staging_entries_p9 PARTITION OF staging_entries FOR VALUES WITH (MODULUS 10, REMAINDER 9);

CREATE UNIQUE index staging_entries_uidx
    on staging_entries (staging_token asc, path asc);
COMMIT;
