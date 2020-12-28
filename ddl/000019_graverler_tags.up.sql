BEGIN;
CREATE TABLE IF NOT EXISTS graveler_tags
(
    repository_id text NOT NULL,
    id            text NOT NULL,

    commit_id     text,

    PRIMARY KEY (repository_id, id)
);
COMMIT;
