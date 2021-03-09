CREATE TABLE IF NOT EXISTS graveler_repositories
(
    id                text        NOT NULL,

    storage_namespace text        NOT NULL,
    creation_date     timestamptz NOT NULL,
    default_branch    text        NOT NULL,

    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS graveler_branches
(
    repository_id text NOT NULL,
    id            text NOT NULL,

    staging_token text,
    commit_id     text,

    PRIMARY KEY (repository_id, id)
);


CREATE TABLE IF NOT EXISTS graveler_commits
(
    repository_id text        NOT NULL,
    id            text        NOT NULL,

    committer     text        NOT NULL,
    message       text        NOT NULL,
    creation_date timestamptz NOT NULL,
    tree_id       text        NOT NULL,
    metadata      jsonb,
    parents       text[],

    PRIMARY KEY (repository_id, id)
);
