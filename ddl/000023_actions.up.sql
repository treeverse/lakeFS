BEGIN;

CREATE TABLE IF NOT EXISTS actions_runs
(
    repository_id       text NOT NULL,
    run_id              text NOT NULL,

    event_type          text NOT NULL,
    start_time          timestamptz NOT NULL,
    end_time            timestamptz NOT NULL,
    branch_id           text NOT NULL,
    source_ref          text NOT NULL,
    commit_id           text NOT NULL,
    passed              BOOLEAN DEFAULT false NOT NULL,

    PRIMARY KEY (repository_id, run_id)
);


CREATE TABLE IF NOT EXISTS actions_run_hooks
(
    repository_id       text NOT NULL,
    run_id              text NOT NULL,
    action_name         text NOT NULL,
    hook_id             text NOT NULL,

    event_type          text NOT NULL,
    start_time          timestamptz NOT NULL,
    end_time            timestamptz NOT NULL,
    passed              BOOLEAN DEFAULT false NOT NULL,

    PRIMARY KEY (repository_id, run_id, action_name, hook_id)
);

COMMIT;
