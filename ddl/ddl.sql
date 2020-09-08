CREATE EXTENSION pgcrypto;

CREATE SCHEMA IF NOT EXISTS parade;

-- (requires extensioon pgcrypto)

CREATE TYPE task_status_code_value AS ENUM (
    'pending',		-- waiting for an actor to perform it (new or being retried)
    'in-progress',		-- task is being performed by an actor
    'aborted',		-- an actor has aborted this task with message, will not be reissued
    'completed'		-- an actor has completed this task with message, will not reissued
);

CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(64) NOT NULL PRIMARY KEY, -- nanoid

    action VARCHAR(128) NOT NULL, -- name (type) of action to perform
    body TEXT,			-- data used by action
    status TEXT,		-- status text defined by action, visible to action

    status_code TASK_STATUS_CODE_VALUE NOT NULL DEFAULT 'pending', -- internal status code, used by parade to issue tasks
    num_tries INTEGER NOT NULL DEFAULT 0,
    max_tries INTEGER,

    total_dependencies INTEGER,	-- number of tasks that must signal this task
    num_signals INTEGER NOT NULL DEFAULT 0, -- number of tasks that have already signalled this task

    -- BUG(ariels): add REFERENCES dependency to each of the to_signal
    --     tasks.  Or at least add triggers that perform ON DELETE
    --     CASCADE.
    to_signal VARCHAR(64) ARRAY, -- IDs to signal after performing this task

    actor_id VARCHAR(64),    -- ID of performing actor if in-progress
    action_deadline TIMESTAMPTZ, -- offer this task to other actors once action_deadline has elapsed
    performance_token UUID,
    finish_channel VARCHAR(64) -- (if non-NULL) name of a channel to NOTIFY when this task ends
-- TODO(ariels): add a lock token to each row, set when leasing the
--     task, and accept task completion only when lock token is unchanged.
);

-- Returns true if task with this id, code and deadline can
-- be allocated.
CREATE OR REPLACE FUNCTION can_allocate_task(id VARCHAR(64), code TASK_STATUS_CODE_VALUE, deadline TIMESTAMPTZ, num_signals INTEGER, total_dependencies INTEGER)
RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$
    SELECT (code = 'pending' OR (code = 'in-progress' AND deadline < NOW())) AND
        (total_dependencies IS NULL OR num_signals = total_dependencies)
$$;

-- Marks up to `max_tasks' on one of `actions' as in-progress and
-- belonging to `actor_id' and returns their ids and a "performance
-- token".  Both must be returned to complete the task successfully.
CREATE OR REPLACE FUNCTION own_tasks(
    max_tasks INTEGER, actions VARCHAR(128) ARRAY, owner_id VARCHAR(64), max_duration INTERVAL
)
RETURNS TABLE(task_id VARCHAR(64), token UUID, body TEXT)
LANGUAGE sql VOLATILE AS $$
    UPDATE tasks
    SET actor_id = owner_id,
        status_code = 'in-progress',
	num_tries = num_tries + 1,
	performance_token = gen_random_uuid(),
	action_deadline = NOW() + max_duration -- NULL if max_duration IS NULL
    WHERE id IN (
        SELECT id
	FROM tasks
        WHERE can_allocate_task(id, status_code, action_deadline, num_signals, total_dependencies) AND
	    action = ANY(actions) AND
	    (max_tries IS NULL OR num_tries < max_tries)
	-- maybe: AND not_before <= NOW()
	-- maybe: ORDER BY priority (eventually)
	FOR NO KEY UPDATE SKIP LOCKED
	LIMIT max_tasks)
    RETURNING id, performance_token, body
$$;

-- Returns an owned task id that was locked with token.  It is an error
-- to return a task with the wrong token; that can happen if the
-- deadline expired and the task was given to another actor.
CREATE OR REPLACE FUNCTION return_task(
    task_id VARCHAR(64), token UUID, result_status TEXT, result_status_code TASK_STATUS_CODE_VALUE
) RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    num_updated INTEGER;
    channel VARCHAR(64);
    tasks_to_signal VARCHAR(64) ARRAY;
BEGIN
    UPDATE tasks INTO channel, tasks_to_signal
    SET status = result_status,
    	status_code = result_status_code,
	actor_id = NULL,
	performance_token = NULL
    WHERE id = task_id AND performance_token = token
    RETURNING finish_channel, to_signal;

    GET DIAGNOSTICS num_updated := ROW_COUNT;

    IF channel IS NOT NULL THEN
        SELECT pg_notify(channel, NULL);
    END IF;

    UPDATE tasks
    SET num_signals = num_signals+1
    WHERE id = ANY(tasks_to_signal);

    RETURN num_updated;
END;
$$;
