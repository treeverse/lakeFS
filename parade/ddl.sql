CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE task_status_code_value AS ENUM (
    'pending',          -- waiting for an actor to perform it (new or being retried)
    'in-progress',      -- task is being performed by an actor
    'aborted',          -- an actor has aborted this task with message, will not be reissued
    'completed'         -- an actor has completed this task with message, will not reissued
);

CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(64) NOT NULL PRIMARY KEY, -- nanoid

    action VARCHAR(128) NOT NULL, -- name (type) of action to perform
    body TEXT,                  -- data used by action
    status TEXT,                -- status text defined by action, visible to action

    status_code task_status_code_value NOT NULL DEFAULT 'pending', -- internal status code, used by parade to issue tasks

    num_tries INTEGER NOT NULL DEFAULT 0, -- number of attempts actors have made on this task
    max_tries INTEGER,

    total_dependencies INTEGER, -- number of tasks that must signal this task
    num_signals INTEGER NOT NULL DEFAULT 0, -- number of tasks that have already signalled this task

    actor_id VARCHAR(64),    -- ID of performing actor if in-progress
    action_deadline TIMESTAMPTZ, -- offer this task to other actors once action_deadline has elapsed
    performance_token UUID,

    -- BUG(ariels): add REFERENCES dependency to each of the to_signal_after
    --     tasks.  Or at least add triggers that perform ON DELETE
    --     CASCADE.
    to_signal_after VARCHAR(64) ARRAY, -- IDs to signal after performing this task
    notify_channel_after VARCHAR(64) -- (if non-NULL) name of a channel to NOTIFY when this task ends
);

-- Returns true if task with this id, code and deadline can
-- be allocated.
CREATE OR REPLACE FUNCTION can_allocate_task(id VARCHAR(64), code task_status_code_value, deadline TIMESTAMPTZ, num_signals INTEGER, total_dependencies INTEGER)
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
RETURNS TABLE(task_id VARCHAR(64), token UUID, action VARCHAR(128), body TEXT)
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
        ORDER BY random()
        FOR UPDATE SKIP LOCKED
        LIMIT max_tasks)
    RETURNING id, performance_token, action, body
$$;

-- Extends ownership of task id by an extra max_duration, if it is still locked with performance
-- token.
CREATE OR REPLACE FUNCTION extend_task_deadline(
    task_id VARCHAR(64), token UUID, max_duration INTERVAL
) RETURNS BOOLEAN
LANGUAGE sql AS $$
    UPDATE tasks
    SET action_deadline = NOW() + max_duration
    WHERE id = task_id AND performance_token = token
    RETURNING true;
$$;

-- Returns an owned task id that was locked with token.  It is an error
-- to return a task with the wrong token; that can happen if the
-- deadline expired and the task was given to another actor.
CREATE OR REPLACE FUNCTION return_task(
    task_id VARCHAR(64), token UUID, result_status TEXT, result_status_code task_status_code_value
) RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE
    num_updated INTEGER;
    channel VARCHAR(64);
    tasks_to_signal_after VARCHAR(64) ARRAY;
BEGIN
    UPDATE tasks INTO channel, tasks_to_signal_after
    SET status = result_status,
        status_code = result_status_code,
        actor_id = NULL,
        performance_token = NULL
    WHERE id = task_id AND performance_token = token
    RETURNING notify_channel_after, to_signal_after;

    GET DIAGNOSTICS num_updated := ROW_COUNT;

    UPDATE tasks
    SET num_signals = num_signals+1
    WHERE id = ANY(tasks_to_signal_after);

    IF channel IS NOT NULL THEN
        PERFORM pg_notify(channel, NULL);
    END IF;


    RETURN num_updated;
END;
$$;

-- (Utility for delete_task function: remove all dependencies from task ID, returning ids of any
-- tasks with no remaining dependencies.)
CREATE OR REPLACE FUNCTION remove_task_dependencies(task_id VARCHAR(64))
RETURNS SETOF VARCHAR(64)
LANGUAGE sql AS $$
WITH updates AS (
        SELECT UNNEST(to_signal_after) effect_id,
            (CASE WHEN status_code IN ('aborted', 'completed') THEN 0 ELSE 1 END) delta
        FROM tasks
        WHERE id=task_id),
    signalled_ids AS (
        UPDATE tasks
        SET total_dependencies = total_dependencies-updates.delta
        FROM updates
        WHERE id=updates.effect_id
	RETURNING (CASE WHEN total_dependencies = num_signals THEN tasks.id ELSE NULL END) id
)
SELECT id FROM signalled_ids WHERE id IS NOT NULL;
$$;

CREATE TYPE tasks_recurse_value AS ENUM ('new', 'in-progress', 'done');

-- Deletes taskIds from column id of task_id_name (with columns id (an ID) and mark (a
-- recurse_value), presumably a temporary table) and empties it, decrements each of its
-- dependent tasks, and deletes that task (effectively recursively) if it has no further
-- dependencies.  Uses table tasks for storage of to-be-deleted tasks during the operation.
-- Returns the total number of tasks deleted.  No abort marking is performed -- make sure to
-- abort the task first!
CREATE OR REPLACE FUNCTION delete_tasks(task_id_name TEXT) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    total_num_updated INTEGER;
    num_updated INTEGER;
    row_count INTEGER;
BEGIN
    LOOP
        EXECUTE format($Q$
            UPDATE %1$I SET mark='in-progress' WHERE mark='new'
        $Q$, task_id_name);
        EXECUTE format($Q$
            WITH new_to_delete AS (
                SELECT remove_task_dependencies(id) id FROM %1$I WHERE mark='in-progress'
            )
            INSERT INTO %1$I (SELECT id, 'new' mark FROM new_to_delete)
        $Q$, task_id_name);
        GET DIAGNOSTICS row_count = ROW_COUNT;
        EXIT WHEN row_count=0;
        EXECUTE format($Q$
            UPDATE %1$I SET mark='done' WHERE mark='in-progress'
        $Q$, task_id_name);
    END LOOP;
    EXECUTE format($Q$
        DELETE FROM tasks WHERE id IN (SELECT id FROM %1$I)
    $Q$, task_id_name);
END;
$$;
