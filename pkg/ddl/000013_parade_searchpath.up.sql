-- Marks up to `max_tasks' on one of `actions' as in-progress and
-- belonging to `actor_id' and returns their ids and a "performance
-- token".  Both must be returned to complete the task successfully.
CREATE OR REPLACE FUNCTION own_tasks(
    max_tasks INTEGER, actions VARCHAR ARRAY, owner_id VARCHAR, max_duration INTERVAL
)
RETURNS TABLE(task_id VARCHAR, token UUID, num_failures INTEGER, action VARCHAR, body TEXT)
LANGUAGE sql VOLATILE AS $$
    UPDATE tasks
    SET actor_id = owner_id,
        status_code = 'in-progress',
        num_tries = num_tries + 1,
        performance_token = public.gen_random_uuid(),
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
    RETURNING id, performance_token, num_failures, action, body
$$;
