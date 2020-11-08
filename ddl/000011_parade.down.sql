BEGIN;

DROP FUNCTION IF EXISTS delete_tasks;
DROP TYPE IF EXISTS tasks_recurse_value;
DROP FUNCTION IF EXISTS remove_task_dependencies;
DROP FUNCTION IF EXISTS return_task;
DROP FUNCTION IF EXISTS no_more_tries;
DROP FUNCTION IF EXISTS extend_task_deadline;
DROP FUNCTION IF EXISTS own_tasks;
DROP FUNCTION IF EXISTS can_allocate_task;
DROP TABLE IF EXISTS tasks;
DROP TYPE IF EXISTS task_status_code_value;

END;
