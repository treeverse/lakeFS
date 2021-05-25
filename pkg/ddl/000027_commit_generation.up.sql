BEGIN;
    ALTER TABLE graveler_commits
        ADD COLUMN generation INT;
    WITH RECURSIVE cte AS
                       (
                           SELECT id, 1 AS generation,creation_date
                           FROM graveler_commits
                           WHERE parents IS NULL OR array_length(parents,1) = 0
                           UNION ALL
                           (SELECT DISTINCT(c.id), cte.generation+1 AS generation,c.creation_date
                            FROM graveler_commits c  INNER JOIN cte ON cte.id = ANY(c.parents))
                       )
    UPDATE graveler_commits u SET generation = cte.generation FROM cte WHERE u.id = cte.id;
COMMIT;
