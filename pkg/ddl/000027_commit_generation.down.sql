BEGIN;
    ALTER TABLE graveler_commits
        DROP COLUMN generation;
COMMIT;

