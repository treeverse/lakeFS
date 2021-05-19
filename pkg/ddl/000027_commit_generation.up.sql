BEGIN;
    ALTER TABLE graveler_commits
        ADD COLUMN generation INT;
COMMIT;
