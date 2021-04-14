BEGIN;
ALTER TABLE graveler_commits
    DROP COLUMN IF EXISTS version;
COMMIT;