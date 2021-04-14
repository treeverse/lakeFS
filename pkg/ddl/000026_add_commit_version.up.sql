BEGIN;
ALTER TABLE graveler_commits
    ADD COLUMN IF NOT EXISTS version text DEFAULT 'commit:v1' NOT NULL;
COMMIT;