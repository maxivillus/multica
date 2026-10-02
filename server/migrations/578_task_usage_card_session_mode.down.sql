ALTER TABLE task_usage
    DROP CONSTRAINT IF EXISTS task_usage_card_session_mode_check;

ALTER TABLE task_usage
    DROP COLUMN IF EXISTS card_session_mode;
