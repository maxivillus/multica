DROP INDEX IF EXISTS card_session_open_token_stats_due_idx;

ALTER TABLE card_session
DROP COLUMN IF EXISTS last_token_stats_at;
