-- A durable publication watermark makes the periodic token snapshot
-- idempotent across multiple server sweepers. It is separate from
-- last_activity_at because provider activity must not change the reporting
-- cadence.
ALTER TABLE card_session
ADD COLUMN last_token_stats_at TIMESTAMPTZ;

CREATE INDEX card_session_open_token_stats_due_idx
    ON card_session (last_token_stats_at NULLS FIRST, id)
    WHERE state = 'open';
