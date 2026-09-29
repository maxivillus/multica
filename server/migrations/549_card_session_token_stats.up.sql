-- A durable publication watermark makes the periodic token snapshot
-- idempotent across multiple server sweepers. It is separate from
-- last_activity_at because provider activity must not change the reporting
-- cadence.
ALTER TABLE card_session
ADD COLUMN last_token_stats_at TIMESTAMPTZ;
