CREATE INDEX CONCURRENTLY IF NOT EXISTS card_session_open_token_stats_due_idx
    ON card_session (last_token_stats_at NULLS FIRST, id)
    WHERE state = 'open';
