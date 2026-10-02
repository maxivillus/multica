CREATE INDEX CONCURRENTLY IF NOT EXISTS card_session_workspace_activity_idx
    ON card_session (workspace_id, state, last_activity_at);
