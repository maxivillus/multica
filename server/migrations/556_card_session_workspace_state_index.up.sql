CREATE INDEX CONCURRENTLY IF NOT EXISTS card_session_workspace_state_idx
    ON card_session (workspace_id, state, retain_until);
