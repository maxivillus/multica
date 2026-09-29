CREATE INDEX CONCURRENTLY IF NOT EXISTS card_session_issue_idx
    ON card_session (issue_id, agent_id, generation DESC);
