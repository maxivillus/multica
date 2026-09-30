CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS card_session_one_resumable_per_issue_agent
    ON card_session (issue_id, agent_id)
    WHERE state <> 'closed';
