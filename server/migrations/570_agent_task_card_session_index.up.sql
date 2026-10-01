CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_task_queue_card_session_idx
    ON agent_task_queue (card_session_id, id)
    WHERE card_session_id IS NOT NULL;
