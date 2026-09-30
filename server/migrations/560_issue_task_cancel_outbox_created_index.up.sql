-- Keep the hot outbox table available while its worker-ordering index builds.
CREATE INDEX CONCURRENTLY IF NOT EXISTS issue_task_cancel_outbox_created_at_idx
    ON issue_task_cancel_outbox (created_at, task_id);
