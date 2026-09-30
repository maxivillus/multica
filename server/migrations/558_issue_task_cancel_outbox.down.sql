DROP TRIGGER IF EXISTS issue_task_cancel_outbox_enqueue ON issue;
DROP FUNCTION IF EXISTS enqueue_issue_task_cancellations();
DROP TABLE IF EXISTS issue_task_cancel_outbox;
