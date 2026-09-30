-- Keep issue cancellation durable across a process crash or a transient
-- database error between the issue status write and task cancellation.
CREATE TABLE issue_task_cancel_outbox (
    issue_id UUID NOT NULL,
    task_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION enqueue_issue_task_cancellations()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    IF issue_effective_status(NEW.workspace_id, NEW.status) = 'cancelled'
       AND issue_effective_status(OLD.workspace_id, OLD.status) <> 'cancelled' THEN
        INSERT INTO issue_task_cancel_outbox (issue_id, task_id)
        SELECT NEW.id, task.id
        FROM agent_task_queue AS task
        WHERE task.issue_id = NEW.id
          AND task.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
        ON CONFLICT (task_id) DO NOTHING;
    END IF;

    RETURN NEW;
END;
$function$;

CREATE TRIGGER issue_task_cancel_outbox_enqueue
    AFTER UPDATE OF status ON issue
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION enqueue_issue_task_cancellations();

-- Repair active work already attached to a cancelled card before this
-- migration was installed.
INSERT INTO issue_task_cancel_outbox (issue_id, task_id)
SELECT issue.id, task.id
FROM issue
JOIN agent_task_queue AS task ON task.issue_id = issue.id
WHERE issue_effective_status(issue.workspace_id, issue.status) = 'cancelled'
  AND task.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
ON CONFLICT (task_id) DO NOTHING;
