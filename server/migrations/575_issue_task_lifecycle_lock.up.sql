-- Serialize issue task creation with status transitions. The task enqueue path
-- takes this transaction-scoped lock before re-reading the effective status;
-- the BEFORE trigger takes it for every direct or application status update.
CREATE OR REPLACE FUNCTION lock_issue_task_lifecycle(p_issue_id UUID)
RETURNS VOID
LANGUAGE sql
VOLATILE
AS $function$
    SELECT pg_advisory_xact_lock(
        hashtextextended('multica.issue-task-lifecycle:' || p_issue_id::text, 0)
    )
$function$;

CREATE OR REPLACE FUNCTION lock_issue_task_lifecycle_before_status_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    PERFORM lock_issue_task_lifecycle(NEW.id);
    RETURN NEW;
END;
$function$;

-- Keep every legacy issue-task writer in the same order as status changes:
-- lifecycle advisory lock first, then the workspace and owner-row fence.
CREATE OR REPLACE FUNCTION lock_task_owner_rows(
    p_agent_id uuid,
    p_issue_id uuid,
    p_runtime_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    required int := (CASE WHEN p_agent_id IS NULL THEN 0 ELSE 1 END)
                  + (CASE WHEN p_issue_id IS NULL THEN 0 ELSE 1 END)
                  + (CASE WHEN p_runtime_id IS NULL THEN 0 ELSE 1 END);
    resolved int;
    distinct_workspaces int;
    locked int;
BEGIN
    IF required = 0 THEN
        RETURN TRUE;
    END IF;

    IF p_issue_id IS NOT NULL THEN
        PERFORM lock_issue_task_lifecycle(p_issue_id);
    END IF;

    WITH owners AS (
        SELECT a.workspace_id FROM agent a WHERE a.id = p_agent_id
        UNION ALL
        SELECT i.workspace_id FROM issue i WHERE i.id = p_issue_id
        UNION ALL
        SELECT r.workspace_id FROM agent_runtime r WHERE r.id = p_runtime_id
    )
    SELECT count(*), count(DISTINCT workspace_id)
    INTO resolved, distinct_workspaces
    FROM owners;

    IF resolved <> required THEN
        RETURN FALSE;
    END IF;

    WITH locked_workspaces AS (
        SELECT w.id
        FROM workspace w
        WHERE w.id IN (
            SELECT a.workspace_id FROM agent a WHERE a.id = p_agent_id
            UNION
            SELECT i.workspace_id FROM issue i WHERE i.id = p_issue_id
            UNION
            SELECT r.workspace_id FROM agent_runtime r WHERE r.id = p_runtime_id
        )
        ORDER BY w.id
        FOR KEY SHARE
    )
    SELECT count(*) INTO locked FROM locked_workspaces;

    IF locked <> distinct_workspaces THEN
        RETURN FALSE;
    END IF;

    locked := 0;
    IF p_agent_id IS NOT NULL THEN
        PERFORM 1 FROM agent WHERE id = p_agent_id FOR KEY SHARE;
        IF FOUND THEN locked := locked + 1; END IF;
    END IF;
    IF p_issue_id IS NOT NULL THEN
        PERFORM 1 FROM issue WHERE id = p_issue_id FOR KEY SHARE;
        IF FOUND THEN locked := locked + 1; END IF;
    END IF;
    IF p_runtime_id IS NOT NULL THEN
        PERFORM 1 FROM agent_runtime WHERE id = p_runtime_id FOR KEY SHARE;
        IF FOUND THEN locked := locked + 1; END IF;
    END IF;

    RETURN locked = required;
END;
$$;

-- The service guard gives current writers a useful error. This database guard
-- also protects older application instances and direct queue writes during a
-- rolling deploy. It only blocks active tasks on closed or unknown statuses.
CREATE OR REPLACE FUNCTION guard_issue_task_lifecycle()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE
    owner_workspace_id UUID;
    owner_status TEXT;
    owner_category TEXT;
BEGIN
    IF NEW.issue_id IS NULL
       OR NEW.status NOT IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred') THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.issue_id IS NOT DISTINCT FROM OLD.issue_id THEN
        RETURN NEW;
    END IF;

    PERFORM lock_issue_task_lifecycle(NEW.issue_id);

    SELECT issue.workspace_id, issue.status
    INTO owner_workspace_id, owner_status
    FROM issue
    WHERE issue.id = NEW.issue_id
    FOR KEY SHARE;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    IF owner_status IN ('backlog', 'todo', 'in_progress', 'in_review', 'done', 'blocked', 'cancelled') THEN
        IF owner_status = 'cancelled' THEN
            RETURN NULL;
        END IF;
        RETURN NEW;
    END IF;

    SELECT status.category
    INTO owner_category
    FROM issue_status AS status
    WHERE status.workspace_id = owner_workspace_id
      AND status.key = owner_status;

    IF NOT FOUND OR owner_category = 'closed' THEN
        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$function$;

CREATE TRIGGER issue_task_lifecycle_lock
    BEFORE UPDATE OF status ON issue
    FOR EACH ROW
    WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION lock_issue_task_lifecycle_before_status_change();

CREATE TRIGGER agent_task_queue_issue_lifecycle_guard
    BEFORE INSERT OR UPDATE OF issue_id ON agent_task_queue
    FOR EACH ROW
    EXECUTE FUNCTION guard_issue_task_lifecycle();

-- Close the deployment gap between the earlier outbox migration and this
-- lifecycle guard. An older application instance could have enqueued a task
-- after migration 558's backfill but before this trigger was installed.
INSERT INTO issue_task_cancel_outbox (issue_id, task_id)
SELECT issue.id, task.id
FROM issue
JOIN agent_task_queue AS task ON task.issue_id = issue.id
WHERE issue_effective_status(issue.workspace_id, issue.status) = 'cancelled'
  AND task.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
ON CONFLICT (task_id) DO NOTHING;
