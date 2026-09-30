-- Keep the removed user values in private rollback metadata, outside the
-- card_sessions settings consumed by the application and UI. The down
-- migration restores these exact values if this migration is rolled back.
UPDATE workspace AS w
SET settings = jsonb_set(
    jsonb_set(
        COALESCE(w.settings, '{}'::jsonb),
        '{_migration_551_card_session_settings}',
        jsonb_strip_nulls(jsonb_build_object(
            'post_done_retention_hours', CASE
                WHEN (w.settings->'card_sessions') ? 'post_done_retention_hours'
                THEN w.settings->'card_sessions'->'post_done_retention_hours'
            END,
            'token_stats_interval_minutes', CASE
                WHEN (w.settings->'card_sessions') ? 'token_stats_interval_minutes'
                THEN w.settings->'card_sessions'->'token_stats_interval_minutes'
            END
        )),
        true
    ),
    '{card_sessions}',
    (
        CASE
            WHEN NOT ((w.settings->'card_sessions') ? 'idle_timeout_hours')
                 AND (w.settings->'card_sessions') ? 'post_done_retention_hours'
            THEN (w.settings->'card_sessions') || jsonb_build_object(
                'idle_timeout_hours', (w.settings->'card_sessions')->'post_done_retention_hours'
            )
            ELSE w.settings->'card_sessions'
        END
    ) - 'post_done_retention_hours' - 'token_stats_interval_minutes',
    true
)
WHERE jsonb_typeof(w.settings->'card_sessions') = 'object'
  AND (
      (w.settings->'card_sessions') ? 'post_done_retention_hours'
      OR (w.settings->'card_sessions') ? 'token_stats_interval_minutes'
  );

CREATE OR REPLACE FUNCTION issue_status_allows_agent_task(p_workspace_id UUID, p_status TEXT)
RETURNS BOOLEAN
LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
    SELECT CASE
        WHEN p_status IN ('backlog', 'blocked', 'cancelled') THEN FALSE
        WHEN p_status IN ('todo', 'in_progress', 'in_review', 'done') THEN TRUE
        ELSE COALESCE((
            SELECT s.category IN ('unstarted', 'started', 'done')
            FROM issue_status AS s
            WHERE s.workspace_id = p_workspace_id
              AND s.key = p_status
        ), FALSE)
    END
$function$;

ALTER TABLE card_session ADD COLUMN pause_reason TEXT;
ALTER TABLE card_session
    ADD COLUMN token_input_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN token_output_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN token_cache_read_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN token_cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN token_task_count BIGINT NOT NULL DEFAULT 0;
DROP TRIGGER IF EXISTS card_session_close_guard ON card_session;
DROP TRIGGER IF EXISTS card_session_issue_status_sync ON issue;
ALTER TABLE card_session DROP CONSTRAINT IF EXISTS card_session_state_check;
ALTER TABLE card_session DROP CONSTRAINT IF EXISTS card_session_state_timestamps;

-- Preserve resumable sessions for active/done cards; pause parked cards and
-- sessions owned by a previous assignee. Cancellation stops tasks in the
-- application status path and keeps provider state resumable if reopened.
UPDATE card_session AS cs
SET state = CASE
        WHEN cs.state = 'closed' THEN 'closed'
        WHEN issue_status_allows_agent_task(i.workspace_id, i.status)
             AND i.assignee_type = 'agent'
             AND i.assignee_id = cs.agent_id THEN 'open'
        ELSE 'paused'
    END,
    pause_reason = CASE
        WHEN cs.state = 'closed' THEN NULL
        WHEN NOT issue_status_allows_agent_task(i.workspace_id, i.status)
            THEN issue_effective_status(i.workspace_id, i.status)
        WHEN i.assignee_type IS DISTINCT FROM 'agent'
             OR i.assignee_id IS NULL
             OR i.assignee_id <> cs.agent_id THEN 'unassigned'
        ELSE NULL
    END,
    done_at = NULL,
    retain_until = NULL,
    closed_at = CASE WHEN cs.state = 'closed' THEN COALESCE(cs.closed_at, now()) ELSE NULL END,
    updated_at = now()
FROM issue AS i
WHERE i.id = cs.issue_id
  AND i.workspace_id = cs.workspace_id;

-- Historical closed rows are immutable to normal lifecycle updates, so clear
-- their resume pointers explicitly during upgrade, including orphan rows that
-- were not joined by the backfill above.
UPDATE card_session
SET provider_session_id = NULL,
    work_dir = NULL,
    updated_at = now()
WHERE state = 'closed'
  AND (provider_session_id IS NOT NULL OR work_dir IS NOT NULL);

ALTER TABLE card_session
    ADD CONSTRAINT card_session_state_check
        CHECK (state IN ('open', 'paused', 'closed')),
    ADD CONSTRAINT card_session_state_timestamps CHECK (
        (state = 'open' AND pause_reason IS NULL AND closed_at IS NULL)
        OR (state = 'paused' AND pause_reason IS NOT NULL AND closed_at IS NULL)
        OR (state = 'closed' AND pause_reason IS NULL AND closed_at IS NOT NULL)
    );

CREATE OR REPLACE FUNCTION guard_card_session_close()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    configured_hours INTEGER;
    configured_value TEXT;
BEGIN
    IF OLD.state <> 'closed' AND NEW.state = 'closed' THEN
        SELECT settings->'card_sessions'->>'idle_timeout_hours'
        INTO configured_value
        FROM workspace
        WHERE id = OLD.workspace_id;
        configured_hours := CASE
            WHEN configured_value ~ '^[0-9]{1,3}$' THEN GREATEST(1, LEAST(999, configured_value::INTEGER))
            ELSE 24
        END;

        IF OLD.last_activity_at > now() - make_interval(hours => configured_hours)
        OR EXISTS (
            SELECT 1
            FROM agent_task_queue AS task
            WHERE task.issue_id = OLD.issue_id
              AND task.agent_id = OLD.agent_id
              AND task.status IN ('dispatched', 'running', 'waiting_local_directory')
        ) OR EXISTS (
            SELECT 1
            FROM agent_task_queue AS task
            JOIN issue ON issue.id = task.issue_id
            WHERE task.issue_id = OLD.issue_id
              AND task.agent_id = OLD.agent_id
              AND task.status IN ('queued', 'deferred')
              AND issue.workspace_id = OLD.workspace_id
              AND issue_status_allows_agent_task(issue.workspace_id, issue.status)
              AND NOT (
                  OLD.state = 'paused'
                  AND OLD.last_activity_at <= now() - make_interval(hours => configured_hours)
                  AND OLD.pause_reason IS NOT NULL
                  AND OLD.pause_reason NOT IN ('capacity', 'unassigned')
                  AND NOT issue_status_allows_agent_task(OLD.workspace_id, OLD.pause_reason)
              )
        ) THEN
            RAISE EXCEPTION 'card session is active or has unfinished work'
                USING ERRCODE = 'check_violation';
        END IF;
        -- Closed generations cannot be resumed. Clear provider identifiers and
        -- work paths only after the close guard has accepted the transition.
        NEW.provider_session_id := NULL;
        NEW.work_dir := NULL;
    END IF;
    IF OLD.state = 'closed' AND NEW.state <> 'closed' THEN
        RAISE EXCEPTION 'closed card sessions are immutable'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER card_session_close_guard
BEFORE UPDATE OF state ON card_session
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION guard_card_session_close();

CREATE OR REPLACE FUNCTION sync_card_session_issue_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    workspace_settings JSONB;
    max_sessions INTEGER;
    idle_timeout_hours INTEGER;
    open_sessions BIGINT;
    current_session_state TEXT;
    current_session_last_activity_at TIMESTAMPTZ;
    current_session_pause_reason TEXT;
    current_session_found BOOLEAN;
    next_generation BIGINT;
BEGIN
    SELECT settings INTO workspace_settings
    FROM workspace
    WHERE id = NEW.workspace_id
    FOR UPDATE;

    IF NOT issue_status_allows_agent_task(NEW.workspace_id, NEW.status) THEN
        UPDATE card_session
        SET state = 'paused',
            pause_reason = issue_effective_status(NEW.workspace_id, NEW.status),
            last_activity_at = now(),
            updated_at = now()
        WHERE issue_id = NEW.id
          AND workspace_id = NEW.workspace_id
          AND state <> 'closed';
        RETURN NEW;
    END IF;

    IF NEW.assignee_type IS DISTINCT FROM 'agent' OR NEW.assignee_id IS NULL THEN
        UPDATE card_session
        SET state = 'paused',
            pause_reason = 'unassigned',
            last_activity_at = now(),
            updated_at = now()
        WHERE issue_id = NEW.id
          AND workspace_id = NEW.workspace_id
          AND state <> 'closed';
        RETURN NEW;
    END IF;

    UPDATE card_session
    SET state = 'paused',
        pause_reason = 'unassigned',
        last_activity_at = now(),
        updated_at = now()
    WHERE issue_id = NEW.id
      AND workspace_id = NEW.workspace_id
      AND agent_id <> NEW.assignee_id
      AND state <> 'closed';

    max_sessions := CASE
        WHEN workspace_settings->'card_sessions'->>'max_open_sessions' ~ '^[0-9]{1,5}$'
            THEN GREATEST(1, LEAST(10000, (workspace_settings->'card_sessions'->>'max_open_sessions')::INTEGER))
        ELSE 100
    END;
    idle_timeout_hours := CASE
        WHEN workspace_settings->'card_sessions'->>'idle_timeout_hours' ~ '^[0-9]{1,3}$'
            THEN GREATEST(1, LEAST(999, (workspace_settings->'card_sessions'->>'idle_timeout_hours')::INTEGER))
        ELSE 24
    END;

    SELECT state, last_activity_at, pause_reason
    INTO current_session_state, current_session_last_activity_at, current_session_pause_reason
    FROM card_session
    WHERE issue_id = NEW.id
      AND agent_id = NEW.assignee_id
      AND workspace_id = NEW.workspace_id
      AND state <> 'closed'
    FOR UPDATE;
    current_session_found := FOUND;

    -- A comment can reopen a parked card before the minute sweeper runs. Check
    -- the idle deadline before touching a paused generation so an expired
    -- provider session/work directory is never resumed on that race.
    IF current_session_found
       AND current_session_last_activity_at <= now() - make_interval(hours => idle_timeout_hours)
       AND NOT EXISTS (
           SELECT 1
           FROM agent_task_queue AS task
           WHERE task.issue_id = NEW.id
             AND task.agent_id = NEW.assignee_id
             AND task.status IN ('dispatched', 'running', 'waiting_local_directory')
       )
       AND (
           NOT EXISTS (
               SELECT 1
               FROM agent_task_queue AS task
               WHERE task.issue_id = NEW.id
                 AND task.agent_id = NEW.assignee_id
                 AND task.status IN ('queued', 'deferred')
           )
           OR (
               current_session_state = 'paused'
               AND current_session_pause_reason NOT IN ('capacity', 'unassigned')
               AND NOT issue_status_allows_agent_task(NEW.workspace_id, current_session_pause_reason)
           )
       ) THEN
        UPDATE card_session
        SET state = 'closed',
            pause_reason = NULL,
            closed_at = now(),
            updated_at = now()
        WHERE issue_id = NEW.id
          AND agent_id = NEW.assignee_id
          AND workspace_id = NEW.workspace_id
          AND state <> 'closed';
        current_session_found := FALSE;
    END IF;

    SELECT COUNT(*) INTO open_sessions
    FROM card_session
    WHERE workspace_id = NEW.workspace_id
      AND state = 'open';

    IF current_session_found THEN
        IF current_session_state = 'open' OR open_sessions < max_sessions THEN
            UPDATE card_session
            SET state = 'open',
                pause_reason = NULL,
                last_activity_at = now(),
                updated_at = now()
            WHERE issue_id = NEW.id
              AND agent_id = NEW.assignee_id
              AND workspace_id = NEW.workspace_id
              AND state <> 'closed';
        ELSE
            UPDATE card_session
            SET pause_reason = 'capacity',
                last_activity_at = now(),
                updated_at = now()
            WHERE issue_id = NEW.id
              AND agent_id = NEW.assignee_id
              AND workspace_id = NEW.workspace_id
              AND state = 'paused';
        END IF;
    ELSIF open_sessions < max_sessions THEN
        SELECT COALESCE(MAX(generation), 0) + 1 INTO next_generation
        FROM card_session
        WHERE issue_id = NEW.id
          AND agent_id = NEW.assignee_id
          AND workspace_id = NEW.workspace_id;

        INSERT INTO card_session (workspace_id, issue_id, agent_id, generation, provider)
        SELECT NEW.workspace_id, NEW.id, agent.id, next_generation, agent.runtime_mode
        FROM agent
        WHERE agent.id = NEW.assignee_id
          AND agent.workspace_id = NEW.workspace_id
        ON CONFLICT (issue_id, agent_id) WHERE state <> 'closed' DO NOTHING;
    END IF;

    IF open_sessions >= max_sessions
       AND (NOT current_session_found OR current_session_state <> 'open') THEN
        -- A task may have remained queued while its parked generation expired.
        -- Deferred tasks also need the capacity marker so the normal promoter
        -- cannot turn them into claimable work before a generation is admitted.
        -- Preserve an existing fire_at (and channel media marker); queued work
        -- becomes due immediately and the recovery sweep admits it later.
        UPDATE agent_task_queue
        SET status = 'deferred',
            fire_at = CASE WHEN status = 'queued' THEN now() ELSE COALESCE(fire_at, now()) END,
            context = jsonb_set(COALESCE(context, '{}'::jsonb), '{card_session_capacity_pending}', 'true'::jsonb, true)
        WHERE issue_id = NEW.id
          AND agent_id = NEW.assignee_id
          AND status IN ('queued', 'deferred');
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER card_session_issue_status_sync
AFTER UPDATE OF status ON issue
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION sync_card_session_issue_status();

CREATE OR REPLACE FUNCTION touch_card_session_on_issue_content()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE card_session
    SET last_activity_at = now(), updated_at = now()
    WHERE issue_id = NEW.id
      AND workspace_id = NEW.workspace_id
      AND state <> 'closed';
    RETURN NEW;
END;
$$;

CREATE TRIGGER card_session_issue_content_touch
AFTER UPDATE OF title, description ON issue
FOR EACH ROW
WHEN (OLD.title IS DISTINCT FROM NEW.title OR OLD.description IS DISTINCT FROM NEW.description)
EXECUTE FUNCTION touch_card_session_on_issue_content();
