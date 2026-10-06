DROP TRIGGER IF EXISTS card_session_issue_content_touch ON issue;
DROP FUNCTION IF EXISTS touch_card_session_on_issue_content();
DROP TRIGGER IF EXISTS card_session_issue_status_sync ON issue;
DROP TRIGGER IF EXISTS card_session_close_guard ON card_session;

-- Restore the previous setting names for older binaries. Removed values are
-- read from rollback-only workspace metadata captured by the up migration;
-- current idle_timeout_hours supplies retention only when no old value existed.
UPDATE workspace AS w
SET settings = (
    jsonb_set(
        COALESCE(w.settings, '{}'::jsonb),
        '{card_sessions}',
        (w.settings->'card_sessions' || jsonb_build_object(
            'post_done_retention_hours', COALESCE(
                w.settings->'_migration_567_card_session_settings'->'post_done_retention_hours',
                w.settings->'card_sessions'->'idle_timeout_hours',
                '24'::jsonb
            )
        )) - 'idle_timeout_hours',
        true
    ) - '_migration_567_card_session_settings'
)
WHERE jsonb_typeof(w.settings->'card_sessions') = 'object';

ALTER TABLE card_session DROP CONSTRAINT IF EXISTS card_session_state_check;
ALTER TABLE card_session DROP CONSTRAINT IF EXISTS card_session_state_timestamps;

UPDATE card_session AS cs
SET state = CASE
        WHEN cs.state = 'closed' THEN 'closed'
        WHEN issue_effective_status(i.workspace_id, i.status) IN ('done', 'cancelled') THEN 'done_retained'
        ELSE 'open'
    END,
    done_at = CASE
        WHEN cs.state <> 'closed' AND issue_effective_status(i.workspace_id, i.status) IN ('done', 'cancelled') THEN now()
        ELSE NULL
    END,
    retain_until = CASE
        WHEN cs.state <> 'closed' AND issue_effective_status(i.workspace_id, i.status) IN ('done', 'cancelled') THEN
            now() + (
                GREATEST(1, LEAST(720, CASE
                    WHEN w.settings->'card_sessions'->>'post_done_retention_hours' ~ '^[0-9]{1,4}$'
                        THEN (w.settings->'card_sessions'->>'post_done_retention_hours')::INTEGER
                    ELSE 24
                END)) * interval '1 hour'
            )
        ELSE NULL
    END,
    pause_reason = NULL,
    provider_session_id = CASE WHEN cs.state = 'closed' THEN NULL ELSE cs.provider_session_id END,
    work_dir = CASE WHEN cs.state = 'closed' THEN NULL ELSE cs.work_dir END,
    closed_at = CASE WHEN cs.state = 'closed' THEN COALESCE(cs.closed_at, now()) ELSE NULL END,
    updated_at = now()
FROM issue AS i, workspace AS w
WHERE i.id = cs.issue_id
  AND i.workspace_id = cs.workspace_id
  AND w.id = cs.workspace_id;

ALTER TABLE card_session
    ADD CONSTRAINT card_session_state_check
        CHECK (state IN ('open', 'done_retained', 'closed')),
    ADD CONSTRAINT card_session_state_timestamps CHECK (
        (state = 'open' AND done_at IS NULL AND retain_until IS NULL AND closed_at IS NULL)
        OR (state = 'done_retained' AND done_at IS NOT NULL AND retain_until IS NOT NULL AND closed_at IS NULL)
        OR (state = 'closed' AND closed_at IS NOT NULL)
    );

ALTER TABLE card_session
    DROP COLUMN IF EXISTS pause_reason;
CREATE OR REPLACE FUNCTION guard_card_session_close()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'open' AND NEW.state = 'closed' THEN
        RAISE EXCEPTION 'card session can only close after issue reaches a terminal status'
            USING ERRCODE = 'check_violation';
    END IF;
    IF OLD.state = 'done_retained'
       AND NEW.state = 'closed'
       AND (OLD.retain_until IS NULL OR OLD.retain_until > now()) THEN
        RAISE EXCEPTION 'card session retention window has not expired'
            USING ERRCODE = 'check_violation';
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
    configured_hours INTEGER;
    configured_value TEXT;
BEGIN
    PERFORM 1
    FROM workspace
    WHERE id = NEW.workspace_id
    FOR UPDATE;

    IF issue_effective_status(OLD.workspace_id, OLD.status) NOT IN ('done', 'cancelled')
       AND issue_effective_status(NEW.workspace_id, NEW.status) IN ('done', 'cancelled') THEN
        SELECT settings->'card_sessions'->>'post_done_retention_hours'
        INTO configured_value
        FROM workspace
        WHERE id = NEW.workspace_id;
        configured_hours := CASE
            WHEN configured_value ~ '^[0-9]{1,4}$' THEN configured_value::INTEGER
            ELSE 24
        END;
        configured_hours := GREATEST(1, LEAST(720, configured_hours));

        UPDATE card_session
        SET state = 'done_retained',
            done_at = now(),
            retain_until = now() + (configured_hours * interval '1 hour'),
            lease_owner = NULL,
            lease_heartbeat_at = NULL,
            last_activity_at = now(),
            updated_at = now()
        WHERE issue_id = NEW.id
          AND state = 'open';
    ELSIF issue_effective_status(OLD.workspace_id, OLD.status) IN ('done', 'cancelled')
       AND issue_effective_status(NEW.workspace_id, NEW.status) NOT IN ('done', 'cancelled') THEN
        UPDATE card_session
        SET state = 'open',
            done_at = NULL,
            retain_until = NULL,
            closed_at = NULL,
            last_activity_at = now(),
            updated_at = now()
        WHERE issue_id = NEW.id
          AND state = 'done_retained'
          AND retain_until > now();
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER card_session_issue_status_sync
AFTER UPDATE OF status ON issue
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION sync_card_session_issue_status();

DROP FUNCTION IF EXISTS issue_status_allows_agent_task(UUID, TEXT);
