-- Restore migration 420's done-only trigger behavior when rolling back this
-- migration. The card_session schema and trigger names remain unchanged.

CREATE OR REPLACE FUNCTION guard_card_session_close()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'open' AND NEW.state = 'closed' THEN
        RAISE EXCEPTION 'card session can only close after issue reaches done'
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

    IF issue_effective_status(OLD.workspace_id, OLD.status) <> 'done'
       AND issue_effective_status(NEW.workspace_id, NEW.status) = 'done' THEN
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
    ELSIF issue_effective_status(OLD.workspace_id, OLD.status) = 'done'
       AND issue_effective_status(NEW.workspace_id, NEW.status) <> 'done' THEN
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
