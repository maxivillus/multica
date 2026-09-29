-- Treat the built-in cancelled category like done for durable card-session
-- retention. Migration 548 created the trigger with done-only predicates;
-- replace the functions in a forward migration so existing databases keep
-- their history and receive the new lifecycle rule safely.

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

CREATE OR REPLACE FUNCTION sync_card_session_issue_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    configured_hours INTEGER;
    configured_value TEXT;
BEGIN
    -- Serialize status-driven lifecycle changes with generation allocation and
    -- capacity checks, which take the same workspace row lock in Go.
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
