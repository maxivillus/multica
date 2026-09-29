-- Durable per-issue agent generations. The row is the server-owned lifecycle
-- record; a provider process may be live, idle, or temporarily absent while
-- the generation remains addressable by its stable id.
CREATE TABLE card_session (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    issue_id UUID NOT NULL REFERENCES issue(id) ON DELETE CASCADE,
    agent_id UUID NOT NULL REFERENCES agent(id) ON DELETE CASCADE,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    state TEXT NOT NULL DEFAULT 'open'
        CHECK (state IN ('open', 'done_retained', 'closed')),
    provider TEXT NOT NULL DEFAULT '',
    provider_session_id TEXT,
    work_dir TEXT,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at TIMESTAMPTZ,
    retain_until TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    lease_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    lease_heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT card_session_state_timestamps CHECK (
        (state = 'open' AND done_at IS NULL AND retain_until IS NULL AND closed_at IS NULL)
        OR (state = 'done_retained' AND done_at IS NOT NULL AND retain_until IS NOT NULL AND closed_at IS NULL)
        OR (state = 'closed' AND closed_at IS NOT NULL)
    )
);

-- At most one resumable generation exists for an issue/agent pair. Closed
-- generations remain as immutable history and the next one increments the
-- generation number under the workspace lock.
CREATE UNIQUE INDEX card_session_one_resumable_per_issue_agent
    ON card_session (issue_id, agent_id)
    WHERE state <> 'closed';

CREATE INDEX card_session_workspace_state_idx
    ON card_session (workspace_id, state, retain_until);

CREATE INDEX card_session_issue_idx
    ON card_session (issue_id, agent_id, generation DESC);

COMMENT ON TABLE card_session IS
    'Server-owned lifecycle for a per-issue agent generation; provider process state is resumable but not the source of truth.';

-- No caller may close a work/review generation directly. The only legal
-- physical close is the expiry pass after the row has first entered the
-- post-done retention state. This remains enforced even if a future API or
-- provider integration bypasses the current service methods.
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

CREATE TRIGGER card_session_close_guard
BEFORE UPDATE OF state ON card_session
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION guard_card_session_close();

-- Keep the lifecycle invariant at the database boundary as well as in the
-- HTTP/service paths. GitHub/VCS webhooks and daemon recovery can write issue
-- status without passing through the HTTP handler, while comments and status
-- changes must still move the same generation atomically with the issue.
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

CREATE TRIGGER card_session_issue_status_sync
AFTER UPDATE OF status ON issue
FOR EACH ROW
WHEN (OLD.status IS DISTINCT FROM NEW.status)
EXECUTE FUNCTION sync_card_session_issue_status();
