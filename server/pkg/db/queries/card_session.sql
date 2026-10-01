-- name: LockWorkspaceForCardSession :one
-- Serializes capacity checks and generation allocation for one workspace.
SELECT id, settings
FROM workspace
WHERE id = $1
FOR UPDATE;

-- name: LockIssueForCardSession :one
-- Serialize generation allocation with issue deletion. DeleteIssue takes
-- FOR UPDATE on the issue before removing card sessions; this key-share lock
-- prevents a concurrent allocator from inserting an orphan after deletion.
SELECT id
FROM issue
WHERE id = $1
  AND workspace_id = $2
FOR KEY SHARE;

-- name: LockIssueTaskLifecycle :exec
-- Shares the transaction-scoped lock used by the issue status trigger. Keep
-- this as a separate statement before reading status so READ COMMITTED takes
-- a fresh snapshot after any transition that held the lock first.
SELECT lock_issue_task_lifecycle($1);

-- name: GetResumableCardSession :one
SELECT *
FROM card_session
WHERE issue_id = $1
  AND agent_id = $2
  AND workspace_id = $3
  AND state <> 'closed'
ORDER BY generation DESC
LIMIT 1;

-- name: GetLatestCardSession :one
SELECT *
FROM card_session
WHERE issue_id = $1
  AND agent_id = $2
  AND workspace_id = $3
ORDER BY generation DESC
LIMIT 1;

-- name: CountOpenCardSessions :one
SELECT COUNT(*)::bigint
FROM card_session
WHERE workspace_id = $1
  AND state = 'open';

-- name: ListDueCardSessionCapacityWaitersForRuntimes :many
-- Deferred issue tasks stay outside the claim queue until their workspace has
-- room for the session they need. The runtime claim loop retries a bounded
-- batch so a full workspace cannot turn one claim into unbounded database work.
SELECT task.id,
       task.issue_id,
       task.agent_id,
       issue.workspace_id,
       agent.runtime_mode
FROM agent_task_queue AS task
JOIN issue ON issue.id = task.issue_id
JOIN agent ON agent.id = task.agent_id
WHERE task.runtime_id = ANY(@runtime_ids::uuid[])
  AND task.status = 'deferred'
  AND task.fire_at <= now()
  AND task.context->>'card_session_capacity_pending' = 'true'
  AND issue_status_allows_agent_task(issue.workspace_id, issue.status)
ORDER BY task.priority DESC, task.created_at ASC, task.id ASC
LIMIT sqlc.arg(waiter_limit)::integer;

-- name: ReleaseCardSessionCapacityWaiter :execrows
UPDATE agent_task_queue
SET context = COALESCE(context, '{}'::jsonb) - 'card_session_capacity_pending'
WHERE id = @id
  AND status = 'deferred'
  AND context->>'card_session_capacity_pending' = 'true';

-- name: RetryCardSessionCapacityWaiter :execrows
UPDATE agent_task_queue
SET fire_at = now() + make_interval(secs => @retry_delay_seconds::double precision)
WHERE id = @id
  AND status = 'deferred'
  AND context->>'card_session_capacity_pending' = 'true';

-- name: CreateCardSession :one
INSERT INTO card_session (
    workspace_id, issue_id, agent_id, generation, provider
)
SELECT @workspace_id, issue.id, agent.id, @generation, @provider
FROM issue
JOIN agent ON agent.id = @agent_id
WHERE issue.id = @issue_id
  AND issue.workspace_id = @workspace_id
  AND agent.workspace_id = @workspace_id
  AND issue_status_allows_agent_task(issue.workspace_id, issue.status)
RETURNING *;

-- name: TouchCardSession :one
UPDATE card_session
SET state = 'open',
    pause_reason = NULL,
    lease_owner = CASE WHEN state = 'paused' THEN NULL ELSE lease_owner END,
    lease_heartbeat_at = CASE WHEN state = 'paused' THEN NULL ELSE lease_heartbeat_at END,
    last_activity_at = now(),
    updated_at = now()
WHERE id = $1
  AND state <> 'closed'
RETURNING *;

-- name: SyncCardSessionsForIssue :many
-- The issue trigger is authoritative for direct SQL and webhook updates. This
-- query pauses generations for inactive issues and previous assignees. Active
-- assigned sessions are opened by EnsureCardSession after capacity checks.
UPDATE card_session AS cs
SET state = 'paused',
    pause_reason = CASE
        WHEN issue_status_allows_agent_task(issue.workspace_id, issue.status) THEN 'unassigned'
        ELSE issue_effective_status(issue.workspace_id, issue.status)
    END,
    lease_owner = NULL,
    lease_heartbeat_at = NULL,
    last_activity_at = now(),
    updated_at = now()
FROM issue
WHERE cs.issue_id = $1
  AND cs.workspace_id = sqlc.arg(workspace_id)
  AND issue.id = cs.issue_id
  AND issue.workspace_id = cs.workspace_id
  AND cs.state <> 'closed'
  AND (
      NOT issue_status_allows_agent_task(issue.workspace_id, issue.status)
      OR issue.assignee_type IS DISTINCT FROM 'agent'
      OR issue.assignee_id IS NULL
      OR cs.agent_id <> issue.assignee_id
  )
RETURNING cs.*;

-- name: ExpireCardSessionsForWorkspace :many
UPDATE card_session AS cs
SET state = 'closed',
    pause_reason = NULL,
    closed_at = now(),
    lease_owner = NULL,
    lease_heartbeat_at = NULL,
    updated_at = now()
FROM workspace AS w
WHERE cs.workspace_id = $1
  AND w.id = cs.workspace_id
  AND cs.state <> 'closed'
  AND cs.last_activity_at <= now() - make_interval(hours => CASE
      WHEN w.settings->'card_sessions'->>'idle_timeout_hours' ~ '^[0-9]{1,3}$'
          THEN GREATEST(1, LEAST(999, (w.settings->'card_sessions'->>'idle_timeout_hours')::INTEGER))
      ELSE 24
  END)
  AND NOT EXISTS (
      SELECT 1
      FROM agent_task_queue AS task
      WHERE task.issue_id = cs.issue_id
        AND task.agent_id = cs.agent_id
        AND task.status IN ('dispatched', 'running', 'waiting_local_directory')
  )
  AND (
      NOT EXISTS (
          SELECT 1 FROM issue
          WHERE issue.id = cs.issue_id
            AND issue.workspace_id = cs.workspace_id
            AND issue_status_allows_agent_task(issue.workspace_id, issue.status)
      )
      OR NOT EXISTS (
          SELECT 1 FROM agent_task_queue AS task
          WHERE task.issue_id = cs.issue_id
            AND task.agent_id = cs.agent_id
            AND task.status IN ('queued', 'deferred')
      )
  )
RETURNING cs.*;

-- name: ExpireCardSessions :many
UPDATE card_session AS cs
SET state = 'closed',
    pause_reason = NULL,
    closed_at = now(),
    lease_owner = NULL,
    lease_heartbeat_at = NULL,
    updated_at = now()
FROM workspace AS w
WHERE w.id = cs.workspace_id
  AND cs.state <> 'closed'
  AND cs.last_activity_at <= now() - make_interval(hours => CASE
      WHEN w.settings->'card_sessions'->>'idle_timeout_hours' ~ '^[0-9]{1,3}$'
          THEN GREATEST(1, LEAST(999, (w.settings->'card_sessions'->>'idle_timeout_hours')::INTEGER))
      ELSE 24
  END)
  AND NOT EXISTS (
      SELECT 1
      FROM agent_task_queue AS task
      WHERE task.issue_id = cs.issue_id
        AND task.agent_id = cs.agent_id
        AND task.status IN ('dispatched', 'running', 'waiting_local_directory')
  )
  AND (
      NOT EXISTS (
          SELECT 1 FROM issue
          WHERE issue.id = cs.issue_id
            AND issue.workspace_id = cs.workspace_id
            AND issue_status_allows_agent_task(issue.workspace_id, issue.status)
      )
      OR NOT EXISTS (
          SELECT 1 FROM agent_task_queue AS task
          WHERE task.issue_id = cs.issue_id
            AND task.agent_id = cs.agent_id
            AND task.status IN ('queued', 'deferred')
      )
  )
RETURNING cs.*;

-- name: UpdateCardSessionProviderStateByTask :exec
UPDATE card_session AS cs
SET provider_session_id = COALESCE(NULLIF(sqlc.arg(provider_session_id), ''), cs.provider_session_id),
    work_dir = COALESCE(NULLIF(sqlc.arg(work_dir), ''), cs.work_dir),
    last_activity_at = now(),
    updated_at = now()
FROM agent_task_queue AS t, workspace AS w
WHERE t.id = $1
  AND t.card_session_id = cs.id
  AND w.id = cs.workspace_id
  AND (
      (t.status IN ('dispatched', 'running') AND cs.state = 'open')
      OR (
          t.status = 'cancelled'
          AND cs.state = 'paused'
          AND cs.pause_reason = 'cancelled'
          AND cs.last_activity_at > now() - make_interval(hours => CASE
              WHEN w.settings->'card_sessions'->>'idle_timeout_hours' ~ '^[0-9]{1,3}$'
                  THEN GREATEST(1, LEAST(999, (w.settings->'card_sessions'->>'idle_timeout_hours')::INTEGER))
              ELSE 24
          END)
      )
  );

-- name: GetCardSession :one
SELECT *
FROM card_session
WHERE id = $1;

-- name: AcquireCardSessionLeaseForTask :one
-- A live provider host acquires the current generation only when no other
-- owner has a fresh heartbeat. A same-owner replay with a fresh heartbeat is
-- idempotent; a takeover or stale same-owner recovery advances the fencing
-- epoch. The task binding keeps a stale task from acquiring a newer generation.
UPDATE card_session AS cs
SET lease_owner = NULLIF(sqlc.arg(lease_owner)::text, ''),
    lease_epoch = cs.lease_epoch + CASE
        WHEN cs.lease_owner = sqlc.arg(lease_owner)::text
             AND cs.lease_heartbeat_at > now() - make_interval(secs => sqlc.arg(stale_after_seconds)::double precision)
            THEN 0
        ELSE 1
    END,
    lease_heartbeat_at = now(),
    updated_at = now()
FROM agent_task_queue AS task
WHERE task.id = sqlc.arg(task_id)
  AND task.card_session_id = cs.id
  AND task.status = 'running'
  AND cs.state = 'open'
  AND NULLIF(sqlc.arg(lease_owner)::text, '') IS NOT NULL
  AND (
      cs.lease_owner IS NULL
      OR cs.lease_heartbeat_at IS NULL
      OR cs.lease_heartbeat_at <= now() - make_interval(secs => sqlc.arg(stale_after_seconds)::double precision)
      OR cs.lease_owner = sqlc.arg(lease_owner)::text
  )
RETURNING cs.*;

-- name: HeartbeatCardSessionLeaseByTask :one
-- Heartbeats fence ownership only; they must never extend card idle activity.
UPDATE card_session AS cs
SET lease_heartbeat_at = now(),
    updated_at = now()
FROM agent_task_queue AS task
WHERE task.id = sqlc.arg(task_id)
  AND task.card_session_id = cs.id
  AND cs.state = 'open'
  AND cs.lease_owner = sqlc.arg(lease_owner)::text
  AND cs.lease_epoch = sqlc.arg(lease_epoch)
RETURNING cs.*;

-- name: ReleaseCardSessionLeaseByTask :one
-- Release is fenced by both owner and epoch so a previous process cannot
-- clear a lease acquired by its replacement.
UPDATE card_session AS cs
SET lease_owner = NULL,
    lease_heartbeat_at = NULL,
    updated_at = now()
FROM agent_task_queue AS task
WHERE task.id = sqlc.arg(task_id)
  AND task.card_session_id = cs.id
  AND cs.state <> 'closed'
  AND cs.lease_owner = sqlc.arg(lease_owner)::text
  AND cs.lease_epoch = sqlc.arg(lease_epoch)
RETURNING cs.*;

-- name: FinalizeCardSessionProviderStateByTask :exec
-- The provider can reveal its session only in the terminal result. Persist
-- that pointer in the same transaction as the task transition so the next
-- comment cannot claim the card between completion and the asynchronous pin.
-- Empty values intentionally preserve the last known pointer: a provider
-- turn may finish without repeating its stable session id.
UPDATE card_session AS cs
SET provider_session_id = COALESCE(NULLIF(sqlc.arg(provider_session_id), ''), cs.provider_session_id),
    work_dir = COALESCE(NULLIF(sqlc.arg(work_dir), ''), cs.work_dir),
    last_activity_at = now(),
    updated_at = now()
FROM agent_task_queue AS t
WHERE t.id = sqlc.arg(task_id)
  AND t.card_session_id = cs.id
  AND cs.state <> 'closed'
  AND t.status IN ('completed', 'failed', 'cancelled');

-- name: TouchCardSessionsForTasks :exec
-- Provider usage and committed task transitions advance activity after their
-- owning transaction, avoiding a task-row -> card-session lock cycle.
UPDATE card_session AS cs
SET last_activity_at = now(),
    updated_at = now()
FROM agent_task_queue AS task
WHERE task.id = ANY(sqlc.arg(task_ids)::uuid[])
  AND task.card_session_id = cs.id
  AND cs.state <> 'closed';

-- Session token totals are cached after each accepted provider usage report.
-- The source remains task_usage so corrections are reflected on refresh. Closed
-- generations remain eligible: a restart can close an idle session before its
-- recovery sweep gets a chance to refresh the final persisted usage totals.
-- name: ListCardSessionsWithStaleTokenStats :many
SELECT cs.id
  FROM card_session AS cs
  WHERE EXISTS (
      SELECT 1
      FROM agent_task_queue AS task
      JOIN task_usage AS usage ON usage.task_id = task.id
      WHERE task.card_session_id = cs.id
        AND usage.created_at >= cs.opened_at
        AND (cs.last_token_stats_at IS NULL OR usage.updated_at > cs.last_token_stats_at)
  )
ORDER BY cs.last_token_stats_at NULLS FIRST, cs.id
LIMIT sqlc.arg('limit')::integer;

-- name: LockCardSessionForTaskTokenStats :one
SELECT cs.id
FROM card_session AS cs
JOIN agent_task_queue AS task
  ON task.card_session_id = cs.id
WHERE task.id = sqlc.arg(task_id)
ORDER BY cs.generation DESC
LIMIT 1
FOR UPDATE OF cs;

-- name: LockCardSessionTokenStats :one
SELECT id
FROM card_session
WHERE id = sqlc.arg(card_session_id)
FOR UPDATE;

-- name: GetCardSessionTokenStats :one
SELECT
    COALESCE(SUM(usage.input_tokens), 0)::bigint AS input_tokens,
    COALESCE(SUM(usage.output_tokens), 0)::bigint AS output_tokens,
    COALESCE(SUM(usage.cache_read_tokens), 0)::bigint AS cache_read_tokens,
    COALESCE(SUM(usage.cache_write_tokens), 0)::bigint AS cache_write_tokens,
    COUNT(DISTINCT task.id)::bigint AS task_count,
    COALESCE(MAX(usage.updated_at), now())::timestamptz AS latest_usage_at
FROM card_session AS cs
JOIN agent_task_queue AS task
  ON task.card_session_id = cs.id
JOIN task_usage AS usage ON usage.task_id = task.id
WHERE cs.id = sqlc.arg(card_session_id)
  AND usage.created_at >= cs.opened_at;

-- name: UpdateCardSessionTokenStats :exec
UPDATE card_session
SET token_input_tokens = sqlc.arg(input_tokens),
    token_output_tokens = sqlc.arg(output_tokens),
    token_cache_read_tokens = sqlc.arg(cache_read_tokens),
    token_cache_write_tokens = sqlc.arg(cache_write_tokens),
    token_task_count = sqlc.arg(task_count),
    last_token_stats_at = sqlc.arg(latest_usage_at),
    updated_at = now()
WHERE id = sqlc.arg(card_session_id);
