-- name: LockWorkspaceForCardSession :one
-- Serializes capacity checks and generation allocation for one workspace.
SELECT id, settings
FROM workspace
WHERE id = $1
FOR UPDATE;

-- name: GetResumableCardSession :one
SELECT *
FROM card_session
WHERE issue_id = $1
  AND agent_id = $2
  AND workspace_id = $3
  AND state <> 'closed'
  AND (state = 'open' OR retain_until > now())
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
  AND (
    state = 'open'
    OR (state = 'done_retained' AND retain_until > now())
  );

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
RETURNING *;

-- name: ReopenCardSession :one
UPDATE card_session
SET state = 'open',
    done_at = NULL,
    retain_until = NULL,
    closed_at = NULL,
    last_activity_at = now(),
    updated_at = now()
WHERE id = $1
  AND state = 'done_retained'
  AND retain_until > now()
RETURNING *;

-- name: TouchCardSession :one
UPDATE card_session
SET last_activity_at = now(), updated_at = now()
WHERE id = $1
  AND state <> 'closed'
RETURNING *;

-- name: MarkCardSessionsDone :many
-- Only a status transition handler may call this query. It moves sessions into
-- the retention state; physical close is a separate expiry operation below.
UPDATE card_session
SET state = 'done_retained',
    done_at = now(),
    retain_until = now() + (sqlc.arg(retention_hours)::bigint * interval '1 hour'),
    lease_owner = NULL,
    lease_heartbeat_at = NULL,
    last_activity_at = now(),
    updated_at = now()
WHERE issue_id = $1
  AND card_session.workspace_id = sqlc.arg(workspace_id)
  AND state = 'open'
  AND EXISTS (
      SELECT 1
      FROM issue
      WHERE issue.id = card_session.issue_id
        AND issue.workspace_id = card_session.workspace_id
        AND issue_effective_status(issue.workspace_id, issue.status) = 'done'
  )
RETURNING *;

-- name: ReopenCardSessionsForIssue :many
UPDATE card_session
SET state = 'open',
    done_at = NULL,
    retain_until = NULL,
    closed_at = NULL,
    last_activity_at = now(),
    updated_at = now()
WHERE issue_id = $1
  AND card_session.workspace_id = sqlc.arg(workspace_id)
  AND state = 'done_retained'
  AND EXISTS (
      SELECT 1
      FROM issue
      WHERE issue.id = card_session.issue_id
        AND issue.workspace_id = card_session.workspace_id
        AND issue_effective_status(issue.workspace_id, issue.status) <> 'done'
  )
  AND retain_until > now()
RETURNING *;

-- name: ExpireCardSessionsForWorkspace :many
-- The expiry worker is the only path allowed to close a session row. It can
-- never close an open todo/in_review generation.
UPDATE card_session
SET state = 'closed',
    closed_at = now(),
    updated_at = now()
WHERE state = 'done_retained'
  AND workspace_id = $1
  AND retain_until <= now()
RETURNING *;

-- name: ExpireCardSessions :many
-- Server-wide expiry pass. The state guard and close trigger ensure this can
-- only close rows that already completed their post-done retention window.
UPDATE card_session
SET state = 'closed',
    closed_at = now(),
    updated_at = now()
WHERE state = 'done_retained'
  AND retain_until <= now()
RETURNING *;

-- name: UpdateCardSessionProviderStateByTask :exec
UPDATE card_session AS cs
SET provider_session_id = COALESCE(NULLIF(sqlc.arg(provider_session_id), ''), cs.provider_session_id),
    work_dir = COALESCE(NULLIF(sqlc.arg(work_dir), ''), cs.work_dir),
    last_activity_at = now(),
    updated_at = now()
FROM agent_task_queue AS t
WHERE t.id = $1
  AND t.issue_id = cs.issue_id
  AND t.agent_id = cs.agent_id
  AND cs.state <> 'closed';

-- Token snapshots are selected without locks first. The service then locks
-- the issue, workspace, and session in that order before creating the system
-- comment, matching the issue-status trigger's lock order and preventing two
-- concurrent sweepers from publishing the same interval snapshot.
-- The one-minute SQL floor keeps the candidate scan bounded while the Go
-- settings parser applies each workspace's configured interval.
-- name: ListOpenCardSessionTokenStatsCandidateIDs :many
SELECT id, workspace_id, issue_id
FROM card_session
WHERE state = 'open'
  AND (
      last_token_stats_at IS NULL
      OR last_token_stats_at <= now() - interval '1 minute'
  )
ORDER BY last_token_stats_at NULLS FIRST, id
LIMIT sqlc.arg('limit');

-- name: LockIssueForCardSessionTokenStats :one
SELECT id
FROM issue
WHERE id = sqlc.arg(issue_id)
  AND workspace_id = sqlc.arg(workspace_id)
FOR UPDATE;

-- name: LockWorkspaceAndGetCardSessionTokenStats :one
SELECT cs.*, w.settings AS workspace_settings
FROM card_session AS cs
JOIN workspace AS w ON w.id = cs.workspace_id
WHERE cs.id = sqlc.arg(id)
  AND cs.state = 'open'
FOR UPDATE OF cs;

-- name: UpdateCardSessionTokenStatsAt :exec
UPDATE card_session
SET last_token_stats_at = sqlc.arg(published_at),
    updated_at = now()
WHERE id = sqlc.arg(id)
  AND state = 'open';
