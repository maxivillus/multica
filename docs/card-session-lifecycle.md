## Card session lifecycle

Multica now keeps a durable per-issue agent generation in `card_session`. The
generation is the server-owned continuity identity; a provider process and its
in-memory cache are implementation details and may be restarted.

### Lifecycle contract

- A generation is `open` while its issue is in the non-terminal work/review
  flow. New task enqueue paths reuse that generation for the same issue and
  agent.
- A transition into the effective `done` or `cancelled` category changes the
  generation to `done_retained`. The retention window is read from
  `workspace.settings.card_sessions.post_done_retention_hours`.
- A comment on a done or cancelled issue atomically changes the issue to
  `in_review`. The same transaction also fires the database lifecycle trigger,
  which reopens the retained generation when it has not expired. Repeated
  comments and concurrent status writers are idempotent at the row boundary.
- Expired retained rows are closed by the server's periodic expiry sweep (and
  opportunistically during allocation). There is no arbitrary close operation
  for an open generation; an ordinary status or comment path cannot close it.
- `workspace.settings.card_sessions.max_open_sessions` limits the number of
  open or unexpired retained generations. Workspace-row locking serializes
  expiry, capacity checks, and new generation allocation.
- While a generation is `open`, the runtime sweeper periodically writes a
  cumulative system comment with input, output, cache-read, cache-write, and
  task counts from `task_usage`. The interval is configured by
  `workspace.settings.card_sessions.token_stats_interval_minutes`; it defaults
  to 15 minutes and is bounded to 1–1,440 minutes. The snapshot is scoped to
  the generation's `opened_at` boundary and the matching issue/agent.
- The publication watermark is stored on the generation row and advanced in
  the same transaction as the comment. This makes concurrent server sweepers
  idempotent and keeps these system comments out of ordinary agent-trigger
  reconciliation. A due session with no usage advances the watermark without
  posting a zero-token comment.

The terminal-retention setting is bounded to 1–720 hours, open sessions to
1–10,000, and token-statistics interval to 1–1,440 minutes. The defaults are
24 hours, 100 sessions, and 15 minutes.

### Provider continuity

`PinTaskSession` mirrors the provider session ID and work directory into the
generation record. Existing provider-specific resume/rejoin behavior remains
the recovery path after a daemon or process restart. This follows the useful
parts of the reviewed harnesses: a server-owned identity and durable lifecycle
state from Codex app-server, transcript/session rejoin from Claude, and
crash-safe event/driver separation from DeepSeek Harness.

This change does not claim that a provider process or prompt cache stays alive
for the entire retention window. The current executor still runs provider turns
through the task queue. A future live-host implementation must use the
generation's lease fields and provider capability checks, while preserving the
durable generation as the source of truth and retaining the existing fresh
session fallback.
