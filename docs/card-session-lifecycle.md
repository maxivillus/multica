## Card session lifecycle

Multica now keeps a durable per-issue agent generation in `card_session`. The
generation is the server-owned continuity identity; a provider process and its
in-memory cache are implementation details and may be restarted.

### Lifecycle contract

- A generation is `open` while its issue is in the non-terminal work/review
  flow. New task enqueue paths reuse that generation for the same issue and
  agent.
- A transition into the effective `done` category changes the generation to
  `done_retained`. The retention window is read from
  `workspace.settings.card_sessions.post_done_retention_hours`.
- A comment on a done issue atomically changes the issue to `in_review`. The
  same transaction also fires the database lifecycle trigger, which reopens
  the retained generation when it has not expired. Repeated comments and
  concurrent status writers are idempotent at the row boundary.
- Expired retained rows are closed by the server's periodic expiry sweep (and
  opportunistically during allocation). There is no arbitrary close operation
  for an open generation; an ordinary status or comment path cannot close it.
- `workspace.settings.card_sessions.max_open_sessions` limits the number of
  open or unexpired retained generations. Workspace-row locking serializes
  expiry, capacity checks, and new generation allocation.

The settings are bounded to 1–720 retention hours and 1–10,000 open sessions.
The defaults are 24 hours and 100 sessions.

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
