## Card session lifecycle

Multica stores a durable per-issue agent generation in `card_session`. The
generation is the server-owned continuity identity. A provider process and its
in-memory cache may be restarted; they are not the source of truth.

### Lifecycle contract

- An assigned issue gets a generation when it enters `todo`. The generation
  stays `open` through `todo`, `in_progress`, `in_review`, and `done`.
- Upgrades do not backfill empty generations for issues that were already
  assigned in `todo`; the next task start or comment lazily creates the first
  generation for that issue.
- A new issue in `backlog` does not get a generation. Explicit comment or
  mention runs in `backlog` remain ordinary tasks and are not bound to a
  persistent card session.
- Moving an issue to `backlog`, `blocked`, or `cancelled` pauses its generation.
  Moving it back to an active status resumes the same generation while it has
  not expired. A new generation is created after expiry.
- Moving an issue to `cancelled` immediately stops its unfinished tasks on that
  issue. It does not stop completed tasks or tasks on other issues. The
  generation and provider resume state remain paused until expiry.
- A generation expires after the configured idle period when no task is
  running, dispatched, or waiting for a local directory. In an active status,
  a queued or deferred task also keeps its generation. In a paused status, the
  generation can expire while work waits; that work opens a new generation when
  the issue returns to an active status. Expiry never interrupts active work or
  removes queued work.
- `workspace.settings.card_sessions.idle_timeout_hours` is the single idle
  timeout. It defaults to 24 hours and accepts values from 1 to 999.
  `workspace.settings.card_sessions.max_open_sessions` defaults to 100 and
  limits open generations; paused generations do not use an open slot.
- A comment on a `done` or `cancelled` issue reopens it as `in_review` and
  resumes its retained generation if the idle timeout has not passed. Repeated
  comments and concurrent status writers are idempotent at the database row.

### Token statistics

After each accepted provider usage update, the server refreshes the generation's
cumulative token totals in the background from `task_usage`. The refresh is
scoped to the generation's start time and the tasks bound to that generation.
A recovery sweep refreshes totals missed during a server restart. Statistics do
not create card comments or agent input. This lifecycle does not measure or
claim token savings, and it does not require a final summary comment from the
agent.

### Diagnostics

The lifecycle has a separate observability surface controlled by
`MULTICA_CARD_SESSION_OBSERVABILITY_ENABLED` (default `true`). When enabled,
the server emits structured events for generation allocation and reuse, status
changes, expiry, provider-state pins, and capacity handling. With `METRICS_ADDR`
enabled it registers the bounded Prometheus families
`multica_card_session_events_total` and
`multica_card_session_operation_duration_seconds`.

Metric labels use fixed event/result values and do not contain issue IDs,
session IDs, provider session IDs, work directories, prompts, or token payloads.
The flag disables these additional logs and metric families; ordinary server
logs and existing task/LLM metrics are unchanged.

The idle timeout is bounded to 1–999 hours. The open-generation limit is
bounded to 1–10,000. Their defaults are 24 hours and 100 generations.

### Provider continuity

When a task is pinned, Multica stores its provider session ID and work directory
on the exact card generation linked to that task. On a later task, the runtime
uses the provider's resume or rejoin mechanism when it is available. If the
provider cannot resume that state, Multica can start a fresh provider session.
The daemon pins the same state as soon as it is observed and repeats the pin in
the transaction that completes or fails a task. This closes the hand-off window
where a follow-up comment could be claimed after the task ended but before an
asynchronous pin reached the database. The durable generation remains
available across daemon or process restarts, but the implementation does not
keep a provider process alive for the entire idle window.
