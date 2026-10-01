## ADR-0001: Durable card session generations

- Status: Accepted for the first implementation slice
- Date: 2026-08-25

### Context

Card follow-up work should continue a durable per-issue generation. A provider
process can still be evicted or restarted, so a provider session ID alone cannot
be the source of truth. The lifecycle also needs to bound retained resources and
preserve queued work when a workspace reaches its session limit.

### Decision

Store a server-owned `card_session` generation per issue and agent. Open it when
an assigned issue enters `todo`; keep it open through `todo`, `in_progress`,
`in_review`, and `done`. Do not create a generation for a new issue in
`backlog`. Pause the generation in `backlog`, `blocked`, and `cancelled`; resume
the same generation when the issue returns to active work before the idle
timeout expires.

Do not backfill empty generations for issues already assigned to `todo` during
upgrade. The next task start or comment lazily allocates the first generation.

Moving an issue to `cancelled` immediately cancels its unfinished tasks. The
cancelled generation stays paused with its provider resume data until expiry.
A follow-up comment on `done` or `cancelled` moves the issue to `in_review` and
resumes the same generation when it is still within the idle timeout.

Use one workspace setting, `idle_timeout_hours`, with a default of 24 hours and
a range of 1–999. Never expire a generation with `running`, `dispatched`, or
`waiting_local_directory` work. In an active status, queued or deferred work
also protects the generation. A paused generation for `backlog`, `blocked`, or
`cancelled` may expire with queued or deferred work; that work remains queued
and starts a new generation when the issue returns to an active status. A
capacity-paused generation on an active issue keeps its generation while
queued or deferred work remains. Use `max_open_sessions` to limit open
generations; its default is 100. Keep capacity-blocked tasks deferred until a
slot becomes available. Generation allocation and issue-driven lifecycle
changes serialize on the workspace row lock. The global idle-expiry sweeper
does not acquire that lock; its guarded update and close trigger reject expiry
when the idle deadline has not passed or unfinished work still protects the
generation.

Persist each task's provider session ID and work directory on the exact
generation linked to that task. Use provider-specific resume or rejoin after a
daemon or process restart when supported, with a fresh-session fallback when
resume fails. The generation survives restarts; it does not keep a provider
process running for its entire lifetime.

Refresh cumulative token totals in the background after each accepted provider
usage update. Read totals from `task_usage`, scope them to the generation and
its tasks, and use a recovery sweep after restart. Do not publish token
statistics as card comments or agent input. The feature does not require an
agent final-summary comment and does not include token-savings measurement.

### Consequences

The same generation survives active status changes and can resume after a
temporary pause. A stale generation is closed before a new one is allocated;
active and queued work is not interrupted or lost to idle expiry. Provider
state remains scoped to one task and generation. Token totals update without
creating new agent triggers.

The implementation keeps using the existing task queue and provider resume
paths. It does not add a separate always-running provider host. Lifecycle
diagnostics remain separately configurable and avoid session identifiers,
work directories, prompts, and token payloads in metric labels.
