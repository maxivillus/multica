## ADR-0002: Continuous live card-session hosts

- Status: Implemented for Codex; AppSec verification pending
- Date: 2026-09-30
- Issue: NTSI-899
- Source: NTSI-713
- Supersedes: [ADR-0001](ADR-0001-card-session-generation.md)

### Context

The first card-session slice preserved a server-owned generation and a provider
session ID, then completed each task and stopped its provider process. That
supports a later provider resume, but it does not meet the requested behavior:
one live provider process and conversation must remain open between comments
until the configured idle timeout expires.

Each new comment remains a durable task. The server already links those tasks to
the card-session generation, and the schema has lease owner, epoch, and
heartbeat fields. Those are the right boundaries for routing each next turn to
one live owner without holding a general task slot throughout idle time.

### Decision

The alternatives were to keep only the provider session ID and resume a new
process for every comment, or to keep the current task worker blocked for the
whole idle window. The first does not preserve the live process; the second
holds a normal task slot and cannot dispatch a later queued turn once its
provider result has completed. The per-generation leased host is selected
because it preserves the process while leaving task scheduling available.

- Treat an open `card_session` generation as the identity of one live host.
- Give that generation to exactly one runtime through a fenced lease. The lease
  owner keeps one provider process and one provider conversation alive after a
  task turn completes. Its lease heartbeat does not extend the idle timeout.
- Keep each comment in the durable task queue. Route later tasks for the same
  generation to its lease owner and deliver them as ordered turns in the same
  provider conversation. Serialize turns for one generation.
- Release the normal task execution slot when a turn completes. Idle hosts use
  the existing card-session capacity bound rather than consuming task slots.
- Use `workspace.settings.card_sessions.idle_timeout_hours` as the sole idle
  expiry. Its default remains 24 hours and its range remains 1–999. Existing
  activity updates continue to define the deadline. On expiry, close the live
  provider process, release the lease, and close the generation.
- Use the persistent multi-turn host only for provider backends that expose
  that capability. Route other providers through their normal one-shot/resume
  execution path and mark the mode as `resume`; never report that path as a
  persistent host or fail a card task only because the optional capability is
  absent.
- If the runtime process itself crashes or is forcibly stopped, the operating
  system necessarily ends its provider process. After fencing the old lease,
  recover from the persisted provider session ID when the provider supports it
  and disclose the process-continuity gap; recovery is not described as the
  same process remaining alive.

The durable task queue, not an additional event log, remains the source of
pending user input. The provider process and its in-memory state are disposable
only after idle expiry or runtime loss; the server generation and queued tasks
remain authoritative.

### Task authorization boundary

The current `mat_` credential is bound to one `(agent_id, task_id,
workspace_id)` tuple and is revoked when that task reaches a terminal state. It
must not be inherited by a provider process that remains alive between turns.

The Codex host therefore starts a loopback credential broker. The provider
process receives only an opaque `mat_host_...` broker token and a broker URL;
the raw task token remains in daemon memory. Before each turn the daemon puts
the current task token into the broker. The broker forwards authenticated API
and local repo requests with that token, and rejects every request while the
host is idle. The daemon clears the token before publishing the turn result,
and host shutdown closes the broker. A later task installs its own token in the
same broker, so no revoked token is reused and no owner PAT is introduced.

Evidence: `server/pkg/agent/card_session_credential_proxy.go` implements the
loopback broker and fail-closed idle state; `server/pkg/agent/codex_persistent.go`
replaces the process environment token with the opaque broker token;
`server/internal/daemon/daemon.go` supplies the current task token only through
`ExecOptions`; and the existing server task-token middleware and revocation
queries remain unchanged.

The boundary still requires AppSec verification against the canonical
token-authenticated communication contract before release. The implementation
is intentionally fail-closed if a current `mat_` token is not supplied.

### Consequences

Comments posted during an idle window continue in the same live provider
process and conversation. Idle sessions no longer occupy task execution slots.
Lease fencing prevents two runtimes from owning one generation at once, while
the configured card-session capacity bounds retained hosts.

Provider adapters, daemon task dispatch, credential brokering, and card-session
lease operations must change together. Tests prove process identity across
consecutive turns, broker rotation and idle rejection, serialization, and the
workspace-configured expiry boundary. The idle-timeout setting and its
established bounds do not change.
