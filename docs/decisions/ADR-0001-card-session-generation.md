## ADR-0001: Durable card session generations

- Status: Accepted for the first implementation slice
- Date: 2026-08-25

### Context

Card follow-up comments should continue the same logical generation instead of
rebuilding a card context for every turn. A provider process can still be
evicted or restarted, so a provider session ID alone cannot be the source of
truth. The lifecycle must also protect a work/review generation from arbitrary
close requests and bound retained resources.

### Decision

Store a server-owned `card_session` generation per issue and agent. Keep it
open through the work/review flow, retain it for a workspace-configured period
after `done`, and reopen the same generation when a comment moves the issue to
`in_review`. Enforce the workspace open-generation cap under a workspace row
lock. Make the comment reopen and issue activity update one database operation,
and enforce status-to-generation synchronization with a database trigger so
non-HTTP status writers follow the same contract.

Persist the latest provider session/workdir pointer when the daemon pins a
task session. Use provider-specific rejoin/resume when available and retain a
fresh-session fallback for rejected or poisoned sessions. Do not keep an
unbounded provider process alive merely because the logical generation is
retained.

While a generation is open, publish cumulative token statistics to the card at
the workspace-configured interval. Store the publication watermark on the
generation and commit it with the system comment so concurrent runtime
sweepers cannot duplicate a snapshot. Scope usage to the generation's opening
time and matching issue/agent rather than mixing previous generations.

### Consequences

The same generation survives `done → in_review`, daemon restarts, and provider
resume attempts while the retention window is valid. Expiry, capacity, and
intermediate token usage are observable and bounded. The first slice does not
yet provide a long-lived provider host or token-savings measurement; those
remain a separate runtime implementation and evaluation step.
