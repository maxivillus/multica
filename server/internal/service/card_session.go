package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/cardsession"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var (
	// ErrCardSessionCapacity tells enqueue paths to keep the task deferred until
	// a slot opens instead of creating a session over the workspace limit.
	ErrCardSessionCapacity  = errors.New("card session capacity exhausted")
	ErrCardSessionStatus    = errors.New("issue status does not allow an active card session")
	ErrCardSessionInactive  = ErrCardSessionStatus
	ErrIssueTaskCancelled   = errors.New("issue is cancelled; agent task was not created")
	ErrIssueTaskUnavailable = errors.New("issue is unavailable; agent task was not created")
)

const (
	cardSessionEventEnsure             = "ensure"
	cardSessionEventStatus             = "status"
	cardSessionEventExpire             = "expire"
	cardSessionEventProviderPin        = "provider_pin"
	cardSessionEventCapacity           = "capacity"
	cardSessionCapacityRetryDelay      = 30 * time.Second
	cardSessionCapacityWaiterBatchSize = 20
)

func (s *TaskService) observeCardSession(ctx context.Context, event, result string, session db.CardSession, attrs ...any) {
	if s == nil {
		return
	}
	s.observeCardSessionIdentity(ctx, event, result, session.IssueID, session.ID, session.AgentID, session.Generation, session.Provider, attrs...)
}

func (s *TaskService) observeCardSessionIdentity(ctx context.Context, event, result string, issueID, sessionID, agentID pgtype.UUID, generation int64, provider string, attrs ...any) {
	if s == nil || !s.CardSessionObservabilityEnabled {
		return
	}
	event = normalizeCardSessionEvent(event)
	result = normalizeCardSessionResult(result)
	if s.CardSessionMetrics != nil {
		s.CardSessionMetrics.RecordEvent(event, result)
	}

	logAttrs := make([]any, 0, 14+len(attrs))
	logAttrs = append(logAttrs, "event", event, "result", result)
	if value := util.UUIDToString(issueID); value != "" {
		logAttrs = append(logAttrs, "issue_id", value)
	}
	if value := util.UUIDToString(sessionID); value != "" {
		logAttrs = append(logAttrs, "card_session_id", value)
	}
	if value := util.UUIDToString(agentID); value != "" {
		logAttrs = append(logAttrs, "agent_id", value)
	}
	if generation > 0 {
		logAttrs = append(logAttrs, "generation", generation)
	}
	if provider != "" {
		logAttrs = append(logAttrs, "provider", provider)
	}
	logAttrs = append(logAttrs, attrs...)
	slog.InfoContext(ctx, "card session diagnostic", logAttrs...)
}

func (s *TaskService) observeCardSessionDuration(event, result string, duration time.Duration) {
	if s == nil || !s.CardSessionObservabilityEnabled || s.CardSessionMetrics == nil {
		return
	}
	s.CardSessionMetrics.ObserveOperation(event, result, duration)
}

// ObserveCardSessionProviderPin records the daemon's provider-state pin after
// the surrounding transaction has committed. The task ID is safe diagnostic
// context; provider session IDs and work directories are intentionally omitted.
func (s *TaskService) ObserveCardSessionProviderPin(ctx context.Context, taskID pgtype.UUID, result string) {
	if s == nil || !s.CardSessionObservabilityEnabled {
		return
	}
	result = normalizeCardSessionResult(result)
	if s.CardSessionMetrics != nil {
		s.CardSessionMetrics.RecordEvent(cardSessionEventProviderPin, result)
	}
	attrs := []any{"event", cardSessionEventProviderPin, "result", result}
	if value := util.UUIDToString(taskID); value != "" {
		attrs = append(attrs, "task_id", value)
	}
	slog.InfoContext(ctx, "card session diagnostic", attrs...)
}

// ObserveCardSessionProviderPinDuration records the provider-state pin and
// its end-to-end duration. Callers use this when the pin is performed inline
// with another transaction rather than through UpdateCardSessionProviderState.
func (s *TaskService) ObserveCardSessionProviderPinDuration(ctx context.Context, taskID pgtype.UUID, result string, duration time.Duration) {
	s.ObserveCardSessionProviderPin(ctx, taskID, result)
	s.observeCardSessionDuration(cardSessionEventProviderPin, result, duration)
}

func normalizeCardSessionEvent(event string) string {
	switch event {
	case cardSessionEventEnsure, cardSessionEventStatus, cardSessionEventExpire, cardSessionEventProviderPin, cardSessionEventCapacity:
		return event
	default:
		return "other"
	}
}

func normalizeCardSessionResult(result string) string {
	switch result {
	case "created", "reused", "paused", "resumed", "expired", "updated",
		"rejected", "noop", "unavailable", "error":
		return result
	default:
		return "other"
	}
}

// EnsureCardSession creates or reopens the server-owned generation for an
// issue/agent pair. Workspace row locking makes the capacity check and
// generation allocation atomic across concurrent comment/status triggers.
func (s *TaskService) EnsureCardSession(ctx context.Context, issueID, workspaceID, agentID pgtype.UUID, provider string) (db.CardSession, error) {
	started := time.Now()
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return db.CardSession{}, nil
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventEnsure, "error", issueID, pgtype.UUID{}, agentID, 0, provider)
		s.observeCardSessionDuration(cardSessionEventEnsure, "error", time.Since(started))
		return db.CardSession{}, fmt.Errorf("begin card session: %w", err)
	}
	defer tx.Rollback(ctx)
	created, outcome, err := s.ensureCardSessionWithQueries(ctx, s.Queries.WithTx(tx), issueID, workspaceID, agentID, provider)
	if err != nil {
		if errors.Is(err, ErrCardSessionCapacity) {
			s.observeCardSessionIdentity(ctx, cardSessionEventCapacity, "rejected", issueID, pgtype.UUID{}, agentID, 0, provider)
			s.observeCardSessionDuration(cardSessionEventCapacity, "rejected", time.Since(started))
		} else {
			s.observeCardSessionIdentity(ctx, cardSessionEventEnsure, "error", issueID, pgtype.UUID{}, agentID, 0, provider)
			s.observeCardSessionDuration(cardSessionEventEnsure, "error", time.Since(started))
		}
		return db.CardSession{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventEnsure, "error", issueID, created.ID, agentID, created.Generation, provider)
		s.observeCardSessionDuration(cardSessionEventEnsure, "error", time.Since(started))
		return db.CardSession{}, fmt.Errorf("commit card session: %w", err)
	}
	s.observeCardSession(ctx, cardSessionEventEnsure, outcome, created)
	s.observeCardSessionDuration(cardSessionEventEnsure, outcome, time.Since(started))
	return created, nil
}

// ensureCardSessionWithQueries performs the lifecycle operation against the
// supplied query handle without beginning or committing a transaction. This
// is used by issue creation, which must allocate the generation in the same
// transaction as the deferred task and the issue row itself.
func (s *TaskService) ensureCardSessionWithQueries(ctx context.Context, q *db.Queries, issueID, workspaceID, agentID pgtype.UUID, provider string) (db.CardSession, string, error) {
	if q == nil {
		return db.CardSession{}, "", errors.New("card session queries are not configured")
	}

	workspace, err := q.LockWorkspaceForCardSession(ctx, workspaceID)
	if err != nil {
		return db.CardSession{}, "", fmt.Errorf("lock workspace for card session: %w", err)
	}
	if _, err := q.LockIssueForCardSession(ctx, db.LockIssueForCardSessionParams{
		ID: issueID, WorkspaceID: workspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.CardSession{}, "", ErrCardSessionInactive
		}
		return db.CardSession{}, "", fmt.Errorf("lock issue for card session: %w", err)
	}
	settings, err := cardsession.Parse(workspace.Settings)
	if err != nil {
		return db.CardSession{}, "", err
	}
	issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: issueID, WorkspaceID: workspaceID})
	if err != nil {
		return db.CardSession{}, "", fmt.Errorf("load issue for card session: %w", err)
	}
	if !issuestatus.AllowsAgentTask(ctx, q, workspaceID, issue.Status) {
		return db.CardSession{}, "", ErrCardSessionInactive
	}

	// Expire only this workspace's idle rows while its lock is held. This keeps
	// stale sessions from consuming capacity without mutating other workspaces.
	if _, err := q.ExpireCardSessionsForWorkspace(ctx, workspaceID); err != nil {
		return db.CardSession{}, "", fmt.Errorf("expire card sessions: %w", err)
	}

	current, err := q.GetResumableCardSession(ctx, db.GetResumableCardSessionParams{
		IssueID:     issueID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
	})
	if err == nil {
		if current.State != "open" {
			openCount, err := q.CountOpenCardSessions(ctx, workspaceID)
			if err != nil {
				return db.CardSession{}, "", fmt.Errorf("count open card sessions: %w", err)
			}
			if !settings.CapacityAvailable(openCount) {
				return db.CardSession{}, "", fmt.Errorf("%w: workspace %s has %d open sessions (limit %d)",
					ErrCardSessionCapacity,
					util.UUIDToString(workspaceID),
					openCount,
					settings.MaxOpenSessions,
				)
			}
		}
		current, err = q.TouchCardSession(ctx, current.ID)
		if err != nil {
			return db.CardSession{}, "", fmt.Errorf("touch card session: %w", err)
		}
		return current, "reused", nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return db.CardSession{}, "", fmt.Errorf("load card session: %w", err)
	}

	openCount, err := q.CountOpenCardSessions(ctx, workspaceID)
	if err != nil {
		return db.CardSession{}, "", fmt.Errorf("count open card sessions: %w", err)
	}
	if !settings.CapacityAvailable(openCount) {
		return db.CardSession{}, "", fmt.Errorf("%w: workspace %s has %d open sessions (limit %d)",
			ErrCardSessionCapacity,
			util.UUIDToString(workspaceID),
			openCount,
			settings.MaxOpenSessions,
		)
	}

	generation := int64(1)
	latest, latestErr := q.GetLatestCardSession(ctx, db.GetLatestCardSessionParams{
		IssueID:     issueID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
	})
	if latestErr == nil {
		generation = latest.Generation + 1
	} else if !errors.Is(latestErr, pgx.ErrNoRows) {
		return db.CardSession{}, "", fmt.Errorf("load latest card session: %w", latestErr)
	}

	created, err := q.CreateCardSession(ctx, db.CreateCardSessionParams{
		WorkspaceID: workspaceID,
		IssueID:     issueID,
		AgentID:     agentID,
		Generation:  generation,
		Provider:    provider,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.CardSession{}, "", ErrCardSessionInactive
		}
		return db.CardSession{}, "", fmt.Errorf("create card session: %w", err)
	}
	return created, "created", nil
}

// SyncIssueCardSessions records the issue's current lifecycle status. The
// migration trigger performs the same transition for direct SQL and webhook
// writes; this call keeps application transitions observable and idempotent.
func (s *TaskService) SyncIssueCardSessions(ctx context.Context, issueID, workspaceID pgtype.UUID) error {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return nil
	}
	started := time.Now()
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventStatus, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "")
		s.observeCardSessionDuration(cardSessionEventStatus, "error", time.Since(started))
		return fmt.Errorf("begin card session status sync: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	if _, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID); err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventStatus, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "")
		s.observeCardSessionDuration(cardSessionEventStatus, "error", time.Since(started))
		return fmt.Errorf("lock workspace for card session status sync: %w", err)
	}
	rows, err := qtx.SyncCardSessionsForIssue(ctx, db.SyncCardSessionsForIssueParams{
		IssueID:     issueID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventStatus, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "")
		s.observeCardSessionDuration(cardSessionEventStatus, "error", time.Since(started))
		return fmt.Errorf("sync card session status: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventStatus, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "")
		s.observeCardSessionDuration(cardSessionEventStatus, "error", time.Since(started))
		return err
	}
	for _, row := range rows {
		result := "paused"
		if row.State == "open" {
			result = "resumed"
		}
		s.observeCardSession(ctx, cardSessionEventStatus, result, row)
	}
	result := "updated"
	if len(rows) == 0 {
		result = "noop"
	}
	s.observeCardSessionDuration(cardSessionEventStatus, result, time.Since(started))
	return nil
}

// ExpireCardSessions closes idle generations after each workspace's configured
// timeout, while keeping any generation with unfinished work open.
func (s *TaskService) ExpireCardSessions(ctx context.Context) (int64, error) {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return 0, nil
	}
	started := time.Now()
	rows, err := s.Queries.ExpireCardSessions(ctx)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventExpire, "error", pgtype.UUID{}, pgtype.UUID{}, pgtype.UUID{}, 0, "")
		s.observeCardSessionDuration(cardSessionEventExpire, "error", time.Since(started))
		return 0, fmt.Errorf("expire card sessions: %w", err)
	}
	for _, row := range rows {
		s.observeCardSession(ctx, cardSessionEventExpire, "expired", row)
	}
	result := "expired"
	if len(rows) == 0 {
		result = "noop"
	}
	s.observeCardSessionDuration(cardSessionEventExpire, result, time.Since(started))
	return int64(len(rows)), nil
}

// WakeCardSessionCapacityWaiters admits due issue tasks one at a time through
// EnsureCardSession, which owns the workspace lock and capacity check. Tasks
// stay deferred when the workspace is full or the issue is paused.
func (s *TaskService) WakeCardSessionCapacityWaiters(ctx context.Context, runtimeIDs []pgtype.UUID) error {
	if s == nil || s.Queries == nil || s.TxStarter == nil || len(runtimeIDs) == 0 {
		return nil
	}
	waiters, err := s.Queries.ListDueCardSessionCapacityWaitersForRuntimes(ctx, db.ListDueCardSessionCapacityWaitersForRuntimesParams{
		RuntimeIds:  runtimeIDs,
		WaiterLimit: int32(cardSessionCapacityWaiterBatchSize),
	})
	if err != nil {
		return fmt.Errorf("list card-session capacity waiters: %w", err)
	}
	for _, waiter := range waiters {
		_, err := s.EnsureCardSession(ctx, waiter.IssueID, waiter.WorkspaceID, waiter.AgentID, waiter.RuntimeMode)
		if err != nil {
			if _, scheduleErr := s.Queries.RetryCardSessionCapacityWaiter(ctx, db.RetryCardSessionCapacityWaiterParams{
				ID:                waiter.ID,
				RetryDelaySeconds: cardSessionCapacityRetryDelay.Seconds(),
			}); scheduleErr != nil {
				return fmt.Errorf("reschedule card-session capacity waiter: %w", scheduleErr)
			}
			if !errors.Is(err, ErrCardSessionCapacity) && !errors.Is(err, ErrCardSessionInactive) {
				slog.Warn("card-session capacity waiter could not open session",
					"task_id", util.UUIDToString(waiter.ID),
					"issue_id", util.UUIDToString(waiter.IssueID),
					"error", err,
				)
			}
			continue
		}
		if _, err := s.Queries.ReleaseCardSessionCapacityWaiter(ctx, waiter.ID); err != nil {
			return fmt.Errorf("release card-session capacity waiter: %w", err)
		}
	}
	return nil
}

// UpdateCardSessionProviderState mirrors the task pin into the durable
// generation record. A provider process may be replaced, but the generation
// keeps the latest provider session/workdir pointer for reconnect.
func (s *TaskService) UpdateCardSessionProviderState(ctx context.Context, taskID pgtype.UUID, providerSessionID, workDir string) error {
	if s == nil || s.Queries == nil {
		return nil
	}
	started := time.Now()
	err := s.Queries.UpdateCardSessionProviderStateByTask(ctx, db.UpdateCardSessionProviderStateByTaskParams{
		ID:                taskID,
		ProviderSessionID: providerSessionID,
		WorkDir:           workDir,
	})
	result := "updated"
	if err != nil {
		result = "error"
	}
	s.ObserveCardSessionProviderPinDuration(ctx, taskID, result, time.Since(started))
	return err
}

// bindStartedIssueTaskToCardSession fixes the issue task's generation before
// the start transaction commits. Backlog comment/mention tasks remain ordinary
// tasks without a persistent card session or provider resume state.
func (s *TaskService) bindStartedIssueTaskToCardSession(ctx context.Context, q *db.Queries, task db.AgentTaskQueue) (db.AgentTaskQueue, error) {
	if !task.IssueID.Valid {
		return task, nil
	}
	issue, err := q.GetIssue(ctx, task.IssueID)
	if err != nil {
		return db.AgentTaskQueue{}, fmt.Errorf("load issue for started task card session: %w", err)
	}
	agent, err := q.GetAgent(ctx, task.AgentID)
	if err != nil {
		return db.AgentTaskQueue{}, fmt.Errorf("load agent for started task card session: %w", err)
	}
	session, _, err := s.ensureCardSessionWithQueries(ctx, q, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
	if errors.Is(err, ErrCardSessionStatus) {
		return task, nil
	}
	if err != nil {
		return db.AgentTaskQueue{}, fmt.Errorf("ensure card session for started task: %w", err)
	}
	bound, err := q.BindAgentTaskToCardSession(ctx, db.BindAgentTaskToCardSessionParams{
		TaskID:        task.ID,
		CardSessionID: session.ID,
	})
	if err != nil {
		return db.AgentTaskQueue{}, fmt.Errorf("bind started task to card session: %w", err)
	}
	return bound, nil
}

// TouchCardSessionActivityForTask advances activity after provider usage or a
// committed task transition. The update runs after the task transaction so it
// cannot invert task and card-session lock order.
func (s *TaskService) TouchCardSessionActivityForTask(ctx context.Context, taskID pgtype.UUID) error {
	return s.touchCardSessionActivityForTaskIDs(ctx, []pgtype.UUID{taskID})
}

func (s *TaskService) touchCardSessionActivityForTasks(ctx context.Context, tasks []db.AgentTaskQueue) {
	ids := make([]pgtype.UUID, 0, len(tasks))
	for _, task := range tasks {
		if task.IssueID.Valid {
			ids = append(ids, task.ID)
		}
	}
	if err := s.touchCardSessionActivityForTaskIDs(ctx, ids); err != nil {
		slog.Warn("failed to touch card session after task transition", "error", err)
	}
}

func (s *TaskService) touchCardSessionActivityForTaskIDs(ctx context.Context, taskIDs []pgtype.UUID) error {
	if s == nil || s.Queries == nil || len(taskIDs) == 0 {
		return nil
	}
	return s.Queries.TouchCardSessionsForTasks(ctx, taskIDs)
}
