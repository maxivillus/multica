package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/cardsession"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var (
	// ErrCardSessionCapacity is returned before a task is inserted when a new
	// generation would exceed the workspace's configured open-session cap.
	ErrCardSessionCapacity = errors.New("card session capacity exhausted")
	// ErrCardSessionCloseForbidden makes the lifecycle rule explicit for future
	// close endpoints: an open generation can only enter retention through a
	// terminal issue transition, never through an arbitrary close request.
	ErrCardSessionCloseForbidden = errors.New("card session can only close after issue reaches done or cancelled")
)

const (
	cardSessionEventEnsure      = "ensure"
	cardSessionEventTerminal    = "terminal"
	cardSessionEventReopen      = "reopen"
	cardSessionEventExpire      = "expire"
	cardSessionEventProviderPin = "provider_pin"
	cardSessionEventTokenStats  = "token_stats"
	cardSessionEventCapacity    = "capacity"
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
	case cardSessionEventEnsure, cardSessionEventTerminal, cardSessionEventReopen,
		cardSessionEventExpire, cardSessionEventProviderPin, cardSessionEventTokenStats,
		cardSessionEventCapacity:
		return event
	default:
		return "other"
	}
}

func normalizeCardSessionResult(result string) string {
	switch result {
	case "created", "reused", "reopened", "ready", "retained", "expired", "updated",
		"published", "empty", "rejected", "noop", "unavailable", "error":
		return result
	default:
		return "other"
	}
}

func normalizeCardSessionReopenSource(source string) string {
	switch source {
	case "status_transition", "comment_after_terminal", "agent_comment_after_terminal":
		return source
	default:
		return "other"
	}
}

func normalizeCardSessionTerminalStatus(status string) string {
	switch status {
	case issuestatus.Done, issuestatus.Cancelled:
		return status
	default:
		return "other"
	}
}

// EnsureCardSession creates or reopens the server-owned generation for an
// issue/agent pair. Workspace row locking makes the capacity check and
// generation allocation atomic across concurrent comment/status triggers.
func (s *TaskService) EnsureCardSession(ctx context.Context, issueID, workspaceID, agentID pgtype.UUID, provider string) (db.CardSession, error) {
	started := time.Now()
	// A few transaction-scoped and compatibility service instances are
	// intentionally built without a transaction starter (for example, a
	// caller that already owns the transaction, or a read-only test service).
	// Card sessions are additive state, so those instances must retain the
	// pre-session enqueue behavior instead of rejecting an otherwise valid task.
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
	settings, err := cardsession.Parse(workspace.Settings)
	if err != nil {
		return db.CardSession{}, "", err
	}

	// Expire only this workspace's retained rows while its lock is held. This
	// keeps stale rows from consuming capacity without allowing one enqueue to
	// mutate another workspace.
	if _, err := q.ExpireCardSessionsForWorkspace(ctx, workspaceID); err != nil {
		return db.CardSession{}, "", fmt.Errorf("expire card sessions: %w", err)
	}

	current, err := q.GetResumableCardSession(ctx, db.GetResumableCardSessionParams{
		IssueID:     issueID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
	})
	if err == nil {
		if current.State == "done_retained" {
			current, err = q.ReopenCardSession(ctx, current.ID)
			if err != nil {
				return db.CardSession{}, "", fmt.Errorf("reopen card session: %w", err)
			}
			return current, "reopened", nil
		} else {
			current, err = q.TouchCardSession(ctx, current.ID)
			if err != nil {
				return db.CardSession{}, "", fmt.Errorf("touch card session: %w", err)
			}
			return current, "reused", nil
		}
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
		return db.CardSession{}, "", fmt.Errorf("create card session: %w", err)
	}
	return created, "created", nil
}

// MarkIssueCardSessionsTerminal moves every open generation for an issue into
// the configured terminal retention state. It is intentionally callable only
// by status-write code; there is no generic close operation for open sessions.
func (s *TaskService) MarkIssueCardSessionsTerminal(ctx context.Context, issueID, workspaceID pgtype.UUID) error {
	return s.MarkIssueCardSessionsTerminalWithStatus(ctx, issueID, workspaceID, "")
}

// MarkIssueCardSessionsTerminalWithStatus is the status-aware form used by
// issue status writers. The status is a bounded diagnostic attribute, not a
// source of lifecycle authority; the database update remains authoritative.
func (s *TaskService) MarkIssueCardSessionsTerminalWithStatus(ctx context.Context, issueID, workspaceID pgtype.UUID, terminalStatus string) error {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return nil
	}
	started := time.Now()
	terminalStatus = normalizeCardSessionTerminalStatus(terminalStatus)
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventTerminal, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "terminal_status", terminalStatus)
		s.observeCardSessionDuration(cardSessionEventTerminal, "error", time.Since(started))
		return fmt.Errorf("begin card session close: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	workspace, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventTerminal, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "terminal_status", terminalStatus)
		s.observeCardSessionDuration(cardSessionEventTerminal, "error", time.Since(started))
		return fmt.Errorf("lock workspace for card session close: %w", err)
	}
	settings, err := cardsession.Parse(workspace.Settings)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventTerminal, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "terminal_status", terminalStatus)
		s.observeCardSessionDuration(cardSessionEventTerminal, "error", time.Since(started))
		return err
	}
	rows, err := qtx.MarkCardSessionsTerminal(ctx, db.MarkCardSessionsTerminalParams{
		IssueID:        issueID,
		WorkspaceID:    workspaceID,
		RetentionHours: int64(settings.PostDoneRetentionHours),
	})
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventTerminal, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "terminal_status", terminalStatus)
		s.observeCardSessionDuration(cardSessionEventTerminal, "error", time.Since(started))
		return fmt.Errorf("retain card sessions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventTerminal, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "terminal_status", terminalStatus)
		s.observeCardSessionDuration(cardSessionEventTerminal, "error", time.Since(started))
		return err
	}
	if len(rows) == 0 {
		s.observeCardSessionIdentity(ctx, cardSessionEventTerminal, "noop", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "terminal_status", terminalStatus)
	} else {
		for _, row := range rows {
			s.observeCardSession(ctx, cardSessionEventTerminal, "retained", row, "terminal_status", terminalStatus)
		}
	}
	s.observeCardSessionDuration(cardSessionEventTerminal, func() string {
		if len(rows) == 0 {
			return "noop"
		}
		return "retained"
	}(), time.Since(started))
	return nil
}

// ReopenIssueCardSessions preserves the same generation when a terminal issue
// is reopened during its configured retention window.
func (s *TaskService) ReopenIssueCardSessions(ctx context.Context, issueID, workspaceID pgtype.UUID) error {
	return s.ReopenIssueCardSessionsWithSource(ctx, issueID, workspaceID, "status_transition")
}

// ReopenIssueCardSessionsWithSource records whether a status transition or a
// terminal-comment readback reopened a retained generation. The source is
// diagnostic context only; the database row and trigger own the decision.
func (s *TaskService) ReopenIssueCardSessionsWithSource(ctx context.Context, issueID, workspaceID pgtype.UUID, source string) error {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return nil
	}
	started := time.Now()
	source = normalizeCardSessionReopenSource(source)
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventReopen, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "source", source)
		s.observeCardSessionDuration(cardSessionEventReopen, "error", time.Since(started))
		return fmt.Errorf("begin card session reopen: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	if _, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID); err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventReopen, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "source", source)
		s.observeCardSessionDuration(cardSessionEventReopen, "error", time.Since(started))
		return fmt.Errorf("lock workspace for card session reopen: %w", err)
	}
	rows, err := qtx.ReopenCardSessionsForIssue(ctx, db.ReopenCardSessionsForIssueParams{
		IssueID:     issueID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventReopen, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "source", source)
		s.observeCardSessionDuration(cardSessionEventReopen, "error", time.Since(started))
		return fmt.Errorf("reopen card sessions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		s.observeCardSessionIdentity(ctx, cardSessionEventReopen, "error", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "source", source)
		s.observeCardSessionDuration(cardSessionEventReopen, "error", time.Since(started))
		return err
	}
	if len(rows) == 0 {
		s.observeCardSessionIdentity(ctx, cardSessionEventReopen, "noop", issueID, pgtype.UUID{}, pgtype.UUID{}, 0, "", "source", source)
	} else {
		for _, row := range rows {
			s.observeCardSession(ctx, cardSessionEventReopen, "reopened", row, "source", source)
		}
	}
	s.observeCardSessionDuration(cardSessionEventReopen, func() string {
		if len(rows) == 0 {
			return "noop"
		}
		return "reopened"
	}(), time.Since(started))
	return nil
}

// ExpireCardSessions closes retained generations whose terminal window has
// elapsed. The server sweeper calls this globally; a missing table is ignored
// so a rolling deployment can start the new binary before migration 420 has
// reached every database node.
func (s *TaskService) ExpireCardSessions(ctx context.Context) (int64, error) {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return 0, nil
	}
	started := time.Now()
	rows, err := s.Queries.ExpireCardSessions(ctx)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			s.observeCardSessionIdentity(ctx, cardSessionEventExpire, "unavailable", pgtype.UUID{}, pgtype.UUID{}, pgtype.UUID{}, 0, "")
			s.observeCardSessionDuration(cardSessionEventExpire, "unavailable", time.Since(started))
			return 0, nil
		}
		s.observeCardSessionIdentity(ctx, cardSessionEventExpire, "error", pgtype.UUID{}, pgtype.UUID{}, pgtype.UUID{}, 0, "")
		s.observeCardSessionDuration(cardSessionEventExpire, "error", time.Since(started))
		return 0, fmt.Errorf("expire card sessions: %w", err)
	}
	if len(rows) == 0 {
		s.observeCardSessionIdentity(ctx, cardSessionEventExpire, "noop", pgtype.UUID{}, pgtype.UUID{}, pgtype.UUID{}, 0, "")
	} else {
		for _, row := range rows {
			s.observeCardSession(ctx, cardSessionEventExpire, "expired", row)
		}
	}
	result := "expired"
	if len(rows) == 0 {
		result = "noop"
	}
	s.observeCardSessionDuration(cardSessionEventExpire, result, time.Since(started))
	return int64(len(rows)), nil
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
