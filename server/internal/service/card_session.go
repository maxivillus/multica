package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/cardsession"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var (
	// ErrCardSessionCapacity is returned before a task is inserted when a new
	// generation would exceed the workspace's configured open-session cap.
	ErrCardSessionCapacity = errors.New("card session capacity exhausted")
	// ErrCardSessionCloseForbidden makes the lifecycle rule explicit for future
	// close endpoints: an open generation can only enter retention through the
	// issue's done transition, never through an arbitrary close request.
	ErrCardSessionCloseForbidden = errors.New("card session can only close after issue reaches done")
)

// EnsureCardSession creates or reopens the server-owned generation for an
// issue/agent pair. Workspace row locking makes the capacity check and
// generation allocation atomic across concurrent comment/status triggers.
func (s *TaskService) EnsureCardSession(ctx context.Context, issueID, workspaceID, agentID pgtype.UUID, provider string) (db.CardSession, error) {
	if s.Queries == nil || s.TxStarter == nil {
		return db.CardSession{}, errors.New("card session service is not configured")
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return db.CardSession{}, fmt.Errorf("begin card session: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)

	workspace, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID)
	if err != nil {
		return db.CardSession{}, fmt.Errorf("lock workspace for card session: %w", err)
	}
	settings, err := cardsession.Parse(workspace.Settings)
	if err != nil {
		return db.CardSession{}, err
	}

	// Expire only this workspace's retained rows while its lock is held. This
	// keeps stale rows from consuming capacity without allowing one enqueue to
	// mutate another workspace.
	if _, err := qtx.ExpireCardSessionsForWorkspace(ctx, workspaceID); err != nil {
		return db.CardSession{}, fmt.Errorf("expire card sessions: %w", err)
	}

	current, err := qtx.GetResumableCardSession(ctx, db.GetResumableCardSessionParams{
		IssueID:     issueID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
	})
	if err == nil {
		if current.State == "done_retained" {
			current, err = qtx.ReopenCardSession(ctx, current.ID)
			if err != nil {
				return db.CardSession{}, fmt.Errorf("reopen card session: %w", err)
			}
		} else {
			current, err = qtx.TouchCardSession(ctx, current.ID)
			if err != nil {
				return db.CardSession{}, fmt.Errorf("touch card session: %w", err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return db.CardSession{}, fmt.Errorf("commit card session reuse: %w", err)
		}
		return current, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return db.CardSession{}, fmt.Errorf("load card session: %w", err)
	}

	openCount, err := qtx.CountOpenCardSessions(ctx, workspaceID)
	if err != nil {
		return db.CardSession{}, fmt.Errorf("count open card sessions: %w", err)
	}
	if !settings.CapacityAvailable(openCount) {
		return db.CardSession{}, fmt.Errorf("%w: workspace %s has %d open sessions (limit %d)",
			ErrCardSessionCapacity,
			util.UUIDToString(workspaceID),
			openCount,
			settings.MaxOpenSessions,
		)
	}

	generation := int64(1)
	latest, latestErr := qtx.GetLatestCardSession(ctx, db.GetLatestCardSessionParams{
		IssueID:     issueID,
		AgentID:     agentID,
		WorkspaceID: workspaceID,
	})
	if latestErr == nil {
		generation = latest.Generation + 1
	} else if !errors.Is(latestErr, pgx.ErrNoRows) {
		return db.CardSession{}, fmt.Errorf("load latest card session: %w", latestErr)
	}

	created, err := qtx.CreateCardSession(ctx, db.CreateCardSessionParams{
		WorkspaceID: workspaceID,
		IssueID:     issueID,
		AgentID:     agentID,
		Generation:  generation,
		Provider:    provider,
	})
	if err != nil {
		return db.CardSession{}, fmt.Errorf("create card session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return db.CardSession{}, fmt.Errorf("commit card session create: %w", err)
	}
	return created, nil
}

// MarkIssueCardSessionsDone moves every open generation for an issue into the
// configured post-done retention state. It is intentionally callable only by
// status-write code; there is no generic close operation for open sessions.
func (s *TaskService) MarkIssueCardSessionsDone(ctx context.Context, issueID, workspaceID pgtype.UUID) error {
	if s.Queries == nil || s.TxStarter == nil {
		return errors.New("card session service is not configured")
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin card session close: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	workspace, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID)
	if err != nil {
		return fmt.Errorf("lock workspace for card session close: %w", err)
	}
	settings, err := cardsession.Parse(workspace.Settings)
	if err != nil {
		return err
	}
	if _, err := qtx.MarkCardSessionsDone(ctx, db.MarkCardSessionsDoneParams{
		IssueID:        issueID,
		WorkspaceID:    workspaceID,
		RetentionHours: int64(settings.PostDoneRetentionHours),
	}); err != nil {
		return fmt.Errorf("retain card sessions: %w", err)
	}
	return tx.Commit(ctx)
}

// ReopenIssueCardSessions preserves the same generation when a done issue is
// reopened during its configured retention window.
func (s *TaskService) ReopenIssueCardSessions(ctx context.Context, issueID, workspaceID pgtype.UUID) error {
	if s.Queries == nil || s.TxStarter == nil {
		return errors.New("card session service is not configured")
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin card session reopen: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	if _, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID); err != nil {
		return fmt.Errorf("lock workspace for card session reopen: %w", err)
	}
	if _, err := qtx.ReopenCardSessionsForIssue(ctx, db.ReopenCardSessionsForIssueParams{
		IssueID:     issueID,
		WorkspaceID: workspaceID,
	}); err != nil {
		return fmt.Errorf("reopen card sessions: %w", err)
	}
	return tx.Commit(ctx)
}

// ExpireCardSessions closes retained generations whose post-done window has
// elapsed. The server sweeper calls this globally; a missing table is ignored
// so a rolling deployment can start the new binary before migration 420 has
// reached every database node.
func (s *TaskService) ExpireCardSessions(ctx context.Context) (int64, error) {
	if s.Queries == nil {
		return 0, errors.New("card session service is not configured")
	}
	rows, err := s.Queries.ExpireCardSessions(ctx)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return 0, nil
		}
		return 0, fmt.Errorf("expire card sessions: %w", err)
	}
	return int64(len(rows)), nil
}

// UpdateCardSessionProviderState mirrors the task pin into the durable
// generation record. A provider process may be replaced, but the generation
// keeps the latest provider session/workdir pointer for reconnect.
func (s *TaskService) UpdateCardSessionProviderState(ctx context.Context, taskID pgtype.UUID, providerSessionID, workDir string) error {
	if s.Queries == nil {
		return errors.New("card session service is not configured")
	}
	return s.Queries.UpdateCardSessionProviderStateByTask(ctx, db.UpdateCardSessionProviderStateByTaskParams{
		ID:                taskID,
		ProviderSessionID: providerSessionID,
		WorkDir:           workDir,
	})
}
