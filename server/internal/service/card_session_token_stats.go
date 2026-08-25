package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/cardsession"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const cardSessionTokenStatsCandidateLimit int32 = 100

type cardSessionTokenStatsPublication struct {
	comment       db.Comment
	issue         db.Issue
	issueRevision int64
}

// PublishDueCardSessionTokenStats writes one cumulative, system-authored
// snapshot for each due open generation. The publication watermark is
// advanced in the same transaction as the comment, so a concurrent server
// sweeper cannot produce duplicate snapshots for the same generation/interval.
// A session with no usage yet advances its watermark without adding a noisy
// zero-token comment; the next interval will include the first real usage.
func (s *TaskService) PublishDueCardSessionTokenStats(ctx context.Context) (int64, error) {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return 0, nil
	}

	candidates, err := s.Queries.ListOpenCardSessionTokenStatsCandidateIDs(ctx, cardSessionTokenStatsCandidateLimit)
	if err != nil {
		// The new binary can overlap a rolling deployment before migration 421
		// has reached the database. Treat that as an unavailable optional sweep,
		// matching the existing card-session expiry compatibility behavior.
		if isMissingCardSessionTokenStatsSchema(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("list card session token stats candidates: %w", err)
	}

	var published int64
	var firstErr error
	for _, candidate := range candidates {
		publication, err := s.publishCardSessionTokenStats(ctx, candidate.ID, candidate.IssueID, candidate.WorkspaceID)
		if err != nil {
			if isMissingCardSessionTokenStatsSchema(err) {
				return published, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if publication == nil {
			continue
		}
		published++
		if s.Bus == nil {
			continue
		}
		s.Bus.Publish(events.Event{
			Type:        protocol.EventCommentCreated,
			WorkspaceID: util.UUIDToString(publication.issue.WorkspaceID),
			ActorType:   "system",
			ActorID:     "",
			Payload: map[string]any{
				"comment": func() map[string]any {
					fields := commentEventFields(publication.comment)
					fields["revision"] = publication.comment.Revision
					return fields
				}(),
				"issue_title":    publication.issue.Title,
				"issue_status":   publication.issue.Status,
				"issue_revision": publication.issueRevision,
			},
		})
	}
	return published, firstErr
}

func (s *TaskService) publishCardSessionTokenStats(ctx context.Context, sessionID, issueID, workspaceID pgtype.UUID) (*cardSessionTokenStatsPublication, error) {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin card session token stats: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)

	// Keep the lock order aligned with issue status changes: issue, workspace,
	// then card_session. CreateComment touches the already-locked issue and the
	// card-session status trigger takes the workspace lock.
	if _, err := qtx.LockIssueForCardSessionTokenStats(ctx, db.LockIssueForCardSessionTokenStatsParams{
		IssueID:     issueID,
		WorkspaceID: workspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lock card session issue: %w", err)
	}
	if _, err := qtx.LockWorkspaceForCardSession(ctx, workspaceID); err != nil {
		return nil, fmt.Errorf("lock card session workspace: %w", err)
	}
	locked, err := qtx.LockWorkspaceAndGetCardSessionTokenStats(ctx, sessionID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lock open card session: %w", err)
	}

	settings, err := cardsession.Parse(locked.WorkspaceSettings)
	if err != nil {
		return nil, fmt.Errorf("parse card session token stats settings: %w", err)
	}
	now := time.Now().UTC()
	interval := time.Duration(settings.TokenStatsIntervalMinutes) * time.Minute
	if locked.LastTokenStatsAt.Valid && now.Sub(locked.LastTokenStatsAt.Time) < interval {
		return nil, nil
	}
	if !locked.LastTokenStatsAt.Valid && locked.OpenedAt.Valid && now.Sub(locked.OpenedAt.Time) < interval {
		return nil, nil
	}

	issue, err := qtx.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID:          locked.IssueID,
		WorkspaceID: locked.WorkspaceID,
	})
	if err != nil {
		return nil, fmt.Errorf("load card session token stats issue: %w", err)
	}
	if issuestatus.IsTerminal(issuestatus.Effective(ctx, qtx, issue.WorkspaceID, issue.Status)) {
		// The status trigger normally moves the session out of open before this
		// transaction can observe it. Do not let a drifted row produce a comment
		// that would reopen the issue through CreateComment.
		return nil, nil
	}

	usage, err := qtx.GetOpenCardSessionTokenUsage(ctx, db.GetOpenCardSessionTokenUsageParams{
		IssueID:     locked.IssueID,
		AgentID:     locked.AgentID,
		WorkspaceID: locked.WorkspaceID,
		Since:       locked.OpenedAt,
	})
	if err != nil {
		return nil, fmt.Errorf("load card session token usage: %w", err)
	}

	if err := qtx.UpdateCardSessionTokenStatsAt(ctx, db.UpdateCardSessionTokenStatsAtParams{
		ID:          locked.ID,
		PublishedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}); err != nil {
		return nil, fmt.Errorf("advance card session token stats watermark: %w", err)
	}
	if usage.TaskCount == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty card session token stats: %w", err)
		}
		return nil, nil
	}

	created, err := qtx.CreateComment(ctx, db.CreateCommentParams{
		ID:           dbid.NewV7(),
		IssueID:      locked.IssueID,
		WorkspaceID:  locked.WorkspaceID,
		AuthorType:   "system",
		AuthorID:     pgtype.UUID{Valid: true},
		Content:      formatCardSessionTokenStats(locked.Generation, now, usage),
		Type:         "system",
		ParentID:     pgtype.UUID{Valid: false},
		SourceTaskID: pgtype.UUID{Valid: false},
	})
	if err != nil {
		return nil, fmt.Errorf("create card session token stats comment: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit card session token stats: %w", err)
	}

	return &cardSessionTokenStatsPublication{
		comment:       created.Comment(),
		issue:         issue,
		issueRevision: created.IssueRevision,
	}, nil
}

func formatCardSessionTokenStats(generation int64, at time.Time, usage db.GetOpenCardSessionTokenUsageRow) string {
	return fmt.Sprintf(
		"Intermediate token usage update for open card generation %d (cumulative since the session opened, as of %s): input %d, output %d, cache read %d, cache write %d, tasks with usage %d.",
		generation,
		at.UTC().Format(time.RFC3339),
		usage.TotalInputTokens,
		usage.TotalOutputTokens,
		usage.TotalCacheReadTokens,
		usage.TotalCacheWriteTokens,
		usage.TaskCount,
	)
}

func isMissingCardSessionTokenStatsSchema(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42P01" || pgErr.Code == "42703"
}
