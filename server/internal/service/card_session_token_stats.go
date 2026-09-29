package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const cardSessionTokenStatsRecoveryLimit int32 = 100

// RefreshCardSessionTokenStatsAsync schedules a session aggregate update after
// provider usage has been persisted. The recovery sweep repairs work lost to a
// process restart between the usage write and this background refresh.
func (s *TaskService) RefreshCardSessionTokenStatsAsync(taskID pgtype.UUID) {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := s.RefreshCardSessionTokenStatsForTask(ctx, taskID); err != nil {
			slog.Warn("card session token stats refresh failed",
				"task_id", util.UUIDToString(taskID),
				"error", err,
			)
		}
	}()
}

// RefreshCardSessionTokenStatsForTask recomputes the owning session's totals
// from task_usage, so provider corrections replace prior values instead of
// being double-counted.
func (s *TaskService) RefreshCardSessionTokenStatsForTask(ctx context.Context, taskID pgtype.UUID) error {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return nil
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin card session token stats refresh: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	cardSessionID, err := qtx.LockCardSessionForTaskTokenStats(ctx, taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lock card session for token stats refresh: %w", err)
	}
	if err := refreshCardSessionTokenStats(ctx, qtx, cardSessionID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit card session token stats refresh: %w", err)
	}
	return nil
}

// RecoverCardSessionTokenStats refreshes sessions whose task usage changed
// after the last successful aggregate update.
func (s *TaskService) RecoverCardSessionTokenStats(ctx context.Context) (int64, error) {
	if s == nil || s.Queries == nil || s.TxStarter == nil {
		return 0, nil
	}
	ids, err := s.Queries.ListCardSessionsWithStaleTokenStats(ctx, cardSessionTokenStatsRecoveryLimit)
	if err != nil {
		return 0, fmt.Errorf("list card sessions with stale token stats: %w", err)
	}
	var refreshed int64
	var firstErr error
	for _, id := range ids {
		if err := s.refreshCardSessionTokenStatsByID(ctx, id); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		refreshed++
	}
	return refreshed, firstErr
}

func (s *TaskService) refreshCardSessionTokenStatsByID(ctx context.Context, cardSessionID pgtype.UUID) error {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin recovered card session token stats refresh: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)
	if _, err := qtx.LockCardSessionTokenStats(ctx, cardSessionID); errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("lock recovered card session token stats: %w", err)
	}
	if err := refreshCardSessionTokenStats(ctx, qtx, cardSessionID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit recovered card session token stats: %w", err)
	}
	return nil
}

func refreshCardSessionTokenStats(ctx context.Context, q *db.Queries, cardSessionID pgtype.UUID) error {
	stats, err := q.GetCardSessionTokenStats(ctx, cardSessionID)
	if err != nil {
		return fmt.Errorf("aggregate card session token stats: %w", err)
	}
	if err := q.UpdateCardSessionTokenStats(ctx, db.UpdateCardSessionTokenStatsParams{
		CardSessionID:    cardSessionID,
		InputTokens:      stats.InputTokens,
		OutputTokens:     stats.OutputTokens,
		CacheReadTokens:  stats.CacheReadTokens,
		CacheWriteTokens: stats.CacheWriteTokens,
		TaskCount:        stats.TaskCount,
		LatestUsageAt:    stats.LatestUsageAt,
	}); err != nil {
		return fmt.Errorf("update card session token stats: %w", err)
	}
	return nil
}
