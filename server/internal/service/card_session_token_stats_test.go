package service

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestRefreshCardSessionTokenStatsAggregatesCurrentUsage(t *testing.T) {
	pool := sharedTestPool(t)
	ctx := context.Background()
	var hasAggregateColumns bool
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) = 5
		FROM information_schema.columns
		WHERE table_name = 'card_session'
		  AND column_name IN (
			  'token_input_tokens', 'token_output_tokens', 'token_cache_read_tokens',
			  'token_cache_write_tokens', 'token_task_count'
		  )`).Scan(&hasAggregateColumns); err != nil {
		t.Fatalf("check card session aggregate columns: %v", err)
	}
	if !hasAggregateColumns {
		t.Skip("card session token aggregate migration is not applied")
	}

	workspaceID, _, agentID, issueID := seedAttributionFixture(t, pool)
	if _, err := pool.Exec(ctx, `UPDATE issue SET status = 'todo' WHERE id = $1`, issueID); err != nil {
		t.Fatalf("move fixture issue to todo: %v", err)
	}
	q := db.New(pool)
	svc := &TaskService{Queries: q, TxStarter: pool, Bus: events.New()}
	issue, err := q.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load fixture issue: %v", err)
	}
	task, err := svc.EnqueueTaskForIssue(ctx, issue)
	if err != nil {
		t.Fatalf("enqueue fixture task: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE agent_task_queue SET status = 'dispatched', dispatched_at = now() WHERE id = $1`, task.ID); err != nil {
		t.Fatalf("dispatch fixture task: %v", err)
	}
	started, err := svc.StartTaskWithCardSessionLease(ctx, task.ID, "test-daemon")
	if err != nil {
		t.Fatalf("start fixture task and bind its card-session generation: %v", err)
	}
	task = *started
	if !task.CardSessionID.Valid {
		t.Fatal("started issue task has no card_session_id binding")
	}

	usage := db.UpsertTaskUsageParams{
		TaskID:           task.ID,
		Provider:         "test-provider",
		Model:            "test-model",
		InputTokens:      120,
		OutputTokens:     30,
		CacheReadTokens:  40,
		CacheWriteTokens: 5,
	}
	if err := q.UpsertTaskUsage(ctx, usage); err != nil {
		t.Fatalf("insert task usage: %v", err)
	}
	if err := svc.RefreshCardSessionTokenStatsForTask(ctx, task.ID); err != nil {
		t.Fatalf("refresh card session token stats: %v", err)
	}

	assertSessionTokenStats := func(wantInput, wantOutput, wantCacheRead, wantCacheWrite int64) {
		t.Helper()
		var input, output, cacheRead, cacheWrite, taskCount int64
		if err := pool.QueryRow(ctx, `
			SELECT token_input_tokens, token_output_tokens, token_cache_read_tokens,
			       token_cache_write_tokens, token_task_count
			FROM card_session
			WHERE issue_id = $1 AND agent_id = $2 AND workspace_id = $3 AND state <> 'closed'
			ORDER BY generation DESC LIMIT 1`,
			util.MustParseUUID(issueID), util.MustParseUUID(agentID), util.MustParseUUID(workspaceID),
		).Scan(&input, &output, &cacheRead, &cacheWrite, &taskCount); err != nil {
			t.Fatalf("read card session token stats: %v", err)
		}
		if input != wantInput || output != wantOutput || cacheRead != wantCacheRead || cacheWrite != wantCacheWrite || taskCount != 1 {
			t.Fatalf("card session token stats = (%d, %d, %d, %d, %d), want (%d, %d, %d, %d, 1)",
				input, output, cacheRead, cacheWrite, taskCount,
				wantInput, wantOutput, wantCacheRead, wantCacheWrite)
		}
	}
	assertSessionTokenStats(120, 30, 40, 5)

	// Provider reports can correct an earlier row. Refresh replaces the session
	// snapshot from task_usage instead of adding the corrected values twice.
	usage.InputTokens = 160
	usage.OutputTokens = 35
	if err := q.UpsertTaskUsage(ctx, usage); err != nil {
		t.Fatalf("correct task usage: %v", err)
	}
	if err := svc.RefreshCardSessionTokenStatsForTask(ctx, task.ID); err != nil {
		t.Fatalf("refresh corrected card session token stats: %v", err)
	}
	assertSessionTokenStats(160, 35, 40, 5)

	// Simulate a process outage longer than idle_timeout_hours: the usage row
	// persisted, but the background aggregate did not run before the lifecycle
	// sweep closed its generation. Recovery must refresh the closed row without
	// reopening or otherwise changing its lifecycle state.
	if _, err := pool.Exec(ctx, `
		UPDATE agent_task_queue
		SET status = 'completed', completed_at = now()
		WHERE id = $1`, task.ID); err != nil {
		t.Fatalf("complete task before simulated downtime: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE card_session
		SET last_activity_at = now() - interval '25 hours',
		    token_input_tokens = 0, token_output_tokens = 0,
		    token_cache_read_tokens = 0, token_cache_write_tokens = 0,
		    token_task_count = 0, last_token_stats_at = NULL
		WHERE id = $1`, task.CardSessionID); err != nil {
		t.Fatalf("age session and clear stale aggregate: %v", err)
	}
	closed, err := svc.ExpireCardSessions(ctx)
	if err != nil {
		t.Fatalf("expire idle session after simulated downtime: %v", err)
	}
	if closed < 1 {
		t.Fatal("idle session was not closed before token recovery")
	}
	recovered, err := svc.RecoverCardSessionTokenStats(ctx)
	if err != nil {
		t.Fatalf("recover token stats for closed generation: %v", err)
	}
	if recovered < 1 {
		t.Fatal("recovery did not refresh the stale closed generation")
	}
	var state string
	var input, output, cacheRead, cacheWrite, taskCount int64
	if err := pool.QueryRow(ctx, `
		SELECT state, token_input_tokens, token_output_tokens, token_cache_read_tokens,
		       token_cache_write_tokens, token_task_count
		FROM card_session WHERE id = $1`, task.CardSessionID,
	).Scan(&state, &input, &output, &cacheRead, &cacheWrite, &taskCount); err != nil {
		t.Fatalf("read recovered closed session totals: %v", err)
	}
	if state != "closed" || input != 160 || output != 35 || cacheRead != 40 || cacheWrite != 5 || taskCount != 1 {
		t.Fatalf("closed generation after recovery = (%s, %d, %d, %d, %d, %d), want closed with (160, 35, 40, 5, 1)",
			state, input, output, cacheRead, cacheWrite, taskCount)
	}
}

func TestRefreshCardSessionTokenStatsForTaskWithoutDatabaseIsNoop(t *testing.T) {
	var taskID pgtype.UUID
	if err := (&TaskService{}).RefreshCardSessionTokenStatsForTask(context.Background(), taskID); err != nil {
		t.Fatalf("refresh without database dependencies: %v", err)
	}
}
