package handler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestIssueTasksWaitForCardSessionCapacity(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}

	for _, enqueuePath := range []string{"issue", "mention"} {
		t.Run(enqueuePath, func(t *testing.T) {
			ctx := context.Background()
			_, runtimeID, agentID, fillerIssueID, targetIssueID := createCardSessionCapacityFixture(t, ctx)
			agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
			if err != nil {
				t.Fatalf("load test agent: %v", err)
			}
			filler, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(fillerIssueID))
			if err != nil {
				t.Fatalf("load filler issue: %v", err)
			}
			if _, err := testHandler.TaskService.EnsureCardSession(ctx, filler.ID, filler.WorkspaceID, agent.ID, agent.RuntimeMode); err != nil {
				t.Fatalf("open slot-filling card session: %v", err)
			}
			target, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(targetIssueID))
			if err != nil {
				t.Fatalf("load target issue: %v", err)
			}

			var task db.AgentTaskQueue
			switch enqueuePath {
			case "issue":
				task, err = testHandler.TaskService.EnqueueTaskForIssueByActor(ctx, target, util.MustParseUUID(testUserID))
			case "mention":
				task, err = testHandler.TaskService.EnqueueTaskForMention(ctx, target, agent.ID, pgtype.UUID{}, service.OriginNamed)
			}
			if err != nil {
				t.Fatalf("enqueue %s task at capacity: %v", enqueuePath, err)
			}
			if task.Status != "deferred" {
				t.Fatalf("task status at capacity = %q, want deferred", task.Status)
			}
			var waiting bool
			if err := testPool.QueryRow(ctx, `SELECT context->>'card_session_capacity_pending' = 'true' FROM agent_task_queue WHERE id = $1`, task.ID).Scan(&waiting); err != nil {
				t.Fatalf("read capacity wait marker: %v", err)
			}
			if !waiting {
				t.Fatal("task was not marked as waiting for a card-session slot")
			}

			if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET fire_at = now() - interval '1 second' WHERE id = $1`, task.ID); err != nil {
				t.Fatalf("make capacity retry due: %v", err)
			}
			claimed, err := testHandler.TaskService.ClaimTaskForRuntime(ctx, util.MustParseUUID(runtimeID))
			if err != nil {
				t.Fatalf("claim while capacity is full: %v", err)
			}
			if claimed != nil {
				t.Fatalf("task claimed while capacity is full: %s", util.UUIDToString(claimed.ID))
			}
			var stillWaiting bool
			if err := testPool.QueryRow(ctx, `SELECT status = 'deferred' AND context->>'card_session_capacity_pending' = 'true' AND fire_at > now() FROM agent_task_queue WHERE id = $1`, task.ID).Scan(&stillWaiting); err != nil {
				t.Fatalf("read scheduled capacity retry: %v", err)
			}
			if !stillWaiting {
				t.Fatal("full workspace did not keep the task deferred with a future retry")
			}

			if _, err := testPool.Exec(ctx, `UPDATE card_session SET last_activity_at = now() - interval '25 hours' WHERE issue_id = $1`, filler.ID); err != nil {
				t.Fatalf("age idle card session: %v", err)
			}
			if _, err := testPool.Exec(ctx, `UPDATE card_session SET state = 'closed', closed_at = now(), updated_at = now() WHERE issue_id = $1`, filler.ID); err != nil {
				t.Fatalf("free card-session slot: %v", err)
			}
			if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET fire_at = now() - interval '1 second' WHERE id = $1`, task.ID); err != nil {
				t.Fatalf("make capacity retry due after freeing slot: %v", err)
			}
			claimed, err = testHandler.TaskService.ClaimTaskForRuntime(ctx, util.MustParseUUID(runtimeID))
			if err != nil {
				t.Fatalf("claim after freeing capacity: %v", err)
			}
			if claimed == nil || claimed.ID != task.ID {
				t.Fatalf("claim after freeing capacity = %v, want task %s", claimed, util.UUIDToString(task.ID))
			}
			started, err := testHandler.TaskService.StartTaskForClaim(ctx, db.LockAgentTaskStartClaimParams{
				ID:           claimed.ID,
				RuntimeID:    claimed.RuntimeID,
				DispatchedAt: claimed.DispatchedAt,
			})
			if err != nil {
				t.Fatalf("start admitted task: %v", err)
			}
			if started == nil || started.Status != "running" {
				t.Fatalf("started task status = %v, want running", started)
			}
		})
	}
}

func TestParkedTaskWaitsForCapacityAfterExpiredGeneration(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	for _, tc := range []struct {
		name   string
		status string
		media  bool
	}{
		{name: "queued", status: "queued"},
		{name: "deferred", status: "deferred"},
		{name: "deferred_with_media_gate", status: "deferred", media: true},
	} {
		t.Run(tc.name, func(t *testing.T) { assertParkedTaskWaitsForCapacity(t, tc.status, tc.media) })
	}
}

func TestBatchCapacityWaiterOverflowCannotBePromotedWithoutSlot(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	workspaceID, runtimeID, agentID, fillerIssueID, _ := createCardSessionCapacityFixture(t, ctx)
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load capacity agent: %v", err)
	}
	filler, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(fillerIssueID))
	if err != nil {
		t.Fatalf("load capacity filler issue: %v", err)
	}
	if _, err := testHandler.TaskService.EnsureCardSession(ctx, filler.ID, filler.WorkspaceID, agent.ID, agent.RuntimeMode); err != nil {
		t.Fatalf("fill workspace card-session capacity: %v", err)
	}

	// WakeCardSessionCapacityWaiters handles a bounded batch of 20. Keep more
	// due rows in the queue so the generic single and batch promoters both see
	// overflow that has not passed EnsureCardSession's capacity check.
	const waiterCount = 101
	suffix := time.Now().UnixNano() % 1_000_000_000
	for i := 0; i < waiterCount; i++ {
		var issueID string
		if err := testPool.QueryRow(ctx, `
			INSERT INTO issue (workspace_id, title, status, priority, creator_id, creator_type, number, position, assignee_type, assignee_id)
			VALUES ($1, $2, 'todo', 'medium', $3, 'member', $4, 0, 'agent', $5)
			RETURNING id`, workspaceID, fmt.Sprintf("card-session overflow %d", i), testUserID, suffix+int64(i)+2, agentID).Scan(&issueID); err != nil {
			t.Fatalf("create overflow issue %d: %v", i, err)
		}
		if _, err := testPool.Exec(ctx, `
			INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority, issue_id, fire_at, context)
			VALUES ($1, $2, 'deferred', 0, $3, now() - interval '1 second', jsonb_build_object('card_session_capacity_pending', true))`,
			agentID, runtimeID, issueID); err != nil {
			t.Fatalf("create capacity waiter %d: %v", i, err)
		}
	}

	runtimeUUID := util.MustParseUUID(runtimeID)
	batchClaimed, err := testHandler.TaskService.ClaimTasksForRuntimes(ctx, []pgtype.UUID{runtimeUUID}, waiterCount)
	if err != nil {
		t.Fatalf("batch claim with overflow waiters: %v", err)
	}
	if len(batchClaimed) != 0 {
		t.Fatalf("batch claim returned %d tasks while every card-session slot is full", len(batchClaimed))
	}
	assertNoCapacityWaiterPromoted(t, ctx, runtimeID, waiterCount)

	claimed, err := testHandler.TaskService.ClaimTaskForRuntime(ctx, runtimeUUID)
	if err != nil {
		t.Fatalf("single claim with remaining capacity waiters: %v", err)
	}
	if claimed != nil {
		t.Fatalf("single claim returned task %s while card-session capacity is full", util.UUIDToString(claimed.ID))
	}
	assertNoCapacityWaiterPromoted(t, ctx, runtimeID, waiterCount)
}

func assertNoCapacityWaiterPromoted(t *testing.T, ctx context.Context, runtimeID string, wantCount int) {
	t.Helper()
	var total, invalid int
	if err := testPool.QueryRow(ctx, `
		SELECT COUNT(*)::int,
		       COUNT(*) FILTER (WHERE status <> 'deferred' OR context->>'card_session_capacity_pending' <> 'true')::int
		FROM agent_task_queue WHERE runtime_id = $1 AND issue_id IN (
			SELECT id FROM issue WHERE title LIKE 'card-session overflow %'
		)`, runtimeID).Scan(&total, &invalid); err != nil {
		t.Fatalf("read overflow waiter states: %v", err)
	}
	if total != wantCount || invalid != 0 {
		t.Fatalf("overflow waiters: total %d, promoted/unmarked %d; want total %d and none promoted", total, invalid, wantCount)
	}
}

func assertParkedTaskWaitsForCapacity(t *testing.T, taskStatus string, mediaPending bool) {
	t.Helper()
	ctx := context.Background()
	_, runtimeID, agentID, fillerIssueID, targetIssueID := createCardSessionCapacityFixture(t, ctx)
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load capacity agent: %v", err)
	}
	target, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(targetIssueID))
	if err != nil {
		t.Fatalf("load parked target issue: %v", err)
	}
	generation, err := testHandler.TaskService.EnsureCardSession(ctx, target.ID, target.WorkspaceID, agent.ID, agent.RuntimeMode)
	if err != nil {
		t.Fatalf("open target generation: %v", err)
	}
	taskID := insertIssueTaskWithStatus(t, agentID, targetIssueID, taskStatus)
	var originalFireAt time.Time
	if taskStatus == "deferred" {
		if mediaPending {
			if _, err := testPool.Exec(ctx, `
				UPDATE agent_task_queue
				SET context = jsonb_set(COALESCE(context, '{}'::jsonb), '{channel_issue_media_pending}', 'true'::jsonb, true),
				    fire_at = now() + interval '2 hours'
				WHERE id = $1`, taskID); err != nil {
				t.Fatalf("mark task as waiting for media: %v", err)
			}
		}
		if err := testPool.QueryRow(ctx, `SELECT fire_at FROM agent_task_queue WHERE id = $1`, taskID).Scan(&originalFireAt); err != nil {
			t.Fatalf("read deferred task fire_at: %v", err)
		}
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'backlog' WHERE id = $1`, targetIssueID); err != nil {
		t.Fatalf("park target issue: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE card_session SET last_activity_at = now() - interval '25 hours' WHERE id = $1`, generation.ID); err != nil {
		t.Fatalf("age parked generation: %v", err)
	}
	if _, err := testHandler.TaskService.ExpireCardSessions(ctx); err != nil {
		t.Fatalf("expire parked generation: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		UPDATE issue SET status = 'in_progress', assignee_type = 'agent', assignee_id = $2
		WHERE id = $1`, fillerIssueID, agentID); err != nil {
		t.Fatalf("assign filler issue: %v", err)
	}
	filler, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(fillerIssueID))
	if err != nil {
		t.Fatalf("load filler issue: %v", err)
	}
	if _, err := testHandler.TaskService.EnsureCardSession(ctx, filler.ID, filler.WorkspaceID, agent.ID, agent.RuntimeMode); err != nil {
		t.Fatalf("fill the open-session capacity slot: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'todo' WHERE id = $1`, targetIssueID); err != nil {
		t.Fatalf("reactivate target issue at capacity: %v", err)
	}

	var state string
	var generationCount int
	if err := testPool.QueryRow(ctx, `
		SELECT state, COUNT(*) OVER () FROM card_session
		WHERE issue_id = $1 AND agent_id = $2 ORDER BY generation DESC LIMIT 1
	`, targetIssueID, agentID).Scan(&state, &generationCount); err != nil {
		t.Fatalf("read target generation after reactivation: %v", err)
	}
	if state != "closed" || generationCount != 1 {
		t.Fatalf("target session after reactivation = state %q with %d generations, want closed generation only until capacity returns", state, generationCount)
	}
	var taskState, taskContext string
	var deferred, waiting bool
	if err := testPool.QueryRow(ctx, `
		SELECT status, COALESCE(context::text, ''), status = 'deferred',
		       COALESCE(context->>'card_session_capacity_pending' = 'true', false)
		FROM agent_task_queue WHERE id = $1
	`, taskID).Scan(&taskState, &taskContext, &deferred, &waiting); err != nil {
		t.Fatalf("read parked task admission state: %v", err)
	}
	if !deferred || !waiting {
		t.Fatalf("parked task = status %q context %s deferred %t capacity_wait %t, want deferred with capacity marker", taskState, taskContext, deferred, waiting)
	}
	if taskStatus == "deferred" {
		var fireAt time.Time
		var mediaStillPending bool
		if err := testPool.QueryRow(ctx, `
			SELECT fire_at, COALESCE(context->>'channel_issue_media_pending' = 'true', false)
			FROM agent_task_queue WHERE id = $1
		`, taskID).Scan(&fireAt, &mediaStillPending); err != nil {
			t.Fatalf("read deferred task retry state: %v", err)
		}
		if !fireAt.Equal(originalFireAt) {
			t.Fatalf("deferred task fire_at = %s, want preserved %s", fireAt, originalFireAt)
		}
		if mediaStillPending != mediaPending {
			t.Fatalf("channel media marker = %t, want %t", mediaStillPending, mediaPending)
		}
	}
	claimed, err := testHandler.TaskService.ClaimTaskForRuntime(ctx, util.MustParseUUID(runtimeID))
	if err != nil {
		t.Fatalf("claim while target capacity is full: %v", err)
	}
	if claimed != nil {
		t.Fatalf("task claimed without a card-session slot: %s", util.UUIDToString(claimed.ID))
	}
	var stillDeferred bool
	if err := testPool.QueryRow(ctx, `SELECT status = 'deferred' AND context->>'card_session_capacity_pending' = 'true' FROM agent_task_queue WHERE id = $1`, taskID).Scan(&stillDeferred); err != nil {
		t.Fatalf("re-read task after claim poll: %v", err)
	}
	if !stillDeferred {
		t.Fatal("capacity-blocked task left the deferred wait state")
	}
}

func createCardSessionCapacityFixture(t *testing.T, ctx context.Context) (workspaceID, runtimeID, agentID, fillerIssueID, targetIssueID string) {
	t.Helper()
	suffix := time.Now().UnixNano()
	slug := fmt.Sprintf("card-session-capacity-%d", suffix)
	if err := testPool.QueryRow(ctx, `
		INSERT INTO workspace (name, slug, description, issue_prefix, settings)
		VALUES ($1, $2, 'card-session capacity fixture', 'CSW', jsonb_build_object('card_sessions', jsonb_build_object('idle_timeout_hours', 24, 'max_open_sessions', 1)))
		RETURNING id`, "Card Session Capacity", slug).Scan(&workspaceID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := testPool.Exec(ctx, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`, workspaceID, testUserID); err != nil {
		t.Fatalf("create workspace member: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at, visibility, owner_id)
		VALUES ($1, $2, 'Card Session Capacity Runtime', 'cloud', 'codex', 'online', 'test', '{}'::jsonb, now(), 'public', $3)
		RETURNING id`, workspaceID, "card-session-capacity-"+slug, testUserID).Scan(&runtimeID); err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (workspace_id, name, description, runtime_mode, runtime_config, runtime_id, visibility, permission_mode, max_concurrent_tasks, owner_id)
		VALUES ($1, 'Card Session Capacity Agent', '', 'cloud', '{}'::jsonb, $2, 'workspace', 'public_to', 1, $3)
		RETURNING id`, workspaceID, runtimeID, testUserID).Scan(&agentID); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	insertIssue := func(title string, number int64) string {
		var issueID string
		if err := testPool.QueryRow(ctx, `
			INSERT INTO issue (workspace_id, title, status, priority, creator_id, creator_type, number, position, assignee_type, assignee_id)
			VALUES ($1, $2, 'todo', 'medium', $3, 'member', $4, 0, 'agent', $5)
			RETURNING id`, workspaceID, title, testUserID, number, agentID).Scan(&issueID); err != nil {
			t.Fatalf("create issue %q: %v", title, err)
		}
		return issueID
	}
	fillerIssueID = insertIssue("card-session capacity filler", suffix%1_000_000_000)
	targetIssueID = insertIssue("card-session capacity target", (suffix%1_000_000_000)+1)
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		testPool.Exec(cleanupCtx, `DELETE FROM issue_task_cancel_outbox WHERE issue_id IN (SELECT id FROM issue WHERE workspace_id = $1)`, workspaceID)
		testPool.Exec(cleanupCtx, `DELETE FROM agent_task_queue WHERE runtime_id = $1`, runtimeID)
		testPool.Exec(cleanupCtx, `DELETE FROM card_session WHERE workspace_id = $1`, workspaceID)
		testPool.Exec(cleanupCtx, `DELETE FROM issue WHERE workspace_id = $1`, workspaceID)
		testPool.Exec(cleanupCtx, `DELETE FROM agent_invocation_target WHERE agent_id = $1`, agentID)
		testPool.Exec(cleanupCtx, `DELETE FROM agent WHERE id = $1`, agentID)
		testPool.Exec(cleanupCtx, `DELETE FROM agent_runtime WHERE id = $1`, runtimeID)
		testPool.Exec(cleanupCtx, `DELETE FROM member WHERE workspace_id = $1 AND user_id = $2`, workspaceID, testUserID)
		testPool.Exec(cleanupCtx, `DELETE FROM workspace WHERE id = $1`, workspaceID)
	})
	return workspaceID, runtimeID, agentID, fillerIssueID, targetIssueID
}
