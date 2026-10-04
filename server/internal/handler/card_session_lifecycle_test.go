package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestCardSessionDoneStaysOpenAndCancelledPausesUntilComment(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var hasCardSessions bool
	if err := testPool.QueryRow(ctx, `SELECT to_regclass('card_session') IS NOT NULL`).Scan(&hasCardSessions); err != nil {
		t.Fatalf("check card_session table: %v", err)
	}
	if !hasCardSessions {
		t.Skip("card_session migration not applied")
	}

	for _, status := range []string{"done", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			agentID := createHandlerTestAgent(t, "card-session-lifecycle-"+status, nil)
			issueID := dbfx.Issue(t, "card session lifecycle "+status, testutil.Cols{
				"status":        "todo",
				"assignee_type": "agent",
				"assignee_id":   agentID,
			})
			var sessionID string
			if err := testPool.QueryRow(ctx, `
				INSERT INTO card_session (
					workspace_id, issue_id, agent_id, generation, state, provider,
					provider_session_id, work_dir
				)
				VALUES ($1, $2, $3, 7, 'open', 'codex', 'provider-session-7', '/work/card-session-7')
				RETURNING id`, testWorkspaceID, issueID, agentID).Scan(&sessionID); err != nil {
				t.Fatalf("insert card session: %v", err)
			}

			if _, err := testPool.Exec(ctx, `UPDATE issue SET status = $2 WHERE id = $1`, issueID, status); err != nil {
				t.Fatalf("set issue %s: %v", status, err)
			}

			var retainedState string
			var retainedReason *string
			var retainedGeneration int64
			if err := testPool.QueryRow(ctx, `
				SELECT state, pause_reason, generation
				FROM card_session WHERE id = $1`, sessionID,
			).Scan(&retainedState, &retainedReason, &retainedGeneration); err != nil {
				t.Fatalf("read retained card session: %v", err)
			}
			wantRetainedState := "open"
			if status == "cancelled" {
				wantRetainedState = "paused"
			}
			if retainedState != wantRetainedState {
				t.Fatalf("state after %s = %q, want %q", status, retainedState, wantRetainedState)
			}
			if status == "cancelled" && (retainedReason == nil || *retainedReason != "cancelled") {
				t.Fatalf("pause_reason after cancelled = %v, want cancelled", retainedReason)
			}
			if retainedGeneration != 7 {
				t.Fatalf("generation after %s = %d, want 7", status, retainedGeneration)
			}

			created, err := testHandler.Queries.CreateComment(ctx, db.CreateCommentParams{
				IssueID:     util.MustParseUUID(issueID),
				WorkspaceID: util.MustParseUUID(testWorkspaceID),
				AuthorType:  "member",
				AuthorID:    util.MustParseUUID(testUserID),
				Content:     "follow up after terminal status",
				Type:        "comment",
			})
			if err != nil {
				t.Fatalf("create follow-up comment: %v", err)
			}
			if created.IssueStatus != "in_review" {
				t.Fatalf("issue status after %s comment = %q, want in_review", status, created.IssueStatus)
			}

			var reopenedState, providerSessionID, workDir string
			var reopenedGeneration int64
			if err := testPool.QueryRow(ctx, `
				SELECT state, generation, provider_session_id, work_dir
				FROM card_session WHERE id = $1`, sessionID,
			).Scan(&reopenedState, &reopenedGeneration, &providerSessionID, &workDir); err != nil {
				t.Fatalf("read reopened card session: %v", err)
			}
			if reopenedState != "open" {
				t.Fatalf("state after %s follow-up = %q, want open", status, reopenedState)
			}
			if reopenedGeneration != retainedGeneration {
				t.Fatalf("generation after %s follow-up = %d, want unchanged %d", status, reopenedGeneration, retainedGeneration)
			}
			if providerSessionID != "provider-session-7" || workDir != "/work/card-session-7" {
				t.Fatalf("provider resume data after %s follow-up = (%q, %q), want retained values", status, providerSessionID, workDir)
			}
		})
	}
}

func TestTerminalTaskPersistsCardSessionProviderState(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	ctx := context.Background()
	_, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}
	session, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
	if err != nil {
		t.Fatalf("open card session: %v", err)
	}

	var taskID pgtype.UUID
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (
			agent_id, runtime_id, issue_id, card_session_id, status, priority, started_at
		)
		VALUES ($1, $2, $3, $4, 'running', 0, now())
		RETURNING id`, agentID, runtimeID, issueID, session.ID).Scan(&taskID); err != nil {
		t.Fatalf("insert running task: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID)
	})

	if _, err := testHandler.TaskService.CompleteTask(
		ctx,
		taskID,
		[]byte(`{"output":"terminal turn"}`),
		"terminal-provider-session",
		"/tmp/terminal-card-session",
		"",
		false,
		"",
		"",
	); err != nil {
		t.Fatalf("complete task: %v", err)
	}

	var providerSessionID, workDir string
	if err := testPool.QueryRow(ctx, `
		SELECT provider_session_id, work_dir
		FROM card_session WHERE id = $1`, session.ID).Scan(&providerSessionID, &workDir); err != nil {
		t.Fatalf("read terminal card session pointer: %v", err)
	}
	if providerSessionID != "terminal-provider-session" || workDir != "/tmp/terminal-card-session" {
		t.Fatalf("terminal card session pointer = (%q, %q), want final provider state", providerSessionID, workDir)
	}
}

func TestCancelledIssueCancelsOnlyItsTasksAfterRequestDisconnect(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	ctx := context.Background()
	workspaceID, runtimeID, _, otherIssueID, issueID := createCardSessionCapacityFixture(t, ctx)
	previous, err := testHandler.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: util.MustParseUUID(issueID), WorkspaceID: util.MustParseUUID(workspaceID),
	})
	if err != nil {
		t.Fatalf("load issue before cancellation: %v", err)
	}
	var targetTaskID, otherTaskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
		VALUES ($1, $2, $3, 'running', 0) RETURNING id`, previous.AssigneeID, runtimeID, issueID,
	).Scan(&targetTaskID); err != nil {
		t.Fatalf("insert task for cancelled issue: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
		VALUES ($1, $2, $3, 'queued', 0) RETURNING id`, previous.AssigneeID, runtimeID, otherIssueID,
	).Scan(&otherTaskID); err != nil {
		t.Fatalf("insert task for other issue: %v", err)
	}

	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'cancelled' WHERE id = $1`, issueID); err != nil {
		t.Fatalf("cancel issue: %v", err)
	}
	var pendingBefore int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM issue_task_cancel_outbox WHERE issue_id = $1`, issueID).Scan(&pendingBefore); err != nil {
		t.Fatalf("count pending cancellations: %v", err)
	}
	if pendingBefore != 1 {
		t.Fatalf("pending cancellation rows after status change = %d, want 1", pendingBefore)
	}
	current, err := testHandler.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: util.MustParseUUID(issueID), WorkspaceID: util.MustParseUUID(workspaceID),
	})
	if err != nil {
		t.Fatalf("load issue after cancellation: %v", err)
	}

	requestCtx, disconnect := context.WithCancel(ctx)
	disconnect()
	testHandler.reconcileCardSessionStatus(requestCtx, previous, current)

	var targetStatus, otherStatus string
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, targetTaskID).Scan(&targetStatus); err != nil {
		t.Fatalf("read cancelled issue task: %v", err)
	}
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, otherTaskID).Scan(&otherStatus); err != nil {
		t.Fatalf("read other issue task: %v", err)
	}
	if targetStatus != "cancelled" {
		t.Fatalf("task on cancelled issue has status %q, want cancelled", targetStatus)
	}
	if otherStatus != "queued" {
		t.Fatalf("task on other issue has status %q, want queued", otherStatus)
	}
	var pendingAfter int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM issue_task_cancel_outbox WHERE issue_id = $1`, issueID).Scan(&pendingAfter); err != nil {
		t.Fatalf("count completed cancellations: %v", err)
	}
	if pendingAfter != 0 {
		t.Fatalf("pending cancellation rows after reconciliation = %d, want 0", pendingAfter)
	}
}

func TestIssueTaskCancellationOutboxRetriesAfterStatusCommit(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	ctx := context.Background()
	workspaceID, runtimeID, _, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issue, err := testHandler.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: util.MustParseUUID(issueID), WorkspaceID: util.MustParseUUID(workspaceID),
	})
	if err != nil {
		t.Fatalf("load issue before cancellation: %v", err)
	}
	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
		VALUES ($1, $2, $3, 'running', 0) RETURNING id`, issue.AssigneeID, runtimeID, issueID,
	).Scan(&taskID); err != nil {
		t.Fatalf("insert task for cancelled issue: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'cancelled' WHERE id = $1`, issueID); err != nil {
		t.Fatalf("cancel issue: %v", err)
	}

	var pendingBefore int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM issue_task_cancel_outbox WHERE issue_id = $1`, issueID).Scan(&pendingBefore); err != nil {
		t.Fatalf("count pending cancellations: %v", err)
	}
	if pendingBefore != 1 {
		t.Fatalf("pending cancellation rows after status commit = %d, want 1", pendingBefore)
	}

	result, err := testHandler.TaskService.RecoverPendingIssueTaskCancellations(ctx, util.MustParseUUID(issueID), 100)
	if err != nil {
		t.Fatalf("recover pending task cancellations: %v", err)
	}
	if result.Scanned != 1 || result.Completed != 1 || result.Cancelled != 1 {
		t.Fatalf("recovery result = %+v, want one scanned, completed, and cancelled", result)
	}
	var status string
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, taskID).Scan(&status); err != nil {
		t.Fatalf("read recovered task status: %v", err)
	}
	if status != "cancelled" {
		t.Fatalf("recovered task status = %q, want cancelled", status)
	}
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM issue_task_cancel_outbox WHERE issue_id = $1`, issueID).Scan(&pendingBefore); err != nil {
		t.Fatalf("count recovered cancellations: %v", err)
	}
	if pendingBefore != 0 {
		t.Fatalf("pending cancellation rows after recovery = %d, want 0", pendingBefore)
	}
}

func TestCancelledTaskLateProviderPinIsRetainedForFollowUp(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	workspaceID, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}
	generation, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
	if err != nil {
		t.Fatalf("open generation: %v", err)
	}
	var taskID pgtype.UUID
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
		VALUES ($1, $2, $3, 'dispatched', 0, now()) RETURNING id`, agentID, runtimeID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("insert dispatched task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })
	started, err := testHandler.TaskService.StartTaskWithCardSessionLease(ctx, taskID, "test-daemon")
	if err != nil {
		t.Fatalf("start task: %v", err)
	}
	if !started.CardSessionID.Valid || started.CardSessionID != generation.ID {
		t.Fatalf("task bound generation = %s (valid=%t), want %s", util.UUIDToString(started.CardSessionID), started.CardSessionID.Valid, util.UUIDToString(generation.ID))
	}
	lease, err := testHandler.Queries.GetCardSession(ctx, started.CardSessionID)
	if err != nil {
		t.Fatalf("load card-session lease: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'cancelled' WHERE id = $1`, issueID); err != nil {
		t.Fatalf("cancel issue: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status = 'cancelled', completed_at = now() WHERE id = $1`, taskID); err != nil {
		t.Fatalf("cancel task: %v", err)
	}
	if err := testHandler.TaskService.UpdateCardSessionProviderState(ctx, taskID, "late-cancelled-provider-marker", "/private/late-cancelled-workdir-marker", lease.LeaseOwner.String, lease.LeaseEpoch); err != nil {
		t.Fatalf("late provider pin: %v", err)
	}
	var state, pauseReason, providerSessionID, workDir string
	if err := testPool.QueryRow(ctx, `SELECT state, pause_reason, provider_session_id, work_dir FROM card_session WHERE id = $1`, generation.ID).Scan(&state, &pauseReason, &providerSessionID, &workDir); err != nil {
		t.Fatalf("read paused generation after pin: %v", err)
	}
	if state != "paused" || pauseReason != "cancelled" || providerSessionID != "late-cancelled-provider-marker" || workDir != "/private/late-cancelled-workdir-marker" {
		t.Fatalf("late pin state = (%q, %q, %q, %q), want cancelled generation with updated provider pointer", state, pauseReason, providerSessionID, workDir)
	}
	comment, err := testHandler.Queries.CreateComment(ctx, db.CreateCommentParams{
		IssueID: util.MustParseUUID(issueID), WorkspaceID: util.MustParseUUID(workspaceID),
		AuthorType: "member", AuthorID: util.MustParseUUID(testUserID), Content: "continue after cancellation", Type: "comment",
	})
	if err != nil {
		t.Fatalf("create follow-up comment: %v", err)
	}
	if comment.IssueStatus != "in_review" {
		t.Fatalf("comment issue status = %q, want in_review", comment.IssueStatus)
	}
	var reopenedID pgtype.UUID
	var reopenedGeneration int64
	var reopenedState, resumedProvider, resumedWorkDir string
	if err := testPool.QueryRow(ctx, `
		SELECT id, generation, state, provider_session_id, work_dir
		FROM card_session WHERE issue_id = $1 AND agent_id = $2 AND state = 'open'`, issueID, agentID,
	).Scan(&reopenedID, &reopenedGeneration, &reopenedState, &resumedProvider, &resumedWorkDir); err != nil {
		t.Fatalf("read resumed generation: %v", err)
	}
	if reopenedID != generation.ID || reopenedGeneration != generation.Generation || reopenedState != "open" || resumedProvider != "late-cancelled-provider-marker" || resumedWorkDir != "/private/late-cancelled-workdir-marker" {
		t.Fatalf("follow-up resumed (%s, %d, %s, %q, %q), want same generation with late provider pointer", util.UUIDToString(reopenedID), reopenedGeneration, reopenedState, resumedProvider, resumedWorkDir)
	}
}

func TestCardSessionAllocationSerializesWithIssueDelete(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	workspaceID, _, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}
	if _, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode); err != nil {
		t.Fatalf("seed card session: %v", err)
	}
	deleteTx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin delete transaction: %v", err)
	}
	defer deleteTx.Rollback(context.Background())
	deleteQueries := testHandler.Queries.WithTx(deleteTx)
	if _, err := deleteQueries.LockIssueForDelete(ctx, db.LockIssueForDeleteParams{ID: issue.ID, WorkspaceID: issue.WorkspaceID}); err != nil {
		t.Fatalf("lock issue for delete: %v", err)
	}
	if err := deleteQueries.DeleteIssue(ctx, db.DeleteIssueParams{ID: issue.ID, WorkspaceID: issue.WorkspaceID}); err != nil {
		t.Fatalf("delete issue in transaction: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("session allocation completed before uncommitted issue deletion: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := deleteTx.Commit(ctx); err != nil {
		t.Fatalf("commit issue deletion: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, service.ErrCardSessionInactive) {
			t.Fatalf("allocation after delete = %v, want ErrCardSessionInactive", err)
		}
	case <-ctx.Done():
		t.Fatalf("allocation did not unblock after issue delete: %v", ctx.Err())
	}
	var issueCount, sessionCount int
	if err := testPool.QueryRow(ctx, `SELECT COUNT(*) FROM issue WHERE id = $1`, issue.ID).Scan(&issueCount); err != nil {
		t.Fatalf("count deleted issue: %v", err)
	}
	if err := testPool.QueryRow(ctx, `SELECT COUNT(*) FROM card_session WHERE workspace_id = $1 AND issue_id = $2`, workspaceID, issue.ID).Scan(&sessionCount); err != nil {
		t.Fatalf("count deleted issue sessions: %v", err)
	}
	if issueCount != 0 || sessionCount != 0 {
		t.Fatalf("after delete race issue_count=%d card_session_count=%d; want both zero", issueCount, sessionCount)
	}
}

func TestExistingAssignedTodoAllocatesGenerationLazilyAtTaskStart(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	_, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	var before int
	if err := testPool.QueryRow(ctx, `SELECT COUNT(*) FROM card_session WHERE issue_id = $1`, issueID).Scan(&before); err != nil {
		t.Fatalf("count preexisting sessions: %v", err)
	}
	if before != 0 {
		t.Fatalf("assigned todo issue already has %d generations; upgrade must remain lazy", before)
	}
	var taskID pgtype.UUID
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
		VALUES ($1, $2, $3, 'dispatched', 0, now()) RETURNING id`, agentID, runtimeID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })
	started, err := testHandler.TaskService.StartTaskWithCardSessionLease(ctx, taskID, "test-daemon")
	if err != nil {
		t.Fatalf("start task: %v", err)
	}
	if !started.CardSessionID.Valid {
		t.Fatal("task start did not allocate a card-session generation")
	}
	var state string
	var generation int64
	if err := testPool.QueryRow(ctx, `SELECT state, generation FROM card_session WHERE id = $1`, started.CardSessionID).Scan(&state, &generation); err != nil {
		t.Fatalf("read lazily created generation: %v", err)
	}
	if state != "open" || generation != 1 {
		t.Fatalf("lazy generation = state %q generation %d, want open generation 1", state, generation)
	}
}

func TestCardSessionLeaseFencesOwnerAndDoesNotTouchActivity(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	_, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	var taskID pgtype.UUID
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
		VALUES ($1, $2, $3, 'dispatched', 0, now()) RETURNING id`, agentID, runtimeID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("insert dispatched task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })

	started, err := testHandler.TaskService.StartTaskWithCardSessionLease(ctx, taskID, "daemon-a")
	if err != nil {
		t.Fatalf("start task with lease: %v", err)
	}
	if !started.CardSessionID.Valid {
		t.Fatal("started task has no card session binding")
	}
	initial, err := testHandler.Queries.GetCardSession(ctx, started.CardSessionID)
	if err != nil {
		t.Fatalf("load acquired card session: %v", err)
	}
	if !initial.LeaseOwner.Valid || initial.LeaseOwner.String != "daemon-a" || initial.LeaseEpoch != 1 {
		t.Fatalf("initial lease = owner=%q valid=%t epoch=%d, want daemon-a/1", initial.LeaseOwner.String, initial.LeaseOwner.Valid, initial.LeaseEpoch)
	}
	activityBefore := initial.LastActivityAt

	heartbeat, err := testHandler.TaskService.HeartbeatCardSessionLeaseByTask(ctx, taskID, "daemon-a", initial.LeaseEpoch)
	if err != nil {
		t.Fatalf("heartbeat lease: %v", err)
	}
	if !heartbeat.LastActivityAt.Valid || !activityBefore.Valid || !heartbeat.LastActivityAt.Time.Equal(activityBefore.Time) {
		t.Fatalf("heartbeat changed last_activity_at from %v to %v", activityBefore, heartbeat.LastActivityAt)
	}
	replayed, err := testHandler.TaskService.AcquireCardSessionLeaseForTask(ctx, taskID, "daemon-a")
	if err != nil {
		t.Fatalf("same-owner lease replay: %v", err)
	}
	if replayed.LeaseEpoch != initial.LeaseEpoch {
		t.Fatalf("same-owner replay advanced epoch from %d to %d", initial.LeaseEpoch, replayed.LeaseEpoch)
	}
	if _, err := testHandler.TaskService.AcquireCardSessionLeaseForTask(ctx, taskID, "daemon-b"); !errors.Is(err, service.ErrCardSessionLeaseUnavailable) {
		t.Fatalf("fresh lease takeover error = %v, want ErrCardSessionLeaseUnavailable", err)
	}

	if _, err := testPool.Exec(ctx, `UPDATE card_session SET lease_heartbeat_at = now() - interval '2 minutes' WHERE id = $1`, started.CardSessionID); err != nil {
		t.Fatalf("age lease heartbeat: %v", err)
	}
	takenOver, err := testHandler.TaskService.AcquireCardSessionLeaseForTask(ctx, taskID, "daemon-b")
	if err != nil {
		t.Fatalf("stale lease takeover: %v", err)
	}
	if !takenOver.LeaseOwner.Valid || takenOver.LeaseOwner.String != "daemon-b" || takenOver.LeaseEpoch != initial.LeaseEpoch+1 {
		t.Fatalf("takeover lease = owner=%q valid=%t epoch=%d, want daemon-b/%d", takenOver.LeaseOwner.String, takenOver.LeaseOwner.Valid, takenOver.LeaseEpoch, initial.LeaseEpoch+1)
	}
	if _, err := testHandler.TaskService.ReleaseCardSessionLeaseByTask(ctx, taskID, "daemon-a", initial.LeaseEpoch); !errors.Is(err, service.ErrCardSessionLeaseUnavailable) {
		t.Fatalf("stale release error = %v, want ErrCardSessionLeaseUnavailable", err)
	}
	if _, err := testHandler.TaskService.ReleaseCardSessionLeaseByTask(ctx, taskID, "daemon-b", takenOver.LeaseEpoch); err != nil {
		t.Fatalf("current owner release: %v", err)
	}
	cleared, err := testHandler.Queries.GetCardSession(ctx, started.CardSessionID)
	if err != nil {
		t.Fatalf("load released card session: %v", err)
	}
	if cleared.LeaseOwner.Valid || cleared.LeaseHeartbeatAt.Valid {
		t.Fatalf("released lease remains owner=%q heartbeat_valid=%t", cleared.LeaseOwner.String, cleared.LeaseHeartbeatAt.Valid)
	}
}

func TestExpiredCardSessionStartsFreshGenerationBeforeSweeper(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	cases := []struct {
		name          string
		inactive      string
		useComment    bool
		pendingStatus string
		wantClosed    bool
	}{
		{name: "comment_after_done", inactive: "done", useComment: true, wantClosed: true},
		{name: "comment_after_cancelled_with_deferred_task", inactive: "cancelled", useComment: true, pendingStatus: "deferred", wantClosed: true},
		{name: "backlog_to_active_status_with_queued_task", inactive: "backlog", pendingStatus: "queued", wantClosed: true},
		{name: "blocked_to_active_status_with_deferred_task", inactive: "blocked", pendingStatus: "deferred", wantClosed: true},
		{name: "blocked_to_active_status_with_running_task", inactive: "blocked", pendingStatus: "running"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertExpiredCardSessionTransition(t, tc.inactive, tc.useComment, tc.pendingStatus, tc.wantClosed)
		})
	}
}

func TestExpiredCapacityPausedCardSessionWithQueuedTaskKeepsGeneration(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	workspaceID, _, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	if _, err := testPool.Exec(ctx, `
		UPDATE workspace
		SET settings = jsonb_set(
			COALESCE(settings, '{}'::jsonb),
			'{card_sessions}',
			COALESCE(settings->'card_sessions', '{}'::jsonb) || '{"idle_timeout_hours":1,"max_open_sessions":100}'::jsonb,
			true
		)
		WHERE id = $1`, workspaceID); err != nil {
		t.Fatalf("set session limits: %v", err)
	}
	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}
	session, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
	if err != nil {
		t.Fatalf("ensure card session: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		UPDATE card_session
		SET state = 'paused', pause_reason = 'capacity', last_activity_at = now() - interval '2 hours',
		    provider_session_id = 'capacity-provider-session', work_dir = '/tmp/capacity-session'
		WHERE id = $1`, session.ID); err != nil {
		t.Fatalf("age capacity-paused session and set resume data: %v", err)
	}
	insertIssueTaskWithStatus(t, agentID, issueID, "queued")

	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'in_progress' WHERE id = $1`, issueID); err != nil {
		t.Fatalf("reactivate capacity-paused issue: %v", err)
	}

	var count int
	var state, pauseReason, providerSessionID, workDir string
	var generation int64
	if err := testPool.QueryRow(ctx, `
		SELECT COUNT(*), MAX(state), COALESCE(MAX(pause_reason), ''), COALESCE(MAX(provider_session_id), ''),
		       COALESCE(MAX(work_dir), ''), MAX(generation)
		FROM card_session WHERE issue_id = $1 AND agent_id = $2`, issueID, agentID,
	).Scan(&count, &state, &pauseReason, &providerSessionID, &workDir, &generation); err != nil {
		t.Fatalf("read capacity-paused card session: %v", err)
	}
	if count != 1 {
		t.Fatalf("card session generations = %d, want 1", count)
	}
	if state != "open" || pauseReason != "" {
		t.Fatalf("reactivated session = state %q pause_reason %q, want open without pause reason", state, pauseReason)
	}
	if generation != session.Generation {
		t.Fatalf("reactivated generation = %d, want unchanged %d", generation, session.Generation)
	}
	if providerSessionID != "capacity-provider-session" || workDir != "/tmp/capacity-session" {
		t.Fatalf("reactivated session resume data = (%q, %q), want retained values", providerSessionID, workDir)
	}
}

func assertExpiredCardSessionTransition(t *testing.T, inactiveStatus string, useComment bool, pendingStatus string, wantClosed bool) {
	t.Helper()
	ctx := context.Background()
	workspaceID, _, agentID, issueID, _ := createCardSessionCapacityFixture(t, ctx)
	if _, err := testPool.Exec(ctx, `
		UPDATE workspace
		SET settings = jsonb_set(
			COALESCE(settings, '{}'::jsonb),
			'{card_sessions}',
			COALESCE(settings->'card_sessions', '{}'::jsonb) || '{"idle_timeout_hours":1}'::jsonb,
			true
		)
		WHERE id = $1`, workspaceID); err != nil {
		t.Fatalf("set one-hour idle timeout: %v", err)
	}

	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}
	session, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
	if err != nil {
		t.Fatalf("open card session: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		UPDATE card_session SET provider_session_id = 'expired-provider-session', work_dir = '/tmp/expired-session'
		WHERE id = $1`, session.ID); err != nil {
		t.Fatalf("set provider resume state: %v", err)
	}
	if pendingStatus != "" {
		// Model work that was already queued when the card left its active
		// state. New active tasks are rejected on cancelled cards.
		insertIssueTaskWithStatus(t, agentID, issueID, pendingStatus)
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = $2 WHERE id = $1`, issueID, inactiveStatus); err != nil {
		t.Fatalf("move issue to %s: %v", inactiveStatus, err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE card_session SET last_activity_at = now() - interval '2 hours' WHERE id = $1`, session.ID); err != nil {
		t.Fatalf("age card session beyond timeout: %v", err)
	}

	if useComment {
		created, err := testHandler.Queries.CreateComment(ctx, db.CreateCommentParams{
			IssueID:     util.MustParseUUID(issueID),
			WorkspaceID: util.MustParseUUID(workspaceID),
			AuthorType:  "member",
			AuthorID:    util.MustParseUUID(testUserID),
			Content:     "resume after idle timeout",
			Type:        "comment",
		})
		if err != nil {
			t.Fatalf("create comment before idle sweeper: %v", err)
		}
		if created.IssueStatus != "in_review" {
			t.Fatalf("issue status after comment = %q, want in_review", created.IssueStatus)
		}
	} else {
		if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'in_review' WHERE id = $1`, issueID); err != nil {
			t.Fatalf("reactivate issue before idle sweeper: %v", err)
		}
	}

	type generation struct {
		id                pgtype.UUID
		generation        int64
		state             string
		providerSessionID pgtype.Text
		workDir           pgtype.Text
	}
	rows, err := testPool.Query(ctx, `
		SELECT id, generation, state, provider_session_id, work_dir
		FROM card_session WHERE issue_id = $1 AND agent_id = $2
		ORDER BY generation`, issueID, agentID)
	if err != nil {
		t.Fatalf("list issue card session generations: %v", err)
	}
	defer rows.Close()
	var generations []generation
	for rows.Next() {
		var row generation
		if err := rows.Scan(&row.id, &row.generation, &row.state, &row.providerSessionID, &row.workDir); err != nil {
			t.Fatalf("scan card session generation: %v", err)
		}
		generations = append(generations, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read card session generations: %v", err)
	}
	wantGenerationCount := 1
	if wantClosed {
		wantGenerationCount = 2
	}
	if len(generations) != wantGenerationCount {
		t.Fatalf("card session generations = %d, want %d", len(generations), wantGenerationCount)
	}
	old := generations[0]
	if old.id != session.ID || old.generation != session.Generation {
		t.Fatalf("original generation = id %s generation %d; want original %s generation %d",
			util.UUIDToString(old.id), old.generation, util.UUIDToString(session.ID), session.Generation)
	}
	if wantClosed {
		if old.state != "closed" {
			t.Fatalf("expired generation state = %q, want closed", old.state)
		}
		if old.providerSessionID.Valid || old.workDir.Valid {
			t.Fatalf("closed generation retained resume data: provider_session_id=%q work_dir=%q", old.providerSessionID.String, old.workDir.String)
		}
	} else if old.state != "open" || !old.providerSessionID.Valid || old.providerSessionID.String != "expired-provider-session" || !old.workDir.Valid || old.workDir.String != "/tmp/expired-session" {
		t.Fatalf("generation with running task = state %q provider %q workdir %q; want open with retained resume data",
			old.state, old.providerSessionID.String, old.workDir.String)
	}
	if !wantClosed {
		return
	}
	current := generations[1]
	if current.generation != session.Generation+1 || current.state != "open" {
		t.Fatalf("new generation = %d %q, want generation %d open", current.generation, current.state, session.Generation+1)
	}
	if current.providerSessionID.Valid || current.workDir.Valid {
		t.Fatalf("new generation inherited resume data: provider_session_id=%q work_dir=%q", current.providerSessionID.String, current.workDir.String)
	}
}

func TestCardSessionIdleTimeoutWaitsForUnfinishedWork(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	workspaceID, _, agentID, issueID, _ := createCardSessionCapacityFixture(t, ctx)
	if _, err := testPool.Exec(ctx, `
		UPDATE workspace
		SET settings = COALESCE(settings, '{}'::jsonb) ||
			'{"card_sessions":{"idle_timeout_hours":1,"max_open_sessions":1}}'::jsonb
		WHERE id = $1`, workspaceID); err != nil {
		t.Fatalf("set one-hour idle timeout: %v", err)
	}

	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}
	session, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
	if err != nil {
		t.Fatalf("open card session: %v", err)
	}

	taskID := insertIssueTaskWithStatus(t, agentID, issueID, "queued")
	if _, err := testPool.Exec(ctx, `UPDATE card_session SET last_activity_at = now() - interval '2 hours' WHERE id = $1`, session.ID); err != nil {
		t.Fatalf("age idle card session: %v", err)
	}

	expired, err := testHandler.Queries.ExpireCardSessionsForWorkspace(ctx, util.MustParseUUID(workspaceID))
	if err != nil {
		t.Fatalf("expire while task is queued: %v", err)
	}
	if len(expired) != 0 {
		t.Fatalf("expired %d sessions with unfinished work, want 0", len(expired))
	}
	var state string
	if err := testPool.QueryRow(ctx, `SELECT state FROM card_session WHERE id = $1`, session.ID).Scan(&state); err != nil {
		t.Fatalf("read session state while work remains: %v", err)
	}
	if state != "open" {
		t.Fatalf("state with unfinished task = %q, want open", state)
	}

	if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status = 'completed', completed_at = now() WHERE id = $1`, taskID); err != nil {
		t.Fatalf("complete queued task: %v", err)
	}
	expired, err = testHandler.Queries.ExpireCardSessionsForWorkspace(ctx, util.MustParseUUID(workspaceID))
	if err != nil {
		t.Fatalf("expire after task completion: %v", err)
	}
	if len(expired) != 1 || expired[0].ID != session.ID {
		t.Fatalf("expired sessions after task completion = %d, want this session", len(expired))
	}
	if expired[0].State != "closed" || expired[0].ClosedAt.Time.Before(time.Now().Add(-time.Minute)) {
		t.Fatalf("expired session = state %q, closed_at %v", expired[0].State, expired[0].ClosedAt)
	}
}

func TestClosedCardSessionRejectsLateTaskCallbacksFromPriorGeneration(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	for _, terminalStatus := range []string{"completed", "cancelled"} {
		t.Run(terminalStatus, func(t *testing.T) {
			ctx := context.Background()
			workspaceID, _, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
			if _, err := testPool.Exec(ctx, `
				UPDATE workspace
				SET settings = COALESCE(settings, '{}'::jsonb) ||
					'{"card_sessions":{"idle_timeout_hours":1,"max_open_sessions":100}}'::jsonb
				WHERE id = $1`, workspaceID); err != nil {
				t.Fatalf("set one-hour idle timeout: %v", err)
			}

			issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
			if err != nil {
				t.Fatalf("load issue: %v", err)
			}
			agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
			if err != nil {
				t.Fatalf("load agent: %v", err)
			}
			generationG, err := testHandler.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode)
			if err != nil {
				t.Fatalf("open generation G: %v", err)
			}

			var taskID pgtype.UUID
			if err := testPool.QueryRow(ctx, `
				INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
				VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), $2, 'dispatched', 0, now())
				RETURNING id`, agentID, issueID).Scan(&taskID); err != nil {
				t.Fatalf("insert dispatched task: %v", err)
			}
			t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })
			started, err := testHandler.TaskService.StartTaskWithCardSessionLease(ctx, taskID, "test-daemon")
			if err != nil {
				t.Fatalf("start task: %v", err)
			}
			if !started.CardSessionID.Valid || started.CardSessionID != generationG.ID {
				t.Fatalf("task card_session_id = %s (valid=%v), want generation G %s",
					util.UUIDToString(started.CardSessionID), started.CardSessionID.Valid, util.UUIDToString(generationG.ID))
			}
			lease, err := testHandler.Queries.GetCardSession(ctx, generationG.ID)
			if err != nil {
				t.Fatalf("load generation G lease: %v", err)
			}
			if err := testHandler.TaskService.UpdateCardSessionProviderState(ctx, taskID, "generation-G-provider", "/private/generation-G-workdir", lease.LeaseOwner.String, lease.LeaseEpoch); err != nil {
				t.Fatalf("pin generation G provider state: %v", err)
			}

			if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status = $2, completed_at = now() WHERE id = $1`, taskID, terminalStatus); err != nil {
				t.Fatalf("mark old task %s: %v", terminalStatus, err)
			}
			if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'backlog' WHERE id = $1`, issueID); err != nil {
				t.Fatalf("pause generation G: %v", err)
			}
			if _, err := testPool.Exec(ctx, `UPDATE card_session SET last_activity_at = now() - interval '2 hours' WHERE id = $1`, generationG.ID); err != nil {
				t.Fatalf("age generation G: %v", err)
			}
			if _, err := testPool.Exec(ctx, `UPDATE issue SET status = 'todo' WHERE id = $1`, issueID); err != nil {
				t.Fatalf("open generation G+1: %v", err)
			}

			var generationGPlus1 pgtype.UUID
			var generation int64
			var beforeActivity time.Time
			if err := testPool.QueryRow(ctx, `
				SELECT id, generation, last_activity_at
				FROM card_session WHERE issue_id = $1 AND agent_id = $2 AND state = 'open'`, issueID, agentID,
			).Scan(&generationGPlus1, &generation, &beforeActivity); err != nil {
				t.Fatalf("load generation G+1: %v", err)
			}
			if generation != generationG.Generation+1 || generationGPlus1 == generationG.ID {
				t.Fatalf("new generation = %d (%s), want generation %d distinct from G",
					generation, util.UUIDToString(generationGPlus1), generationG.Generation+1)
			}

			// Simulate a delayed pin and usage callback from task G after G+1 is
			// active. The callbacks must remain scoped to the task's fixed UUID.
			if err := testHandler.TaskService.UpdateCardSessionProviderState(ctx, taskID, "late-generation-G-provider", "/private/late-generation-G-workdir", lease.LeaseOwner.String, lease.LeaseEpoch); err != nil {
				t.Fatalf("late provider pin: %v", err)
			}
			if err := testHandler.Queries.UpsertTaskUsage(ctx, db.UpsertTaskUsageParams{
				TaskID: taskID, Provider: "test-provider", Model: "test-model",
				InputTokens: 701, OutputTokens: 809, CacheReadTokens: 13, CacheWriteTokens: 17,
			}); err != nil {
				t.Fatalf("record late generation G usage: %v", err)
			}
			if err := testHandler.TaskService.TouchCardSessionActivityForTask(ctx, taskID); err != nil {
				t.Fatalf("late activity touch: %v", err)
			}
			derivedUsage, err := testHandler.Queries.GetCardSessionTokenUsage(ctx, generationGPlus1)
			if err != nil {
				t.Fatalf("derive new generation token usage after old task callback: %v", err)
			}
			if derivedUsage.TotalInputTokens != 0 || derivedUsage.TotalOutputTokens != 0 ||
				derivedUsage.TotalCacheReadTokens != 0 || derivedUsage.TotalCacheWriteTokens != 0 ||
				derivedUsage.TaskCount != 0 {
				t.Fatalf("generation G+1 derived token usage includes old task: %#v", derivedUsage)
			}

			var afterActivity time.Time
			var providerSessionID, workDir pgtype.Text
			if err := testPool.QueryRow(ctx, `
				SELECT last_activity_at, provider_session_id, work_dir
				FROM card_session WHERE id = $1`, generationGPlus1,
			).Scan(&afterActivity, &providerSessionID, &workDir); err != nil {
				t.Fatalf("read generation G+1 after old callbacks: %v", err)
			}
			if !afterActivity.Equal(beforeActivity) {
				t.Fatalf("generation G+1 activity changed from %v to %v after old task callback", beforeActivity, afterActivity)
			}
			if providerSessionID.Valid || workDir.Valid {
				t.Fatalf("generation G+1 gained old provider state: session=%q workdir=%q", providerSessionID.String, workDir.String)
			}
		})
	}
}
