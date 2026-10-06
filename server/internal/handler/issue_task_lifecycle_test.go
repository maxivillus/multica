package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

func TestIssueTaskLifecycleEnqueueBeforeCancelIsCaptured(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issueUUID := util.MustParseUUID(issueID)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM issue_task_cancel_outbox WHERE issue_id = $1`, issueUUID)
	})

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin enqueue transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	qtx := testHandler.Queries.WithTx(tx)
	if err := qtx.LockIssueTaskLifecycle(ctx, issueUUID); err != nil {
		t.Fatalf("lock issue lifecycle: %v", err)
	}

	conn, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire status-update connection: %v", err)
	}
	defer conn.Release()
	var statusPID int32
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&statusPID); err != nil {
		t.Fatalf("read status-update backend id: %v", err)
	}
	statusUpdate := make(chan error, 1)
	go func() {
		_, err := conn.Exec(ctx, `UPDATE issue SET status = 'cancelled' WHERE id = $1`, issueUUID)
		statusUpdate <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		if err := testPool.QueryRow(ctx, `
			SELECT COALESCE(wait_event_type = 'Lock', FALSE)
			FROM pg_stat_activity WHERE pid = $1`, statusPID).Scan(&waiting); err != nil {
			t.Fatalf("check status-update lock wait: %v", err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatalf("status update did not wait on the enqueue transaction's lifecycle lock")
	}

	task, err := qtx.CreateAgentTask(ctx, db.CreateAgentTaskParams{
		ID:        dbid.NewV7(),
		AgentID:   util.MustParseUUID(agentID),
		RuntimeID: util.MustParseUUID(runtimeID),
		IssueID:   issueUUID,
	})
	if err != nil {
		t.Fatalf("create task while status update waits: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit enqueue transaction: %v", err)
	}
	if err := <-statusUpdate; err != nil {
		t.Fatalf("commit cancelled status update: %v", err)
	}

	var captured int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM issue_task_cancel_outbox WHERE issue_id = $1 AND task_id = $2`,
		issueUUID, task.ID).Scan(&captured); err != nil {
		t.Fatalf("read cancellation outbox: %v", err)
	}
	if captured != 1 {
		t.Fatalf("outbox rows for enqueued task = %d, want 1", captured)
	}
}

func TestIssueTaskLifecycleEnqueueCanProceedWhileIssueEditHoldsRow(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workspaceID, _, _, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issueUUID := util.MustParseUUID(issueID)
	workspaceUUID := util.MustParseUUID(workspaceID)

	issue, err := testHandler.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: issueUUID, WorkspaceID: workspaceUUID})
	if err != nil {
		t.Fatalf("read issue: %v", err)
	}
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin issue edit transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := testHandler.Queries.WithTx(tx).LockIssueForDescriptionUpdate(ctx, db.LockIssueForDescriptionUpdateParams{
		ID: issueUUID, WorkspaceID: workspaceUUID,
	}); err != nil {
		t.Fatalf("lock issue for edit: %v", err)
	}

	enqueued := make(chan db.AgentTaskQueue, 1)
	enqueueErr := make(chan error, 1)
	go func() {
		task, err := testHandler.TaskService.EnqueueTaskForIssue(ctx, issue, pgtype.UUID{})
		if err != nil {
			enqueueErr <- err
			return
		}
		enqueued <- task
	}()
	var task db.AgentTaskQueue
	select {
	case err := <-enqueueErr:
		t.Fatalf("enqueue while issue edit holds row: %v", err)
	case task = <-enqueued:
	case <-ctx.Done():
		t.Fatal("task enqueue waited on the issue edit row lock")
	}

	if _, err := tx.Exec(ctx, `UPDATE issue SET status = 'cancelled' WHERE id = $1`, issueUUID); err != nil {
		t.Fatalf("cancel issue after enqueue: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit issue edit transaction: %v", err)
	}
	var captured int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM issue_task_cancel_outbox WHERE issue_id = $1 AND task_id = $2`,
		issueUUID, task.ID).Scan(&captured); err != nil {
		t.Fatalf("read cancellation outbox: %v", err)
	}
	if captured != 1 {
		t.Fatalf("outbox rows for enqueued task = %d, want 1", captured)
	}
}

func TestIssueTaskLifecycleRejectsCancelledAndUnknownStatuses(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	workspaceID, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	issueUUID := util.MustParseUUID(issueID)
	workspaceUUID := util.MustParseUUID(workspaceID)
	agentUUID := util.MustParseUUID(agentID)
	runtimeUUID := util.MustParseUUID(runtimeID)

	createStatus := func(key, category string) {
		t.Helper()
		if _, err := testHandler.Queries.CreateIssueStatusEntry(ctx, db.CreateIssueStatusEntryParams{
			WorkspaceID: workspaceUUID,
			Key:         key,
			Name:        key,
			Description: "",
			Category:    category,
			Color:       "#123456",
		}); err != nil {
			t.Fatalf("create custom status %q: %v", key, err)
		}
	}
	createStatus("custom_closed", "closed")
	createStatus("custom_started", "started")
	createStatus("custom_done", "done")
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM issue_status WHERE workspace_id = $1 AND key IN ('custom_closed', 'custom_started', 'custom_done')`, workspaceUUID)
	})

	createTask := func() db.AgentTaskQueue {
		t.Helper()
		task, err := testHandler.Queries.CreateAgentTask(ctx, db.CreateAgentTaskParams{
			ID:        dbid.NewV7(),
			AgentID:   agentUUID,
			RuntimeID: runtimeUUID,
			IssueID:   issueUUID,
		})
		if err != nil {
			t.Fatalf("create issue task: %v", err)
		}
		return task
	}
	finishTask := func(task db.AgentTaskQueue) {
		t.Helper()
		if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status = 'completed', completed_at = now() WHERE id = $1`, task.ID); err != nil {
			t.Fatalf("finish task %s: %v", util.UUIDToString(task.ID), err)
		}
	}
	setStatus := func(status string) {
		t.Helper()
		if _, err := testPool.Exec(ctx, `UPDATE issue SET status = $2 WHERE id = $1`, issueUUID, status); err != nil {
			t.Fatalf("set issue status %q: %v", status, err)
		}
	}

	for _, status := range []string{"custom_started", "custom_done"} {
		setStatus(status)
		finishTask(createTask())
	}

	setStatus("custom_closed")
	if _, err := testHandler.Queries.CreateAgentTask(ctx, db.CreateAgentTaskParams{
		ID:        dbid.NewV7(),
		AgentID:   agentUUID,
		RuntimeID: runtimeUUID,
		IssueID:   issueUUID,
	}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("create task on custom closed status error = %v, want pgx.ErrNoRows", err)
	}

	setStatus("unknown_custom_status")
	if _, err := testHandler.Queries.CreateAgentTask(ctx, db.CreateAgentTaskParams{
		ID:        dbid.NewV7(),
		AgentID:   agentUUID,
		RuntimeID: runtimeUUID,
		IssueID:   issueUUID,
	}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("create task on unknown status error = %v, want pgx.ErrNoRows", err)
	}
	unknownIssue, err := testHandler.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: issueUUID, WorkspaceID: workspaceUUID})
	if err != nil {
		t.Fatalf("read issue with unknown status: %v", err)
	}
	if _, err := testHandler.TaskService.EnqueueTaskForIssue(ctx, unknownIssue, pgtype.UUID{}); err == nil {
		t.Fatal("enqueue with unknown issue status succeeded, want fail-closed error")
	}

	staleIssue, err := testHandler.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: issueUUID, WorkspaceID: workspaceUUID})
	if err != nil {
		t.Fatalf("read issue before cancellation: %v", err)
	}
	setStatus("cancelled")
	_, err = testHandler.TaskService.EnqueueTaskForIssue(ctx, staleIssue, pgtype.UUID{})
	if err == nil {
		t.Fatal("enqueue with stale active issue succeeded, want a closed-status error")
	}
	var activeTasks int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')`, issueUUID).Scan(&activeTasks); err != nil {
		t.Fatalf("count active issue tasks: %v", err)
	}
	if activeTasks != 0 {
		t.Fatalf("active tasks after enqueue on cancelled issue = %d, want 0", activeTasks)
	}
	if _, err := testHandler.Queries.CreateAgentTask(ctx, db.CreateAgentTaskParams{
		ID:        dbid.NewV7(),
		AgentID:   agentUUID,
		RuntimeID: runtimeUUID,
		IssueID:   issueUUID,
	}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("legacy task insert after cancelled error = %v, want pgx.ErrNoRows", err)
	}
}
