package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestCreateComment_SquadMentionStampsSquadIDOnLeaderTask locks the enqueue
// side of the MUL-3730 fix: when a comment @mentions a squad, the leader task
// it enqueues must carry squad_id on the task row, so the daemon claim handler
// can locate the squad and inject the briefing (keyed off is_leader_task +
// squad_id, not issue assignee). The issue here is NOT assigned to the squad —
// exactly the comment-mention path that the old issue-assignee gate missed.
func TestCreateComment_SquadMentionStampsSquadIDOnLeaderTask(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var leaderID string
	if err := testPool.QueryRow(ctx, `
		SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1
	`, testWorkspaceID).Scan(&leaderID); err != nil {
		t.Fatalf("load leader agent: %v", err)
	}

	var squadID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO squad (workspace_id, name, description, leader_id, creator_id)
		VALUES ($1, 'Squad ID Stamp Squad', '', $2, $3)
		RETURNING id
	`, testWorkspaceID, leaderID, testUserID).Scan(&squadID); err != nil {
		t.Fatalf("create squad: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM squad WHERE id = $1`, squadID) })

	// Issue assigned to nobody (definitely not the squad) — the leader task is
	// produced purely by the @squad comment mention. Backlog mentions remain
	// explicit ordinary tasks and must not open a persistent card session.
	var issueID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, creator_type, creator_id, title, status)
		VALUES ($1, 'member', $2, 'squad_id stamp test', 'backlog')
		RETURNING id
	`, testWorkspaceID, testUserID).Scan(&issueID); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, issueID)
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
	})

	w := httptest.NewRecorder()
	r := newRequest("POST", "/api/issues/"+issueID+"/comments", map[string]any{
		"content": "[@Squad](mention://squad/" + squadID + ") please handle this",
	})
	r = withURLParam(r, "id", issueID)
	testHandler.CreateComment(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateComment: expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// The leader task must be queued AND carry squad_id = squadID, with
	// is_leader_task = true.
	var taskID, gotSquadID string
	var isLeader bool
	if err := testPool.QueryRow(ctx, `
		SELECT id, squad_id::text, is_leader_task
		FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'
	`, issueID, leaderID).Scan(&taskID, &gotSquadID, &isLeader); err != nil {
		t.Fatalf("load leader task: %v", err)
	}
	if gotSquadID != squadID {
		t.Fatalf("leader task squad_id = %q, want %q", gotSquadID, squadID)
	}
	if !isLeader {
		t.Fatalf("leader task is_leader_task = false, want true")
	}
	var cardSessionCount int
	if err := testPool.QueryRow(ctx, `SELECT COUNT(*) FROM card_session WHERE issue_id = $1`, issueID).Scan(&cardSessionCount); err != nil {
		t.Fatalf("count backlog card sessions: %v", err)
	}
	if cardSessionCount != 0 {
		t.Fatalf("backlog mention created %d card sessions, want none", cardSessionCount)
	}
	if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status = 'dispatched', dispatched_at = now() WHERE id = $1`, taskID); err != nil {
		t.Fatalf("dispatch ordinary backlog mention task: %v", err)
	}
	started, err := testHandler.TaskService.StartTaskWithCardSessionLease(ctx, util.MustParseUUID(taskID), "test-daemon")
	if err != nil {
		t.Fatalf("start ordinary backlog mention task: %v", err)
	}
	if started.CardSessionID.Valid {
		t.Fatalf("backlog mention task card_session_id = %s, want NULL", util.UUIDToString(started.CardSessionID))
	}
	if err := testHandler.TaskService.UpdateCardSessionProviderState(ctx, started.ID, "backlog-provider-sentinel", "/private/backlog-workdir"); err != nil {
		t.Fatalf("pin ordinary backlog task provider state: %v", err)
	}
	if err := testHandler.TaskService.TouchCardSessionActivityForTask(ctx, started.ID); err != nil {
		t.Fatalf("touch ordinary backlog task activity: %v", err)
	}
}

// TestCreateRetryTask_InheritsSquadID locks the retry-clone contract for the
// MUL-3730 fix: a retried leader task must inherit squad_id from its parent so
// the squad-leader briefing keeps being injected across retries. Parallels
// TestCreateRetryTask_InheritsIsLeaderTask.
func TestCreateRetryTask_InheritsSquadID(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	fx := newSquadCommentTriggerFixture(t)
	issueID := uuidToString(fx.Issue.ID)

	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	})

	var runtimeID string
	if err := testPool.QueryRow(ctx, `SELECT runtime_id FROM agent WHERE id = $1`, fx.LeaderID).Scan(&runtimeID); err != nil {
		t.Fatalf("load runtime: %v", err)
	}

	var parentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, attempt, max_attempts, is_leader_task, squad_id)
		VALUES ($1, $2, $3, 'failed', 1, 3, TRUE, $4)
		RETURNING id
	`, fx.LeaderID, runtimeID, issueID, fx.SquadID).Scan(&parentID); err != nil {
		t.Fatalf("seed parent task: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1 OR parent_task_id = $1`, parentID)
	})

	child, err := testHandler.Queries.CreateRetryTask(ctx, db.CreateRetryTaskParams{ID: util.MustParseUUID(parentID)})
	if err != nil {
		t.Fatalf("CreateRetryTask: %v", err)
	}
	if !child.SquadID.Valid || util.UUIDToString(child.SquadID) != fx.SquadID {
		t.Fatalf("child.SquadID = %v (valid=%v), want %s", util.UUIDToString(child.SquadID), child.SquadID.Valid, fx.SquadID)
	}
	if !child.IsLeaderTask {
		t.Fatalf("child.IsLeaderTask = false, want true (provenance must survive retry)")
	}
}
