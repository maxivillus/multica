package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var activeTaskStatuses = []string{"queued", "dispatched", "running", "waiting_local_directory", "deferred"}

func insertIssueTaskWithStatus(t *testing.T, agentID, issueID, status string) string {
	t.Helper()
	var startedAt, fireAt, waitReason any
	switch status {
	case "running":
		startedAt = time.Now()
	case "deferred":
		fireAt = time.Now().Add(time.Hour)
	case "waiting_local_directory":
		waitReason = "waiting for local directory"
	}
	var taskID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority, issue_id, started_at, fire_at, wait_reason)
		VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), $2, 0, $3, $4, $5, $6)
		RETURNING id
	`, agentID, status, issueID, startedAt, fireAt, waitReason).Scan(&taskID); err != nil {
		t.Fatalf("insert %s task: %v", status, err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })
	return taskID
}

func insertCompletedIssueTask(t *testing.T, agentID, issueID string) string {
	t.Helper()
	var taskID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority, issue_id, started_at, completed_at)
		VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), 'completed', 0, $2, now(), now())
		RETURNING id
	`, agentID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("insert completed issue task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })
	return taskID
}

func TestUpdateIssueCancelledCancelsAllActiveIssueTasks(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}

	ownerAgent := createHandlerTestAgent(t, "CancelStatusOwner", []byte("[]"))
	mentionAgent := createHandlerTestAgent(t, "CancelStatusMention", []byte("[]"))
	otherIssueID := insertAgentAssignedIssue(t, ownerAgent, 92190, "cancel-status-other-issue")
	otherTaskID := insertRunningIssueTask(t, ownerAgent, otherIssueID)
	var taskIDs []string
	var completedTaskID string
	for i, status := range activeTaskStatuses {
		issueID := insertAgentAssignedIssue(t, ownerAgent, 92130+i, "cancel-status-"+status)
		taskIDs = append(taskIDs, insertIssueTaskWithStatus(t, ownerAgent, issueID, status))
		taskIDs = append(taskIDs, insertRunningIssueTask(t, mentionAgent, issueID))
		if i == 0 {
			completedTaskID = insertCompletedIssueTask(t, ownerAgent, issueID)
		}

		w := httptest.NewRecorder()
		req := newRequest("PUT", "/api/issues/"+issueID, map[string]any{"status": "cancelled"})
		req = withURLParam(req, "id", issueID)
		testHandler.UpdateIssue(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UpdateIssue cancel: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	}

	for _, taskID := range taskIDs {
		if got := taskStatus(t, taskID); got != "cancelled" {
			t.Fatalf("unfinished task on cancelled issue has status %q, want cancelled", got)
		}
	}
	if got := taskStatus(t, completedTaskID); got != "completed" {
		t.Fatalf("completed task changed to %q, want completed", got)
	}

	if got := taskStatus(t, otherTaskID); got != "running" {
		t.Fatalf("task on another issue changed to %q, want running", got)
	}
}

func TestBatchUpdateIssueCancelledCancelsAllActiveIssueTasks(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}

	ownerAgent := createHandlerTestAgent(t, "BatchCancelStatusOwner", []byte("[]"))
	mentionAgent := createHandlerTestAgent(t, "BatchCancelStatusMention", []byte("[]"))
	issueIDs := make([]string, 0, len(activeTaskStatuses))
	var taskIDs []string
	var completedTaskID string
	for i, status := range activeTaskStatuses {
		issueID := insertAgentAssignedIssue(t, ownerAgent, 92230+i, "batch-cancel-status-"+status)
		issueIDs = append(issueIDs, issueID)
		taskIDs = append(taskIDs, insertIssueTaskWithStatus(t, ownerAgent, issueID, status))
		if i == 0 {
			taskIDs = append(taskIDs, insertRunningIssueTask(t, mentionAgent, issueID))
			completedTaskID = insertCompletedIssueTask(t, ownerAgent, issueID)
		}
	}
	otherIssueID := insertAgentAssignedIssue(t, ownerAgent, 92290, "batch-cancel-status-other-issue")
	otherTaskID := insertRunningIssueTask(t, ownerAgent, otherIssueID)

	w := httptest.NewRecorder()
	req := newRequest("POST", "/api/issues/batch-update", map[string]any{
		"issue_ids": issueIDs,
		"updates":   map[string]any{"status": "cancelled"},
	})
	testHandler.BatchUpdateIssues(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("BatchUpdateIssues cancel: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	for _, taskID := range taskIDs {
		if got := taskStatus(t, taskID); got != "cancelled" {
			t.Fatalf("unfinished batch task has status %q, want cancelled", got)
		}
	}
	if got := taskStatus(t, completedTaskID); got != "completed" {
		t.Fatalf("completed batch task changed to %q, want completed", got)
	}

	if got := taskStatus(t, otherTaskID); got != "running" {
		t.Fatalf("task outside the batch changed to %q, want running", got)
	}
}
