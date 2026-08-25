package handler

import (
	"context"
	"log/slog"

	"github.com/multica-ai/multica/server/internal/issuestatus"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// reconcileCardSessionStatus keeps the durable generation aligned with the
// issue's business status. A done transition enters the configured retention
// window; leaving done reopens the same generation while it is retained.
// Cancellation and other status changes deliberately do not close a session.
func (h *Handler) reconcileCardSessionStatus(ctx context.Context, previous, current db.Issue) {
	if h.TaskService == nil || previous.Status == current.Status {
		return
	}
	previousCategory := issuestatus.Effective(ctx, h.Queries, previous.WorkspaceID, previous.Status)
	currentCategory := issuestatus.Effective(ctx, h.Queries, current.WorkspaceID, current.Status)

	switch {
	case previousCategory != issuestatus.Done && currentCategory == issuestatus.Done:
		if err := h.TaskService.MarkIssueCardSessionsDone(ctx, current.ID, current.WorkspaceID); err != nil {
			slog.Warn("failed to retain card session after issue done",
				"issue_id", uuidToString(current.ID),
				"error", err,
			)
		}
	case previousCategory == issuestatus.Done && currentCategory != issuestatus.Done:
		if err := h.TaskService.ReopenIssueCardSessions(ctx, current.ID, current.WorkspaceID); err != nil {
			slog.Warn("failed to reopen retained card session",
				"issue_id", uuidToString(current.ID),
				"error", err,
			)
		}
	}
}

// reopenDoneIssueOnComment publishes the status transition performed by the
// CreateComment SQL statement and wakes the retained generation. The database
// statement owns the transition itself, so this helper is deliberately a
// readback/reconcile step rather than a second status write.
func (h *Handler) reopenDoneIssueOnComment(ctx context.Context, issue db.Issue, newStatus, actorType, actorID string) db.Issue {
	if h.Queries == nil || newStatus == "" || newStatus == issue.Status || issuestatus.Effective(ctx, h.Queries, issue.WorkspaceID, issue.Status) != issuestatus.Done {
		return issue
	}

	updated, err := h.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID:          issue.ID,
		WorkspaceID: issue.WorkspaceID,
	})
	if err != nil {
		slog.Warn("failed to read reopened issue after comment",
			"issue_id", uuidToString(issue.ID),
			"error", err,
		)
		return issue
	}

	if h.TaskService != nil {
		if err := h.TaskService.ReopenIssueCardSessions(ctx, updated.ID, updated.WorkspaceID); err != nil {
			slog.Warn("failed to reopen retained card session from comment",
				"issue_id", uuidToString(updated.ID),
				"error", err,
			)
		}
	}

	prefix := h.getIssuePrefix(ctx, updated.WorkspaceID)
	resp := issueToResponse(updated, prefix)
	h.fillStatusCategory(ctx, updated.WorkspaceID, &resp)
	h.publish(protocol.EventIssueUpdated, uuidToString(updated.WorkspaceID), actorType, actorID, map[string]any{
		"issue":          resp,
		"status_changed": true,
		"prev_status":    issue.Status,
		"source":         "comment_after_done",
	})
	return updated
}
