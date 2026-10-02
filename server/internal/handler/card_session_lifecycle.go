package handler

import (
	"context"
	"log/slog"
	"time"

	"github.com/multica-ai/multica/server/internal/issuestatus"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const issueTaskCancellationTimeout = 15 * time.Second

// reconcileCardSessionStatus pauses sessions outside the active statuses,
// resumes them when the issue becomes active, and opens one for a newly active
// issue. Cancelled also interrupts all current tasks on that issue.
func (h *Handler) reconcileCardSessionStatus(ctx context.Context, previous, current db.Issue) {
	if h.TaskService == nil || previous.Status == current.Status {
		return
	}
	status := issuestatus.Effective(ctx, h.Queries, current.WorkspaceID, current.Status)
	if status == issuestatus.Cancelled {
		// The issue status write has already committed when this reconciliation
		// runs. A client disconnect must not cancel the follow-up hard stop, or
		// the card can stay cancelled while its agent task keeps running. The
		// database outbox also lets the runtime sweeper retry if this attempt
		// fails or the process exits before it runs.
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), issueTaskCancellationTimeout)
		err := h.TaskService.CancelIssueTasksFromOutbox(cancelCtx, current.ID)
		cancel()
		if err != nil {
			slog.Warn("failed to cancel issue tasks after cancelled status",
				"issue_id", uuidToString(current.ID),
				"error", err,
			)
		}
	}
	if err := h.TaskService.SyncIssueCardSessions(ctx, current.ID, current.WorkspaceID); err != nil {
		slog.Warn("failed to sync card session after issue status change",
			"issue_id", uuidToString(current.ID),
			"error", err,
		)
	}
	if issuestatus.AllowsAgentTask(ctx, h.Queries, current.WorkspaceID, current.Status) {
		h.ensureIssueCardSession(ctx, current)
	}
}

func (h *Handler) reconcileCardSessionAssignee(ctx context.Context, issue db.Issue) {
	if h.TaskService == nil {
		return
	}
	if err := h.TaskService.SyncIssueCardSessions(ctx, issue.ID, issue.WorkspaceID); err != nil {
		slog.Warn("failed to sync card session after issue assignee change",
			"issue_id", uuidToString(issue.ID),
			"error", err,
		)
	}
	if issuestatus.AllowsAgentTask(ctx, h.Queries, issue.WorkspaceID, issue.Status) {
		h.ensureIssueCardSession(ctx, issue)
	}
}

func (h *Handler) ensureIssueCardSession(ctx context.Context, issue db.Issue) {
	if h.TaskService == nil || h.Queries == nil || !issue.AssigneeID.Valid {
		return
	}
	agent, err := h.Queries.GetAgent(ctx, issue.AssigneeID)
	if err != nil {
		slog.Warn("failed to load issue agent for card session",
			"issue_id", uuidToString(issue.ID),
			"error", err,
		)
		return
	}
	if _, err := h.TaskService.EnsureCardSession(ctx, issue.ID, issue.WorkspaceID, agent.ID, agent.RuntimeMode); err != nil {
		slog.Warn("failed to ensure card session after active issue status",
			"issue_id", uuidToString(issue.ID),
			"agent_id", uuidToString(agent.ID),
			"error", err,
		)
	}
}

// reopenTerminalIssueOnComment publishes the status transition performed by
// the CreateComment SQL statement and ensures the active generation exists.
// The database statement owns the transition itself, so this is a readback
// step rather than a second status write.
func (h *Handler) reopenTerminalIssueOnComment(ctx context.Context, issue db.Issue, newStatus, actorType, actorID string) db.Issue {
	if h.Queries == nil || newStatus == "" || newStatus == issue.Status || !issuestatus.IsTerminal(issuestatus.Effective(ctx, h.Queries, issue.WorkspaceID, issue.Status)) {
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
		if err := h.TaskService.SyncIssueCardSessions(ctx, updated.ID, updated.WorkspaceID); err != nil {
			slog.Warn("failed to sync card session after comment reopened issue",
				"issue_id", uuidToString(updated.ID),
				"error", err,
			)
		}
		if issuestatus.AllowsAgentTask(ctx, h.Queries, updated.WorkspaceID, updated.Status) {
			h.ensureIssueCardSession(ctx, updated)
		}
	}

	prefix := h.getIssuePrefix(ctx, updated.WorkspaceID)
	resp := issueToResponse(updated, prefix)
	h.fillStatusCategory(ctx, updated.WorkspaceID, &resp)
	h.publish(protocol.EventIssueUpdated, uuidToString(updated.WorkspaceID), actorType, actorID, map[string]any{
		"issue":          resp,
		"status_changed": true,
		"prev_status":    issue.Status,
		"source":         "comment_after_terminal",
	})
	return updated
}
