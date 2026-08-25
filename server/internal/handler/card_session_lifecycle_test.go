package handler

import (
	"context"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestCardSessionRetainsDoneAndCancelledAndReopensOnComment pins the shared
// terminal lifecycle: both built-in terminal categories retain the current
// generation, and a follow-up comment returns the same generation to open
// without allocating a new one.
func TestCardSessionRetainsDoneAndCancelledAndReopensOnComment(t *testing.T) {
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

	for _, terminalStatus := range []string{"done", "cancelled"} {
		t.Run(terminalStatus, func(t *testing.T) {
			agentID := createHandlerTestAgent(t, "card-session-"+terminalStatus, nil)
			issueID := dbfx.Issue(t, "card session "+terminalStatus, testutil.Cols{
				"status": "todo",
			})
			sessionID := dbfx.Insert(t, "card_session", testutil.Cols{
				"workspace_id": testWorkspaceID,
				"issue_id":     issueID,
				"agent_id":     agentID,
				"provider":     "test",
			})

			if _, err := testPool.Exec(ctx, `UPDATE issue SET status = $2 WHERE id = $1`, issueID, terminalStatus); err != nil {
				t.Fatalf("set issue %s: %v", terminalStatus, err)
			}

			var retainedState string
			var retainedGeneration int64
			var retainUntil *time.Time
			if err := testPool.QueryRow(ctx, `
				SELECT state, generation, retain_until
				FROM card_session WHERE id = $1
			`, sessionID).Scan(&retainedState, &retainedGeneration, &retainUntil); err != nil {
				t.Fatalf("read retained card session: %v", err)
			}
			if retainedState != "done_retained" {
				t.Fatalf("state after %s = %q, want done_retained", terminalStatus, retainedState)
			}
			if retainUntil == nil || !retainUntil.After(time.Now()) {
				t.Fatalf("retain_until after %s = %v, want a future time", terminalStatus, retainUntil)
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
				t.Fatalf("issue status after %s comment = %q, want in_review", terminalStatus, created.IssueStatus)
			}

			var reopenedState string
			var reopenedGeneration int64
			if err := testPool.QueryRow(ctx, `
				SELECT state, generation
				FROM card_session WHERE id = $1
			`, sessionID).Scan(&reopenedState, &reopenedGeneration); err != nil {
				t.Fatalf("read reopened card session: %v", err)
			}
			if reopenedState != "open" {
				t.Fatalf("state after %s follow-up = %q, want open", terminalStatus, reopenedState)
			}
			if reopenedGeneration != retainedGeneration {
				t.Fatalf("generation after %s follow-up = %d, want unchanged %d", terminalStatus, reopenedGeneration, retainedGeneration)
			}
		})
	}
}
