package main

import (
	"context"
	"testing"
)

func TestIssueTaskLifecycleMigrationBackfillsCancelledTasks(t *testing.T) {
	t.Parallel()
	adminPool := openTestPool(t)
	ctx := context.Background()
	schema := createScratchSchema(t, ctx, adminPool, "issue_task_lifecycle_backfill_")
	pool := openTestPoolWithSearchPath(t, schema)

	for _, statement := range []string{
		`CREATE TABLE issue (id UUID PRIMARY KEY, workspace_id UUID NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE agent_task_queue (id UUID PRIMARY KEY, issue_id UUID NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE issue_task_cancel_outbox (issue_id UUID NOT NULL, task_id UUID PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
		`CREATE FUNCTION issue_effective_status(UUID, TEXT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$ SELECT $2 $$`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("create migration fixture: %v", err)
		}
	}
	issueID := "00000000-0000-0000-0000-000000005590"
	queuedTaskID := "00000000-0000-0000-0000-000000005591"
	completedTaskID := "00000000-0000-0000-0000-000000005592"
	if _, err := pool.Exec(ctx, `INSERT INTO issue (id, workspace_id, status) VALUES ($1, $2, 'cancelled')`, issueID, "00000000-0000-0000-0000-000000005593"); err != nil {
		t.Fatalf("insert cancelled issue: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO agent_task_queue (id, issue_id, status) VALUES ($1, $3, 'queued'), ($2, $3, 'completed')`, queuedTaskID, completedTaskID, issueID); err != nil {
		t.Fatalf("insert issue tasks: %v", err)
	}

	source := readMigration(t, "../../migrations/559_issue_task_lifecycle_lock.up.sql")
	backfill := statementBetween(t, source, "INSERT INTO issue_task_cancel_outbox (issue_id, task_id)\nSELECT", "ON CONFLICT (task_id) DO NOTHING;") + "ON CONFLICT (task_id) DO NOTHING;"
	if _, err := pool.Exec(ctx, backfill); err != nil {
		t.Fatalf("apply cancellation outbox backfill: %v", err)
	}
	var got []string
	rows, err := pool.Query(ctx, `SELECT task_id::text FROM issue_task_cancel_outbox ORDER BY task_id`)
	if err != nil {
		t.Fatalf("read cancellation outbox: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			t.Fatalf("scan outbox task: %v", err)
		}
		got = append(got, taskID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("finish reading outbox: %v", err)
	}
	if len(got) != 1 || got[0] != queuedTaskID {
		t.Fatalf("backfilled task IDs = %v, want only active task %s", got, queuedTaskID)
	}
}
