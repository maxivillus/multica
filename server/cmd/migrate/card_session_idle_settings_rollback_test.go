package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestCardSessionIdleSettingsRollbackRestoresLegacyValues(t *testing.T) {
	t.Parallel()
	adminPool := openTestPool(t)
	ctx := context.Background()
	schema := createScratchSchema(t, ctx, adminPool, "card_session_settings_rollback_")
	pool := openTestPoolWithSearchPath(t, schema)
	if _, err := pool.Exec(ctx, `CREATE TABLE workspace (id UUID PRIMARY KEY, settings JSONB NOT NULL)`); err != nil {
		t.Fatalf("create workspace fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE issue (id UUID PRIMARY KEY, workspace_id UUID NOT NULL, status TEXT NOT NULL)`); err != nil {
		t.Fatalf("create issue fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE card_session (
		workspace_id UUID NOT NULL,
		issue_id UUID NOT NULL,
		state TEXT NOT NULL,
		done_at TIMESTAMPTZ,
		retain_until TIMESTAMPTZ,
		pause_reason TEXT,
		provider_session_id TEXT,
		work_dir TEXT,
		closed_at TIMESTAMPTZ,
		updated_at TIMESTAMPTZ
	)`); err != nil {
		t.Fatalf("create card session fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE FUNCTION issue_effective_status(UUID, TEXT) RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$ SELECT $2 $$`); err != nil {
		t.Fatalf("create effective status fixture function: %v", err)
	}

	workspaces := []struct {
		id       string
		settings string
	}{
		{"00000000-0000-0000-0000-000000000551", `{"card_sessions":{"post_done_retention_hours":72,"token_stats_interval_minutes":31,"max_open_sessions":7}}`},
		{"00000000-0000-0000-0000-000000000552", `{"card_sessions":{"idle_timeout_hours":36,"post_done_retention_hours":48,"token_stats_interval_minutes":27,"max_open_sessions":8}}`},
	}
	for _, workspace := range workspaces {
		if _, err := pool.Exec(ctx, `INSERT INTO workspace (id, settings) VALUES ($1, $2::jsonb)`, workspace.id, workspace.settings); err != nil {
			t.Fatalf("insert workspace fixture %s: %v", workspace.id, err)
		}
	}
	for i, workspace := range workspaces {
		issueID := []string{"00000000-0000-0000-0000-000000000651", "00000000-0000-0000-0000-000000000652"}[i]
		if _, err := pool.Exec(ctx, `INSERT INTO issue (id, workspace_id, status) VALUES ($1, $2, 'done')`, issueID, workspace.id); err != nil {
			t.Fatalf("insert done issue fixture %s: %v", issueID, err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO card_session (workspace_id, issue_id, state) VALUES ($1, $2, 'open')`, workspace.id, issueID); err != nil {
			t.Fatalf("insert existing card session fixture %s: %v", issueID, err)
		}
	}

	up := readMigration(t, "../../migrations/567_card_session_idle_lifecycle.up.sql")
	down := readMigration(t, "../../migrations/567_card_session_idle_lifecycle.down.sql")
	upSettings := statementBetween(t, up, "UPDATE workspace AS w\nSET settings", "\n\nCREATE OR REPLACE FUNCTION issue_status_allows_agent_task")
	downSettings := statementBetween(t, down, "UPDATE workspace AS w\nSET settings", "\n\nALTER TABLE card_session DROP CONSTRAINT")
	downCardSessions := statementBetween(t, down, "UPDATE card_session AS cs\nSET state", "\n\nALTER TABLE card_session\n    ADD CONSTRAINT")
	if _, err := pool.Exec(ctx, upSettings); err != nil {
		t.Fatalf("apply settings upgrade: %v", err)
	}

	assertSettings := func(id, idle, retention, interval, maxOpen, rollbackMetadata string) {
		t.Helper()
		var gotIdle, gotRetention, gotInterval, gotMax, gotRollback string
		if err := pool.QueryRow(ctx, `
			SELECT
				COALESCE(settings->'card_sessions'->>'idle_timeout_hours', ''),
				COALESCE(settings->'card_sessions'->>'post_done_retention_hours', ''),
				COALESCE(settings->'card_sessions'->>'token_stats_interval_minutes', ''),
				COALESCE(settings->'card_sessions'->>'max_open_sessions', ''),
				COALESCE(settings->'_migration_567_card_session_settings'->>'token_stats_interval_minutes', '')
			FROM workspace WHERE id = $1`, id).Scan(&gotIdle, &gotRetention, &gotInterval, &gotMax, &gotRollback); err != nil {
			t.Fatalf("read workspace settings %s: %v", id, err)
		}
		if gotIdle != idle || gotRetention != retention || gotInterval != interval || gotMax != maxOpen || gotRollback != rollbackMetadata {
			t.Fatalf("settings %s = idle %q retention %q interval %q max %q rollback %q; want %q %q %q %q %q",
				id, gotIdle, gotRetention, gotInterval, gotMax, gotRollback,
				idle, retention, interval, maxOpen, rollbackMetadata)
		}
	}
	assertSettings(workspaces[0].id, "72", "", "", "7", "31")
	assertSettings(workspaces[1].id, "36", "", "", "8", "27")

	if _, err := pool.Exec(ctx, downSettings); err != nil {
		t.Fatalf("apply settings rollback: %v", err)
	}
	for _, workspace := range workspaces {
		var idle, retention, interval, maxOpen string
		var rollbackMetadata bool
		if err := pool.QueryRow(ctx, `
			SELECT
				COALESCE(settings->'card_sessions'->>'idle_timeout_hours', ''),
				COALESCE(settings->'card_sessions'->>'post_done_retention_hours', ''),
				COALESCE(settings->'card_sessions'->>'token_stats_interval_minutes', ''),
				COALESCE(settings->'card_sessions'->>'max_open_sessions', ''),
				settings ? '_migration_567_card_session_settings'
			FROM workspace WHERE id = $1`, workspace.id).Scan(&idle, &retention, &interval, &maxOpen, &rollbackMetadata); err != nil {
			t.Fatalf("read rolled back workspace settings %s: %v", workspace.id, err)
		}
		if idle != "" || maxOpen != map[string]string{workspaces[0].id: "7", workspaces[1].id: "8"}[workspace.id] || rollbackMetadata {
			t.Fatalf("rollback metadata/settings for %s: idle %q max %q metadata=%t", workspace.id, idle, maxOpen, rollbackMetadata)
		}
		wantRetention, wantInterval := map[string][2]string{
			workspaces[0].id: {"72", "31"},
			workspaces[1].id: {"48", "27"},
		}[workspace.id][0], map[string][2]string{
			workspaces[0].id: {"72", "31"},
			workspaces[1].id: {"48", "27"},
		}[workspace.id][1]
		if retention != wantRetention || interval != wantInterval {
			t.Fatalf("rollback values for %s = retention %q interval %q; want %q %q", workspace.id, retention, interval, wantRetention, wantInterval)
		}
	}
	if _, err := pool.Exec(ctx, downCardSessions); err != nil {
		t.Fatalf("apply card session rollback: %v", err)
	}
	for i := range workspaces {
		issueID := []string{"00000000-0000-0000-0000-000000000651", "00000000-0000-0000-0000-000000000652"}[i]
		wantHours := []int{72, 48}[i]
		var state string
		var retentionMatches bool
		if err := pool.QueryRow(ctx, `
			SELECT state,
			       retain_until > now() + ($2 - 1) * interval '1 hour'
			       AND retain_until < now() + ($2 + 1) * interval '1 hour'
			FROM card_session WHERE issue_id = $1`, issueID, wantHours).Scan(&state, &retentionMatches); err != nil {
			t.Fatalf("read rolled back card session %s: %v", issueID, err)
		}
		if state != "done_retained" || !retentionMatches {
			t.Fatalf("rolled back card session %s: state=%q retention matches %dh=%t; want done_retained and configured retention", issueID, state, wantHours, retentionMatches)
		}
	}
}

func readMigration(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	return string(data)
}

func statementBetween(t *testing.T, source, start, end string) string {
	t.Helper()
	startIndex := strings.Index(source, start)
	if startIndex < 0 {
		t.Fatalf("migration is missing statement marker %q", start)
	}
	endIndex := strings.Index(source[startIndex:], end)
	if endIndex < 0 {
		t.Fatalf("migration is missing end marker %q after %q", end, start)
	}
	return source[startIndex : startIndex+endIndex]
}
