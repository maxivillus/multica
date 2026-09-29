package main

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type cardSessionMigrationVersion struct {
	newVersion    string
	legacyVersion string
	effectTable   string
}

func TestCardSessionMigrationRenumberPreservesLegacyAndAppliesFresh(t *testing.T) {
	basePool := openTestPool(t)
	versions := []cardSessionMigrationVersion{
		{"548_card_sessions", "420_card_sessions", "card_session_effect"},
		{"549_card_session_token_stats", "421_card_session_token_stats", "card_session_token_stats_effect"},
		{"550_card_session_cancel_retention", "422_card_session_cancel_retention", "card_session_cancel_retention_effect"},
	}

	for _, legacy := range versions {
		t.Run("legacy_"+legacy.newVersion, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			schema := createScratchSchema(t, ctx, basePool, "card_session_legacy_")
			pool := openTestPoolWithSearchPath(t, schema)
			createCardSessionMigrationLedger(t, ctx, pool)
			if _, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", legacy.legacyVersion); err != nil {
				t.Fatalf("seed legacy migration %s: %v", legacy.legacyVersion, err)
			}
			if _, err := pool.Exec(ctx, "CREATE TABLE "+legacy.effectTable+" (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatalf("seed existing %s schema: %v", legacy.legacyVersion, err)
			}

			upFiles := cardSessionMigrationFixtureFiles(t, versions, "up")
			if err := runCardSessionMigrationFixtures(ctx, pool, schema, "up", upFiles); err != nil {
				t.Fatalf("apply renumbered migrations over legacy ledger: %v", err)
			}
			for _, version := range versions {
				assertCardSessionMigrationTable(t, ctx, pool, version.effectTable, true)
				assertCardSessionMigrationVersion(t, ctx, pool, version.newVersion, true)
			}

			downFiles := cardSessionMigrationFixtureFiles(t, reverseCardSessionMigrationVersions(versions), "down")
			if err := runCardSessionMigrationFixtures(ctx, pool, schema, "down", downFiles); err != nil {
				t.Fatalf("roll back renumbered migrations over legacy ledger: %v", err)
			}
			for _, version := range versions {
				assertCardSessionMigrationTable(t, ctx, pool, version.effectTable, version.newVersion == legacy.newVersion)
				assertCardSessionMigrationVersion(t, ctx, pool, version.newVersion, false)
			}
			assertCardSessionMigrationVersion(t, ctx, pool, legacy.legacyVersion, true)
		})
	}

	t.Run("fresh_database", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		schema := createScratchSchema(t, ctx, basePool, "card_session_fresh_")
		pool := openTestPoolWithSearchPath(t, schema)
		createCardSessionMigrationLedger(t, ctx, pool)

		upFiles := cardSessionMigrationFixtureFiles(t, versions, "up")
		if err := runCardSessionMigrationFixtures(ctx, pool, schema, "up", upFiles); err != nil {
			t.Fatalf("apply renumbered migrations to fresh ledger: %v", err)
		}
		for _, version := range versions {
			assertCardSessionMigrationTable(t, ctx, pool, version.effectTable, true)
			assertCardSessionMigrationVersion(t, ctx, pool, version.newVersion, true)
			assertCardSessionMigrationVersion(t, ctx, pool, version.legacyVersion, false)
		}

		downFiles := cardSessionMigrationFixtureFiles(t, reverseCardSessionMigrationVersions(versions), "down")
		if err := runCardSessionMigrationFixtures(ctx, pool, schema, "down", downFiles); err != nil {
			t.Fatalf("roll back renumbered migrations on fresh ledger: %v", err)
		}
		for _, version := range versions {
			assertCardSessionMigrationTable(t, ctx, pool, version.effectTable, false)
			assertCardSessionMigrationVersion(t, ctx, pool, version.newVersion, false)
		}
	})
}

func cardSessionMigrationFixtureFiles(
	t *testing.T,
	versions []cardSessionMigrationVersion,
	direction string,
) []string {
	t.Helper()
	dir := t.TempDir()
	files := make([]string, 0, len(versions))
	for _, version := range versions {
		statement := "CREATE TABLE " + version.effectTable + " (id INTEGER PRIMARY KEY)"
		if direction == "down" {
			statement = "DROP TABLE " + version.effectTable
		}
		path := filepath.Join(dir, version.newVersion+"."+direction+".sql")
		if err := os.WriteFile(path, []byte(statement), 0o600); err != nil {
			t.Fatalf("write migration fixture %s: %v", path, err)
		}
		files = append(files, path)
	}
	return files
}

func reverseCardSessionMigrationVersions(versions []cardSessionMigrationVersion) []cardSessionMigrationVersion {
	reversed := append([]cardSessionMigrationVersion(nil), versions...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	return reversed
}

func createCardSessionMigrationLedger(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `CREATE TABLE schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		t.Fatalf("create migration ledger fixture: %v", err)
	}
}

func runCardSessionMigrationFixtures(ctx context.Context, pool *pgxpool.Pool, schema, direction string, files []string) error {
	return runMigrations(ctx, pool, runOptions{
		Direction:             direction,
		Files:                 files,
		SchemaMigrationsTable: schema + ".schema_migrations",
		AdvisoryLockKey:       int64(rand.Uint64()&0x7fffffffffffffff) | 1,
		Conditions:            conditionsForDirection(direction),
	})
}

func assertCardSessionMigrationTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, want bool) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	if exists != want {
		t.Fatalf("table %s exists=%v, want %v", table, exists, want)
	}
}

func assertCardSessionMigrationVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, version string, want bool) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", version).Scan(&exists); err != nil {
		t.Fatalf("check migration ledger row %s: %v", version, err)
	}
	if exists != want {
		t.Fatalf("migration ledger row %s exists=%v, want %v", version, exists, want)
	}
}
