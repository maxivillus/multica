package service

import (
	"testing"
	"time"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestFormatCardSessionTokenStats(t *testing.T) {
	got := formatCardSessionTokenStats(
		3,
		time.Date(2026, 8, 25, 9, 45, 0, 0, time.UTC),
		db.GetOpenCardSessionTokenUsageRow{
			TotalInputTokens:      1200,
			TotalOutputTokens:     340,
			TotalCacheReadTokens:  80,
			TotalCacheWriteTokens: 20,
			TaskCount:             4,
		},
	)
	want := "Intermediate token usage update for open card generation 3 (cumulative since the session opened, as of 2026-08-25T09:45:00Z): input 1200, output 340, cache read 80, cache write 20, tasks with usage 4."
	if got != want {
		t.Fatalf("formatCardSessionTokenStats() = %q, want %q", got, want)
	}
}
