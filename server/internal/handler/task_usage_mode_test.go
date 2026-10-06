package handler

import "testing"

func TestNormalizeCardSessionModeAcceptsOnlyKnownModes(t *testing.T) {
	for _, mode := range []string{"persistent", "resume"} {
		got := normalizeCardSessionMode(mode)
		if !got.Valid || got.String != mode {
			t.Fatalf("normalizeCardSessionMode(%q) = %#v, want valid value", mode, got)
		}
	}
	if got := normalizeCardSessionMode(" resume "); !got.Valid || got.String != "resume" {
		t.Fatalf("normalizeCardSessionMode(whitespace) = %#v, want resume", got)
	}
	for _, mode := range []string{"", "one-shot", "PERSISTENT", "unknown"} {
		if got := normalizeCardSessionMode(mode); got.Valid {
			t.Fatalf("normalizeCardSessionMode(%q) = %#v, want NULL", mode, got)
		}
	}
}
