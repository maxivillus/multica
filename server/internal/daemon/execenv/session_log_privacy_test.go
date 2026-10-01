package execenv

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionStoreDiagnosticsOmitFilesystemPaths(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sensitivePath := filepath.Join(t.TempDir(), "private-session-path-sentinel")

	logCodexAuthState(filepath.Join(sensitivePath, "auth.json"), logger)
	touchCodexSessionStore(sensitivePath, logger)
	touchHermesSessionStore(sensitivePath, logger)
	logs := output.String()
	if strings.Contains(logs, sensitivePath) {
		t.Fatalf("session store diagnostics contain private path %q: %s", sensitivePath, logs)
	}
	for _, safeField := range []string{"path_present=true", "store_present=true"} {
		if !strings.Contains(logs, safeField) {
			t.Fatalf("session store diagnostics missing safe field %q: %s", safeField, logs)
		}
	}
}
