package daemon

import (
	"bytes"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestResumeDiagnosticsOmitSessionIDsAndPaths(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sessionID := "provider-session-sensitive-sentinel"
	priorWorkDir := filepath.Join(t.TempDir(), "prior-workdir-sensitive-sentinel")
	currentWorkDir := filepath.Join(t.TempDir(), "current-workdir-sensitive-sentinel")
	task := &Task{PriorSessionID: sessionID, PriorWorkDir: priorWorkDir}
	taskContext := &execenv.TaskContextForEnv{}
	gateResumeToReachableSession(task, taskContext, "claude", currentWorkDir, false, false, logger)

	firstLog := output.String()
	for _, secret := range []string{sessionID, priorWorkDir, currentWorkDir} {
		if strings.Contains(firstLog, secret) {
			t.Fatalf("resume gate log contains sensitive value %q: %s", secret, firstLog)
		}
	}
	for _, safeField := range []string{"session_id_present=true", "prior_workdir_present=true", "workdir_present=true"} {
		if !strings.Contains(firstLog, safeField) {
			t.Fatalf("resume gate log missing safe field %q: %s", safeField, firstLog)
		}
	}

	output.Reset()
	task = &Task{PriorSessionID: sessionID}
	taskContext = &execenv.TaskContextForEnv{}
	codexHome := filepath.Join(t.TempDir(), "codex-home-sensitive-sentinel")
	gateCodexResumeToRolloutPresence(task, taskContext, "codex", codexHome, logger)
	secondLog := output.String()
	for _, secret := range []string{sessionID, codexHome} {
		if strings.Contains(secondLog, secret) {
			t.Fatalf("Codex rollout gate log contains sensitive value %q: %s", secret, secondLog)
		}
	}
	for _, safeField := range []string{"session_id_present=true", "codex_home_present=true"} {
		if !strings.Contains(secondLog, safeField) {
			t.Fatalf("Codex rollout gate log missing safe field %q: %s", safeField, secondLog)
		}
	}
}

func TestAgentResultLogOmitsProviderErrorContents(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	errorMarker := "provider failure session_id=private-session-marker cwd=/private/provider-workdir-marker"
	logAgentResultDetail(logger, agent.Result{
		Status:    "failed",
		SessionID: "private-session-marker",
		Error:     errorMarker,
	})
	got := output.String()
	for _, secret := range []string{"private-session-marker", "/private/provider-workdir-marker", errorMarker} {
		if strings.Contains(got, secret) {
			t.Fatalf("agent result log contains provider error data %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "agent_error_present=true") || !strings.Contains(got, "session_id_present=true") {
		t.Fatalf("agent result log omitted safe presence fields: %s", got)
	}
}

func TestResumeRetryLogsOmitProviderErrorContents(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	errorMarker := "provider failure session_id=retry-private-session cwd=/private/retry-workdir"

	logSessionResumeRetry(logger, agent.Result{Error: errorMarker})
	logFreshSessionStartFailure(logger, errors.New(errorMarker))
	logFreshSessionRetryFailure(logger, "failed", errorMarker)

	got := output.String()
	for _, secret := range []string{"retry-private-session", "/private/retry-workdir", errorMarker} {
		if strings.Contains(got, secret) {
			t.Fatalf("resume retry log contains provider error data %q: %s", secret, got)
		}
	}
	for _, safeField := range []string{"error_present=true", "retry_error_present=true"} {
		if !strings.Contains(got, safeField) {
			t.Fatalf("resume retry log omitted safe field %q: %s", safeField, got)
		}
	}
}
