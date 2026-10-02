package daemon

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunTaskRoutesCardSessionToOneShotBackend(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script ACP fixture is POSIX-only")
	}

	fake := filepath.Join(t.TempDir(), "grok-fixture")
	fixture := strings.NewReplacer("FIXTURE_INPUT", "1", "FIXTURE_COST", "0").Replace(taskUsageGrokFixture)
	writeTestExecutable(t, fake, []byte(fixture))

	var releases atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/daemon/tasks/task-card-one-shot/start":
			_, _ = io.WriteString(w, `{"card_session_id":"session-1","card_session_generation":1,"card_session_lease_epoch":7}`)
		case "/api/daemon/tasks/task-card-one-shot/card-session/release":
			releases.Add(1)
			_, _ = io.WriteString(w, `{}`)
		default:
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer srv.Close()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	d := &Daemon{
		client:         NewClient(srv.URL),
		logger:         logger,
		workspaces:     make(map[string]*workspaceState),
		activeEnvRoots: make(map[string]int),
		runtimeIndex:   map[string]Runtime{"rt-grok": {ID: "rt-grok", Provider: "grok"}},
		cfg: Config{
			WorkspacesRoot: t.TempDir(),
			AgentTimeout:   5 * time.Second,
			ServerBaseURL:  srv.URL,
			Agents:         map[string]AgentEntry{"grok": {Path: fake}},
		},
	}
	task := Task{
		ID:                  "task-card-one-shot",
		WorkspaceID:         "ws-card",
		RuntimeID:           "rt-grok",
		IssueID:             "issue-card",
		AgentID:             "agent-card",
		AuthToken:           "mat_card_one_shot",
		StartClaimSupported: false,
		Agent:               &AgentData{ID: "agent-card", Name: "card-agent", Model: "grok-4.6"},
	}

	result, err := d.runTask(context.Background(), task, "grok", 0, logger)
	if err != nil {
		t.Fatalf("runTask: %v", err)
	}
	if result.Status != "completed" || result.Comment != "done" {
		t.Fatalf("result = %+v, want completed one-shot result", result)
	}
	if d.cardSessionHosts != nil {
		t.Fatal("one-shot card task initialized a persistent host registry")
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("card-session lease releases = %d, want 1", got)
	}
	if !strings.Contains(logs.String(), "card_session_mode=resume") {
		t.Fatalf("task log does not identify resume mode: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "persistent=false") {
		t.Fatalf("task log does not identify non-persistent execution: %s", logs.String())
	}
}
