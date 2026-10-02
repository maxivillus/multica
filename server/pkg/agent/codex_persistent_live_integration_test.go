//go:build agentintegration

package agent

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestCodexRealPersistentTwoTurns is an opt-in acceptance smoke for the
// persistent card-session contract. It is intentionally excluded from CI:
// every turn uses an authenticated provider account and may consume quota.
func TestCodexRealPersistentTwoTurns(t *testing.T) {
	requireRealAgentSmoke(t)
	path, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("codex not on PATH; skipping real persistent Codex smoke")
	}

	env := map[string]string{}
	if home := strings.TrimSpace(os.Getenv("CODEX_HOME")); home != "" {
		env["CODEX_HOME"] = home
	}
	backend, err := New("codex", Config{ExecutablePath: path, Env: env})
	if err != nil {
		t.Fatalf("new Codex backend: %v", err)
	}
	persistent, ok := backend.(PersistentBackend)
	if !ok {
		t.Fatal("Codex backend does not expose PersistentBackend")
	}
	host, err := persistent.OpenPersistent(t.Context(), ExecOptions{
		Cwd:                    t.TempDir(),
		HandshakeTimeout:       45 * time.Second,
		ThreadHandshakeTimeout: 45 * time.Second,
	})
	if err != nil {
		t.Fatalf("open persistent Codex host: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })

	pid := host.ProcessID()
	var threadID string
	for i, prompt := range []string{
		"Reply with exactly first-turn-ok and do not use tools.",
		"Reply with exactly second-turn-ok and do not use tools.",
	} {
		session, err := host.Execute(t.Context(), prompt, ExecOptions{
			Cwd:                       t.TempDir(),
			Timeout:                   90 * time.Second,
			SemanticInactivityTimeout: 45 * time.Second,
			ThinkingLevel:             "low",
		})
		if err != nil {
			t.Fatalf("execute persistent turn %d: %v", i+1, err)
		}
		for range session.Messages {
		}
		result, ok := <-session.Result
		if !ok {
			t.Fatalf("persistent turn %d result channel closed without value", i+1)
		}
		if result.Status != "completed" {
			t.Fatalf("persistent turn %d result = %+v", i+1, result)
		}
		if result.SessionID == "" {
			t.Fatalf("persistent turn %d returned empty thread id", i+1)
		}
		if threadID == "" {
			threadID = result.SessionID
		} else if result.SessionID != threadID {
			t.Fatalf("persistent turn %d thread id = %q, want %q", i+1, result.SessionID, threadID)
		}
		if !strings.Contains(strings.ToLower(result.Output), strings.TrimSuffix(strings.TrimPrefix(prompt, "Reply with exactly "), " and do not use tools.")) {
			t.Fatalf("persistent turn %d output = %q, want prompt marker", i+1, result.Output)
		}
	}
	if got := host.ProcessID(); got != pid {
		t.Fatalf("persistent process id after two turns = %d, want %d", got, pid)
	}
	if host.IsClosed() {
		t.Fatal("persistent host closed between turns")
	}
	t.Logf("observed persistent Codex host pid=%d thread_id=%s turns=2", pid, threadID)
}
