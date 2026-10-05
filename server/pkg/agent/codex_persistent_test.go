package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCodexPersistentSessionReusesProcessAndThread(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	fakePath := filepath.Join(t.TempDir(), "codex")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then echo "codex-cli 0.0.0-test"; exit 0; fi
turn=0
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  method=$(printf '%s\n' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
  case "$method" in
    initialize) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    thread/start) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-live"}}}\n' "$id" ;;
    thread/name/set) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    turn/start)
      turn=$((turn+1))
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      printf '{"jsonrpc":"2.0","method":"turn/started","params":{"threadId":"thread-live","turn":{"id":"turn-%s"}}}\n' "$turn"
      printf '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-live","turnId":"turn-%s","item":{"type":"agentMessage","id":"message-%s","text":"answer-%s","phase":"final_answer"}}}\n' "$turn" "$turn" "$turn"
      printf '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-live","turn":{"id":"turn-%s","status":"completed"}}}\n' "$turn"
      ;;
    *) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
  esac
done
`
	writeTestExecutable(t, fakePath, []byte(script))
	backend, err := New("codex", Config{ExecutablePath: fakePath})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}
	host, err := backend.(PersistentBackend).OpenPersistent(context.Background(), ExecOptions{HandshakeTimeout: time.Second, ThreadHandshakeTimeout: time.Second})
	if err != nil {
		t.Fatalf("open persistent backend: %v", err)
	}
	persistent := host.(*codexPersistentSession)
	pid := persistent.ProcessID()
	defer persistent.Close()

	for i := 1; i <= 2; i++ {
		session, err := host.Execute(context.Background(), fmt.Sprintf("prompt-%d", i), ExecOptions{Timeout: 3 * time.Second, SemanticInactivityTimeout: time.Second})
		if err != nil {
			t.Fatalf("execute turn %d: %v", i, err)
		}
		for range session.Messages {
		}
		result, ok := <-session.Result
		if !ok {
			t.Fatalf("turn %d result channel closed without value", i)
		}
		if result.Status != "completed" || result.Output != fmt.Sprintf("answer-%d", i) {
			t.Fatalf("turn %d result = %+v", i, result)
		}
		if result.SessionID != "thread-live" {
			t.Fatalf("turn %d session id = %q", i, result.SessionID)
		}
		if got := persistent.ProcessID(); got != pid {
			t.Fatalf("turn %d process id = %d, want %d", i, got, pid)
		}
	}
	if persistent.IsClosed() {
		t.Fatal("persistent session closed between turns")
	}
	if strings.TrimSpace(persistent.threadID) != "thread-live" {
		t.Fatalf("thread id = %q", persistent.threadID)
	}
}

func TestCodexPersistentSessionClearsCurrentBeforePublishingResult(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	fakePath := filepath.Join(t.TempDir(), "codex")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then echo "codex-cli 0.0.0-test"; exit 0; fi
turn=0
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  method=$(printf '%s\n' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
  case "$method" in
    initialize) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    thread/start) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-live"}}}\n' "$id" ;;
    turn/start)
      turn=$((turn+1))
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      printf '{"jsonrpc":"2.0","method":"turn/started","params":{"threadId":"thread-live","turn":{"id":"turn-%s"}}}\n' "$turn"
      printf '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-live","turnId":"turn-%s","item":{"type":"agentMessage","id":"message-%s","text":"answer-%s","phase":"final_answer"}}}\n' "$turn" "$turn" "$turn"
      printf '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-live","turn":{"id":"turn-%s","status":"completed"}}}\n' "$turn"
      ;;
    *) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
  esac
done
`
	writeTestExecutable(t, fakePath, []byte(script))
	backend, err := New("codex", Config{ExecutablePath: fakePath})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}
	host, err := backend.(PersistentBackend).OpenPersistent(context.Background(), ExecOptions{HandshakeTimeout: time.Second, ThreadHandshakeTimeout: time.Second})
	if err != nil {
		t.Fatalf("open persistent backend: %v", err)
	}
	persistent := host.(*codexPersistentSession)
	defer persistent.Close()

	ready := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	persistent.beforeResultPublish = func() {
		once.Do(func() {
			close(ready)
			<-release
		})
	}
	first, err := host.Execute(context.Background(), "first", ExecOptions{Timeout: 3 * time.Second, SemanticInactivityTimeout: time.Second})
	if err != nil {
		t.Fatalf("first execute: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("first turn did not reach result handoff barrier")
	}
	second, err := host.Execute(context.Background(), "second", ExecOptions{Timeout: 3 * time.Second, SemanticInactivityTimeout: time.Second})
	if err != nil {
		t.Fatalf("second execute before first result publish: %v", err)
	}
	close(release)
	for range first.Messages {
	}
	if result, ok := <-first.Result; !ok || result.Status != "completed" {
		t.Fatalf("first result = %+v, open=%t", result, ok)
	}
	for range second.Messages {
	}
	if result, ok := <-second.Result; !ok || result.Status != "completed" {
		t.Fatalf("second result = %+v, open=%t", result, ok)
	}
}
