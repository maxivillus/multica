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
	backend, err := New("codex", Config{
		ExecutablePath: fakePath,
		Env:            map[string]string{"MULTICA_SERVER_URL": "http://127.0.0.1:1"},
	})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}
	host, err := backend.(PersistentBackend).OpenPersistent(context.Background(), ExecOptions{
		TaskAuthToken:          "mat_task_a",
		HandshakeTimeout:       time.Second,
		ThreadHandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("open persistent backend: %v", err)
	}
	persistent := host.(*codexPersistentSession)
	defer persistent.Close()

	ready := make(chan struct{})
	secondReady := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecondCleanup := make(chan struct{})
	firstCleaned := make(chan struct{})
	var hookMu sync.Mutex
	hookCount := 0
	persistent.beforeResultPublish = func() {
		hookMu.Lock()
		hookCount++
		count := hookCount
		hookMu.Unlock()
		if count == 1 {
			close(ready)
			<-releaseFirst
		}
	}
	var cleanupGateMu sync.Mutex
	cleanupGateCount := 0
	persistent.beforeTurnTokenCleanup = func() {
		cleanupGateMu.Lock()
		cleanupGateCount++
		count := cleanupGateCount
		cleanupGateMu.Unlock()
		if count == 2 {
			close(secondReady)
			<-releaseSecondCleanup
		}
	}
	var cleanupOnce sync.Once
	persistent.afterTurnTokenCleanup = func() {
		cleanupOnce.Do(func() { close(firstCleaned) })
	}
	first, err := host.Execute(context.Background(), "first", ExecOptions{TaskAuthToken: "mat_task_a", Timeout: 3 * time.Second, SemanticInactivityTimeout: time.Second})
	if err != nil {
		t.Fatalf("first execute: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("first turn did not reach result handoff barrier")
	}
	second, err := host.Execute(context.Background(), "second", ExecOptions{TaskAuthToken: "mat_task_b", Timeout: 3 * time.Second, SemanticInactivityTimeout: time.Second})
	if err != nil {
		t.Fatalf("second execute before first result publish: %v", err)
	}
	select {
	case <-secondReady:
	case <-time.After(2 * time.Second):
		t.Fatal("second turn did not reach token cleanup barrier")
	}
	close(releaseFirst)
	for range first.Messages {
	}
	if result, ok := <-first.Result; !ok || result.Status != "completed" {
		t.Fatalf("first result = %+v, open=%t", result, ok)
	}
	select {
	case <-firstCleaned:
	case <-time.After(2 * time.Second):
		t.Fatal("first turn did not finish token cleanup")
	}
	persistent.proxy.mu.RLock()
	current := persistent.proxy.currentToken
	persistent.proxy.mu.RUnlock()
	if current != "mat_task_b" {
		t.Fatalf("first turn cleanup changed next token to %q, want mat_task_b", current)
	}
	close(releaseSecondCleanup)
	for range second.Messages {
	}
	if result, ok := <-second.Result; !ok || result.Status != "completed" {
		t.Fatalf("second result = %+v, open=%t", result, ok)
	}
}

func TestCodexPersistentSessionRejectsExecuteAfterCloseDuringAdmission(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	fakePath := writeFakeCodexAppServer(t, `
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  method=$(printf '%s\n' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
  case "$method" in
    initialize) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    thread/start) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-close"}}}\n' "$id" ;;
    *) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
  esac
done
`)
	backend, err := New("codex", Config{
		ExecutablePath: fakePath,
		Env:            map[string]string{"MULTICA_SERVER_URL": "http://127.0.0.1:1"},
	})
	if err != nil {
		t.Fatalf("new codex backend: %v", err)
	}
	host, err := backend.(PersistentBackend).OpenPersistent(context.Background(), ExecOptions{
		TaskAuthToken:          "mat_task_a",
		HandshakeTimeout:       time.Second,
		ThreadHandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("open persistent backend: %v", err)
	}
	persistent := host.(*codexPersistentSession)

	admissionEntered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	persistent.beforeTurnAdmission = func() {
		close(admissionEntered)
		<-releaseAdmission
	}
	executeDone := make(chan struct {
		session *Session
		err     error
	}, 1)
	go func() {
		session, executeErr := host.Execute(context.Background(), "after close", ExecOptions{
			TaskAuthToken: "mat_task_b",
			Timeout:       time.Second,
		})
		executeDone <- struct {
			session *Session
			err     error
		}{session: session, err: executeErr}
	}()
	select {
	case <-admissionEntered:
	case <-time.After(2 * time.Second):
		_ = host.Close()
		t.Fatal("execute did not reach admission barrier")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- host.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close persistent backend: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not complete while admission was blocked")
	}
	close(releaseAdmission)
	result := <-executeDone
	if result.session != nil || result.err == nil || result.err.Error() != "codex persistent session is closed" {
		t.Fatalf("execute after close = session=%v err=%v, want closed error", result.session, result.err)
	}
	persistent.proxy.mu.RLock()
	currentToken := persistent.proxy.currentToken
	persistent.proxy.mu.RUnlock()
	if currentToken != "" {
		t.Fatalf("credential remained after rejected admission: %q", currentToken)
	}
}
