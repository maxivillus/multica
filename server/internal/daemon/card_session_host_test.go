package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

type fakePersistentBackend struct {
	mu      sync.Mutex
	opens   int
	closes  int
	process int
	turns   int
	started chan struct{}
	release chan struct{}
}

func (f *fakePersistentBackend) Execute(context.Context, string, agent.ExecOptions) (*agent.Session, error) {
	return nil, errors.New("one-shot path should not be used")
}

func (f *fakePersistentBackend) OpenPersistent(context.Context, agent.ExecOptions) (agent.PersistentSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens++
	if f.process == 0 {
		f.process = 7000 + f.opens
	}
	return &fakePersistentSession{backend: f, pid: f.process}, nil
}

type fakePersistentSession struct {
	backend *fakePersistentBackend
	pid     int
}

func (s *fakePersistentSession) Execute(ctx context.Context, prompt string, _ agent.ExecOptions) (*agent.Session, error) {
	s.backend.mu.Lock()
	s.backend.turns++
	started := s.backend.started
	cont := s.backend.release
	s.backend.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	messages := make(chan agent.Message, 1)
	result := make(chan agent.Result, 1)
	go func() {
		defer close(messages)
		defer close(result)
		if cont != nil {
			select {
			case <-cont:
			case <-ctx.Done():
				result <- agent.Result{Status: "aborted", Error: ctx.Err().Error()}
				return
			}
		}
		result <- agent.Result{Status: "completed", Output: prompt, SessionID: "thread-1"}
	}()
	return &agent.Session{Messages: messages, Result: result}, nil
}

func (s *fakePersistentSession) Close() error {
	s.backend.mu.Lock()
	s.backend.closes++
	s.backend.mu.Unlock()
	return nil
}

func (s *fakePersistentSession) ProcessID() int { return s.pid }

func (s *fakePersistentSession) IsClosed() bool {
	return false
}

func TestCardSessionIDForTurnPrefersStartAcknowledgement(t *testing.T) {
	claimed := Task{CardSessionID: "claimed-session"}

	if got := cardSessionIDForTurn(claimed, CardSessionLease{CardSessionID: "started-session", LeaseEpoch: 7}); got != "started-session" {
		t.Fatalf("card session id = %q, want start acknowledgement id", got)
	}
	if got := cardSessionIDForTurn(claimed, CardSessionLease{}); got != "" {
		t.Fatalf("unfenced card session id = %q, want empty", got)
	}
}

func TestCardSessionIDForTurnSkipsUnfencedSession(t *testing.T) {
	claimed := Task{CardSessionID: "claimed-session"}

	if got := cardSessionIDForTurn(claimed, CardSessionLease{CardSessionID: "started-session"}); got != "" {
		t.Fatalf("unfenced card session id = %q, want empty", got)
	}
}

func TestCardSessionHostRegistryReusesOnePersistentProcess(t *testing.T) {
	backend := &fakePersistentBackend{}
	registry := newCardSessionHostRegistry(nil)
	host, err := registry.acquire(context.Background(), "card-1", backend, agent.ExecOptions{})
	if err != nil {
		t.Fatalf("acquire first host: %v", err)
	}
	firstPID := host.session.ProcessID()

	second, err := registry.acquire(context.Background(), "card-1", backend, agent.ExecOptions{}, 2*time.Hour)
	if err != nil {
		t.Fatalf("acquire second host: %v", err)
	}
	if second != host || second.session.ProcessID() != firstPID {
		t.Fatalf("host was not reused: first=%p/%d second=%p/%d", host, firstPID, second, second.session.ProcessID())
	}
	host.mu.Lock()
	idle := host.idle
	host.mu.Unlock()
	if idle != 2*time.Hour {
		t.Fatalf("host idle timeout = %s, want 2h", idle)
	}
	backend.mu.Lock()
	opens := backend.opens
	backend.mu.Unlock()
	if opens != 1 {
		t.Fatalf("OpenPersistent calls = %d, want 1", opens)
	}
}

func TestCardSessionHostRejectsConcurrentTurns(t *testing.T) {
	backend := &fakePersistentBackend{started: make(chan struct{}, 1), release: make(chan struct{})}
	registry := newCardSessionHostRegistry(nil)
	host, err := registry.acquire(context.Background(), "card-1", backend, agent.ExecOptions{})
	if err != nil {
		t.Fatalf("acquire host: %v", err)
	}
	first, err := host.execute(context.Background(), "first", agent.ExecOptions{})
	if err != nil {
		t.Fatalf("execute first turn: %v", err)
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("first turn did not start")
	}
	if _, err := host.execute(context.Background(), "second", agent.ExecOptions{}); err == nil {
		t.Fatal("concurrent turn succeeded, want serialized host rejection")
	}
	close(backend.release)
	for range first.Messages {
	}
	if _, ok := <-first.Result; ok {
		// The first result is intentionally consumed below as the channel may
		// close after the value; keeping this branch makes the test tolerant of
		// either ordering while still draining the session.
	}
}

func TestCardSessionHostRegistryClosesIdleHosts(t *testing.T) {
	backend := &fakePersistentBackend{}
	registry := newCardSessionHostRegistry(nil)
	host, err := registry.acquire(context.Background(), "card-1", backend, agent.ExecOptions{})
	if err != nil {
		t.Fatalf("acquire host: %v", err)
	}
	host.mu.Lock()
	host.lastUsed = time.Now().Add(-time.Hour)
	host.idle = 30 * time.Minute
	host.mu.Unlock()
	if got := registry.closeExpired(time.Now(), 30*time.Minute); got != 1 {
		t.Fatalf("closed hosts = %d, want 1", got)
	}
	backend.mu.Lock()
	closes := backend.closes
	backend.mu.Unlock()
	if closes != 1 {
		t.Fatalf("Close calls = %d, want 1", closes)
	}
}

func TestCardSessionHostRegistryDoesNotFallbackToOneShot(t *testing.T) {
	registry := newCardSessionHostRegistry(nil)
	_, err := registry.acquire(context.Background(), "card-1", &oneShotOnlyBackend{}, agent.ExecOptions{})
	if !errors.Is(err, ErrCardSessionPersistentUnsupported) {
		t.Fatalf("unsupported backend error = %v, want %v", err, ErrCardSessionPersistentUnsupported)
	}
}

func TestCardSessionHostKeepsLeaseUntilHostClose(t *testing.T) {
	var releases atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/daemon/tasks/task-1/card-session/release":
			releases.Add(1)
		case "/api/daemon/tasks/task-1/card-session/heartbeat":
		default:
			t.Fatalf("unexpected lease path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CardSessionLease{})
	}))
	defer srv.Close()

	oldInterval := cardSessionLeaseHeartbeatInterval
	cardSessionLeaseHeartbeatInterval = time.Millisecond
	defer func() { cardSessionLeaseHeartbeatInterval = oldInterval }()

	d := &Daemon{client: NewClient(srv.URL)}
	lease := d.newCardSessionLeaseHandle(context.Background(), "task-1", 7, nil, slog.Default())
	if lease == nil {
		t.Fatal("newCardSessionLeaseHandle returned nil")
	}
	host := &cardSessionHost{
		key:      "card-1",
		session:  &fakePersistentSession{backend: &fakePersistentBackend{}, pid: 7001},
		lease:    lease,
		lastUsed: time.Now(),
	}
	if err := host.close(); err != nil {
		t.Fatalf("close host: %v", err)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("lease releases = %d, want 1", got)
	}
}

type oneShotOnlyBackend struct{}

func (*oneShotOnlyBackend) Execute(context.Context, string, agent.ExecOptions) (*agent.Session, error) {
	return nil, nil
}
