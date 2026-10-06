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
	closed  atomic.Bool
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
	s.closed.Store(true)
	s.backend.mu.Lock()
	s.backend.closes++
	s.backend.mu.Unlock()
	return nil
}

func (s *fakePersistentSession) ProcessID() int { return s.pid }

func (s *fakePersistentSession) IsClosed() bool {
	return s.closed.Load()
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

func TestCardSessionExecutionModeUsesResumeForOneShotBackend(t *testing.T) {
	mode, usePersistentHost := cardSessionExecutionModeFor(&oneShotOnlyBackend{}, "card-1")
	if mode != cardSessionExecutionModeResume {
		t.Fatalf("card session mode = %q, want %q", mode, cardSessionExecutionModeResume)
	}
	if usePersistentHost {
		t.Fatal("one-shot backend was routed to persistent card-session host")
	}
}

func TestCardSessionExecutionModeUsesPersistentHostForPersistentBackend(t *testing.T) {
	mode, usePersistentHost := cardSessionExecutionModeFor(&fakePersistentBackend{}, "card-1")
	if mode != cardSessionExecutionModePersistent {
		t.Fatalf("card session mode = %q, want %q", mode, cardSessionExecutionModePersistent)
	}
	if !usePersistentHost {
		t.Fatal("persistent backend was not routed to persistent card-session host")
	}
}

func TestCardSessionExecutionModeIsEmptyWithoutCardSession(t *testing.T) {
	mode, usePersistentHost := cardSessionExecutionModeFor(&fakePersistentBackend{}, "")
	if mode != "" {
		t.Fatalf("card session mode = %q, want empty", mode)
	}
	if usePersistentHost {
		t.Fatal("backend without a card session was routed to persistent host")
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

func TestCardSessionHostRegistryEvictsLostHostBeforeCallback(t *testing.T) {
	backend := &fakePersistentBackend{}
	registry := newCardSessionHostRegistry(nil)
	var oldHost *cardSessionHost
	lease := &cardSessionLeaseHandle{}
	lease.onLost = func() {
		_ = registry.closeAfterLeaseLoss(oldHost)
	}

	var err error
	oldHost, err = registry.acquire(context.Background(), "card-1", backend, agent.ExecOptions{})
	if err != nil {
		t.Fatalf("acquire old host: %v", err)
	}
	if !oldHost.attachLease(lease) {
		t.Fatal("attach old lease")
	}
	onLost, markedLost := lease.markLost()
	if !markedLost || onLost == nil {
		t.Fatal("lease was not marked lost with a callback")
	}

	// The heartbeat has fenced the lease, but its callback is deliberately
	// paused. acquire must still remove the old process before returning it.
	replacement, err := registry.acquire(context.Background(), "card-1", backend, agent.ExecOptions{})
	if err != nil {
		t.Fatalf("acquire replacement host: %v", err)
	}
	if replacement == oldHost {
		t.Fatal("registry reused a host whose lease was already lost")
	}

	// A delayed callback from the old generation must not evict or close the
	// replacement now stored under the same card key.
	onLost()
	if replacement.isClosed() {
		t.Fatal("delayed old lease callback closed replacement host")
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

func TestCardSessionHostRegistryRejectsUnsupportedBackend(t *testing.T) {
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
