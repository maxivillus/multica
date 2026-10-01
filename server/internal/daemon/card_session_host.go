package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

// ErrCardSessionPersistentUnsupported is returned when a card task is routed
// to a backend that cannot keep one provider process alive between turns. A
// card session must fail explicitly in this case; falling back to Backend.Execute
// would recreate the old one-shot behavior while appearing to preserve the
// session.
var ErrCardSessionPersistentUnsupported = errors.New("provider backend does not support persistent card sessions")

// cardSessionHostRegistry owns the process handles for live card-session
// generations. It is daemon-local state: the database generation and lease
// remain the source of truth, while this registry is only the process handle
// that can be reused for the next task on the same generation.
type cardSessionHostRegistry struct {
	mu    sync.Mutex
	hosts map[string]*cardSessionHost
	log   *slog.Logger
}

type cardSessionHost struct {
	key     string
	session agent.PersistentSession
	log     *slog.Logger
	idle    time.Duration

	mu       sync.Mutex
	busy     bool
	closed   bool
	lastUsed time.Time
	lease    *cardSessionLeaseHandle
}

const cardSessionHostReaperInterval = time.Minute

func newCardSessionHostRegistry(logger *slog.Logger) *cardSessionHostRegistry {
	if logger == nil {
		logger = slog.Default()
	}
	return &cardSessionHostRegistry{hosts: make(map[string]*cardSessionHost), log: logger}
}

// acquire opens one persistent provider process for key, or returns the
// process already owned by this daemon. Creation is serialized so two tasks
// racing to claim the same generation cannot start duplicate processes before
// the server-side lease fencing response arrives.
func (r *cardSessionHostRegistry) acquire(ctx context.Context, key string, backend agent.Backend, opts agent.ExecOptions, idleTimeout ...time.Duration) (*cardSessionHost, error) {
	if r == nil || key == "" {
		return nil, fmt.Errorf("card session host key is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if host, ok := r.hosts[key]; ok {
		if host.isClosed() {
			delete(r.hosts, key)
		} else {
			if len(idleTimeout) > 0 && idleTimeout[0] > 0 {
				host.mu.Lock()
				host.idle = idleTimeout[0]
				host.mu.Unlock()
			}
			return host, nil
		}
	}
	persistent, ok := backend.(agent.PersistentBackend)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrCardSessionPersistentUnsupported, backend)
	}
	session, err := persistent.OpenPersistent(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("open persistent card session: %w", err)
	}
	idle := 24 * time.Hour
	if len(idleTimeout) > 0 && idleTimeout[0] > 0 {
		idle = idleTimeout[0]
	}
	host := &cardSessionHost{
		key:      key,
		session:  session,
		log:      r.log,
		idle:     idle,
		lastUsed: time.Now(),
	}
	r.hosts[key] = host
	r.log.Info("persistent card-session host started",
		"card_session_id", key,
		"pid", session.ProcessID(),
	)
	return host, nil
}

func (h *cardSessionHost) isClosed() bool {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return true
	}
	return h.session == nil || h.session.IsClosed()
}

func (h *cardSessionHost) hasLease() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	has := h.lease != nil
	h.mu.Unlock()
	return has
}

func (h *cardSessionHost) updateLease(taskID string, epoch int64, onLost func()) {
	if h == nil {
		return
	}
	h.mu.Lock()
	lease := h.lease
	h.mu.Unlock()
	if lease != nil {
		lease.update(taskID, epoch, onLost)
	}
}

func (h *cardSessionHost) attachLease(lease *cardSessionLeaseHandle) bool {
	if h == nil || lease == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.lease != nil {
		return false
	}
	h.lease = lease
	return true
}

// closeAfterLeaseLoss closes the provider without trying to release a lease
// whose ownership the server has already rejected. The heartbeat goroutine
// invokes this path, so it must not wait for its own done channel.
func (h *cardSessionHost) closeAfterLeaseLoss() error {
	if h == nil || h.session == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	session := h.session
	h.mu.Unlock()
	return session.Close()
}

func (h *cardSessionHost) execute(ctx context.Context, prompt string, opts agent.ExecOptions) (*agent.Session, error) {
	if h == nil || h.session == nil {
		return nil, errors.New("card session host is not initialized")
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, errors.New("card session host is closed")
	}
	if h.busy {
		h.mu.Unlock()
		return nil, errors.New("card session host is already executing a turn")
	}
	h.busy = true
	h.mu.Unlock()

	raw, err := h.session.Execute(ctx, prompt, opts)
	if err != nil {
		h.finishTurn()
		return nil, err
	}

	// The daemon's normal drain loop owns raw.Messages and raw.Result. Wrap the
	// channels only to release the host for the next queued task after the
	// terminal result has been observed; the provider process itself is kept by
	// h.session until Close or idle expiry.
	messages := make(chan agent.Message, 256)
	result := make(chan agent.Result, 1)
	go func() {
		defer close(messages)
		defer close(result)
		for msg := range raw.Messages {
			messages <- msg
		}
		if final, ok := <-raw.Result; ok {
			result <- final
		}
		h.finishTurn()
	}()

	return &agent.Session{
		Supplement:               raw.Supplement,
		SupplementReady:          raw.SupplementReady,
		ToolActivity:             raw.ToolActivity,
		InterruptBackgroundTools: raw.InterruptBackgroundTools,
		TerminalObserved:         raw.TerminalObserved,
		Messages:                 messages,
		Result:                   result,
	}, nil
}

func (h *cardSessionHost) finishTurn() {
	h.mu.Lock()
	h.busy = false
	h.lastUsed = time.Now()
	h.mu.Unlock()
}

func (h *cardSessionHost) close() error {
	if h == nil || h.session == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	lease := h.lease
	session := h.session
	h.mu.Unlock()
	if lease != nil {
		lease.stop(true)
	}
	return session.Close()
}

func (h *cardSessionHost) expired(now time.Time, idle time.Duration) bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.idle > 0 {
		idle = h.idle
	}
	if idle <= 0 {
		return false
	}
	return !h.closed && !h.busy && !h.lastUsed.IsZero() && now.Sub(h.lastUsed) >= idle
}

// closeExpired closes only idle hosts. Active turns are never interrupted by
// this local reaper; the server-side card-session expiry and lease fencing
// decide whether an active generation may continue.
func (r *cardSessionHostRegistry) closeExpired(now time.Time, idle time.Duration) int {
	if r == nil || idle <= 0 {
		return 0
	}
	r.mu.Lock()
	var expired []*cardSessionHost
	for key, host := range r.hosts {
		if host.isClosed() {
			delete(r.hosts, key)
			expired = append(expired, host)
			continue
		}
		if host.expired(now, idle) {
			delete(r.hosts, key)
			expired = append(expired, host)
		}
	}
	r.mu.Unlock()
	for _, host := range expired {
		if err := host.close(); err != nil {
			r.log.Warn("persistent card-session host close failed", "card_session_id", host.key, "error", err)
		}
	}
	return len(expired)
}

func (r *cardSessionHostRegistry) closeAll() {
	if r == nil {
		return
	}
	r.mu.Lock()
	hosts := make([]*cardSessionHost, 0, len(r.hosts))
	for key, host := range r.hosts {
		delete(r.hosts, key)
		hosts = append(hosts, host)
	}
	r.mu.Unlock()
	for _, host := range hosts {
		if err := host.close(); err != nil {
			r.log.Warn("persistent card-session host close failed", "card_session_id", host.key, "error", err)
		}
	}
}

// cardSessionHostReaper is deliberately separate from the lease heartbeat.
// Heartbeats prove process ownership but must not refresh card activity; this
// loop closes a process only after the local idle deadline has elapsed. The
// server remains authoritative and fences the host if its generation expires
// first.
func (d *Daemon) cardSessionHostReaper(ctx context.Context) {
	if d == nil || d.cardSessionHosts == nil {
		return
	}
	ticker := time.NewTicker(cardSessionHostReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			idle := d.cardSessionHostIdleTimeout
			if idle > 0 {
				d.cardSessionHosts.closeExpired(now, idle)
			}
		}
	}
}
