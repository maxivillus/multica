package daemon

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

var cardSessionLeaseHeartbeatInterval = 30 * time.Second

const cardSessionLeaseReleaseTimeout = 10 * time.Second

// cardSessionLeaseHandle is the daemon-local owner of a card-session lease.
// Its heartbeat survives completion of the task that first opened the host;
// the lease remains fenced until the live provider process is closed.
type cardSessionLeaseHandle struct {
	d      *Daemon
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	log    *slog.Logger

	mu       sync.Mutex
	taskID   string
	epoch    int64
	onLost   func()
	lost     bool
	stopOnce sync.Once
}

func (d *Daemon) newCardSessionLeaseHandle(ctx context.Context, taskID string, epoch int64, onLost func(), taskLog *slog.Logger) *cardSessionLeaseHandle {
	if d == nil || d.client == nil || taskID == "" || epoch <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if taskLog == nil {
		taskLog = slog.Default()
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	h := &cardSessionLeaseHandle{
		d: d, ctx: heartbeatCtx, cancel: cancel, done: make(chan struct{}),
		log: taskLog, taskID: taskID, epoch: epoch, onLost: onLost,
	}
	go h.loop()
	return h
}

func (h *cardSessionLeaseHandle) loop() {
	defer close(h.done)
	ticker := time.NewTicker(cardSessionLeaseHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.mu.Lock()
			taskID, epoch := h.taskID, h.epoch
			h.mu.Unlock()
			if taskID == "" || epoch <= 0 {
				continue
			}
			if _, err := h.d.client.HeartbeatCardSessionLease(h.ctx, taskID, epoch); err != nil {
				if h.ctx.Err() != nil {
					return
				}
				h.mu.Lock()
				if h.lost {
					h.mu.Unlock()
					return
				}
				h.lost = true
				onLost := h.onLost
				h.mu.Unlock()
				h.log.Warn("card session lease heartbeat failed; stopping provider host",
					"task_id", taskID, "lease_epoch", epoch, "error", err)
				if onLost != nil {
					onLost()
				}
				return
			}
		}
	}
}

func (h *cardSessionLeaseHandle) update(taskID string, epoch int64, onLost func()) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if !h.lost {
		h.taskID, h.epoch, h.onLost = taskID, epoch, onLost
	}
	h.mu.Unlock()
}

// stop ends the heartbeat. A failed heartbeat must not issue a stale release
// against a replacement owner, so callers pass release=false in that case.
func (h *cardSessionLeaseHandle) stop(release bool) {
	if h == nil {
		return
	}
	h.stopOnce.Do(func() {
		h.cancel()
		<-h.done
		h.mu.Lock()
		taskID, epoch, lost := h.taskID, h.epoch, h.lost
		h.mu.Unlock()
		if !release || lost || taskID == "" || epoch <= 0 {
			return
		}
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), cardSessionLeaseReleaseTimeout)
		defer cancelRelease()
		if _, err := h.d.client.ReleaseCardSessionLease(releaseCtx, taskID, epoch); err != nil {
			h.log.Warn("card session lease release failed",
				"task_id", taskID, "lease_epoch", epoch, "error", err)
		}
	})
}

// maintainCardSessionLease keeps a server-issued provider-host fence alive
// while a task is executing. Losing the ability to prove ownership cancels the
// provider turn; continuing after a failed heartbeat could let a stale host
// overlap a replacement host after the server's stale-lease window expires.
func (d *Daemon) maintainCardSessionLease(ctx context.Context, taskID string, leaseEpoch int64, cancelRun context.CancelFunc, taskLog *slog.Logger) func() {
	return d.maintainCardSessionLeaseWithInterval(ctx, taskID, leaseEpoch, cancelRun, taskLog, cardSessionLeaseHeartbeatInterval)
}

func (d *Daemon) maintainCardSessionLeaseWithInterval(ctx context.Context, taskID string, leaseEpoch int64, cancelRun context.CancelFunc, taskLog *slog.Logger, interval time.Duration) func() {
	if d == nil || d.client == nil || taskID == "" || leaseEpoch <= 0 {
		return func() {}
	}
	if taskLog == nil {
		taskLog = slog.Default()
	}
	if interval <= 0 {
		interval = cardSessionLeaseHeartbeatInterval
	}

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if _, err := d.client.HeartbeatCardSessionLease(heartbeatCtx, taskID, leaseEpoch); err != nil {
					if heartbeatCtx.Err() != nil {
						return
					}
					taskLog.Warn("card session lease heartbeat failed; stopping provider turn",
						"task_id", taskID,
						"lease_epoch", leaseEpoch,
						"error", err,
					)
					if cancelRun != nil {
						cancelRun()
					}
					return
				}
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			stopHeartbeat()
			<-heartbeatDone
			releaseCtx, cancelRelease := context.WithTimeout(context.Background(), cardSessionLeaseReleaseTimeout)
			defer cancelRelease()
			if _, err := d.client.ReleaseCardSessionLease(releaseCtx, taskID, leaseEpoch); err != nil {
				taskLog.Warn("card session lease release failed",
					"task_id", taskID,
					"lease_epoch", leaseEpoch,
					"error", err,
				)
			}
		})
	}
}
