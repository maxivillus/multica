package daemon

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	cardSessionLeaseHeartbeatInterval = 30 * time.Second
	cardSessionLeaseReleaseTimeout    = 10 * time.Second
)

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
