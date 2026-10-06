package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaintainCardSessionLeaseHeartbeatsAndReleases(t *testing.T) {
	var heartbeats atomic.Int32
	var releases atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/daemon/tasks/task-1/card-session/heartbeat":
			heartbeats.Add(1)
		case "/api/daemon/tasks/task-1/card-session/release":
			releases.Add(1)
		default:
			t.Fatalf("unexpected lease path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CardSessionLease{
			CardSessionID: "session-1",
			Generation:    3,
			LeaseEpoch:    7,
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	d := &Daemon{client: client}
	var cancelled atomic.Bool
	stop := d.maintainCardSessionLeaseWithInterval(
		context.Background(), "task-1", 7,
		func() { cancelled.Store(true) }, slog.Default(), time.Millisecond,
	)
	deadline := time.Now().Add(time.Second)
	for heartbeats.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()

	if heartbeats.Load() == 0 {
		t.Fatal("lease heartbeat was not sent")
	}
	if releases.Load() != 1 {
		t.Fatalf("lease releases = %d, want 1", releases.Load())
	}
	if cancelled.Load() {
		t.Fatal("successful lease heartbeat cancelled the provider turn")
	}
}

func TestMaintainCardSessionLeaseCancelsWhenHeartbeatIsRejected(t *testing.T) {
	var heartbeats atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/daemon/tasks/task-1/card-session/heartbeat" {
			heartbeats.Add(1)
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"lease replaced"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CardSessionLease{LeaseEpoch: 7})
	}))
	defer srv.Close()

	client := NewClient(srv.URL)
	d := &Daemon{client: client}
	var cancelled atomic.Bool
	stop := d.maintainCardSessionLeaseWithInterval(
		context.Background(), "task-1", 7,
		func() { cancelled.Store(true) }, slog.Default(), time.Millisecond,
	)
	defer stop()
	deadline := time.Now().Add(time.Second)
	for !cancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if heartbeats.Load() == 0 {
		t.Fatal("lease heartbeat was not sent")
	}
	if !cancelled.Load() {
		t.Fatal("rejected lease heartbeat did not cancel the provider turn")
	}
}

func TestCardSessionLeaseHandleIgnoresRejectedHeartbeatAfterRebind(t *testing.T) {
	firstHeartbeatStarted := make(chan struct{})
	releaseFirstHeartbeat := make(chan struct{})
	var heartbeats atomic.Int32
	var reboundHeartbeats atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/daemon/tasks/task-old/card-session/heartbeat" {
			if heartbeats.Add(1) == 1 {
				close(firstHeartbeatStarted)
				<-releaseFirstHeartbeat
			}
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"lease replaced"}`))
			return
		}
		if r.URL.Path != "/api/daemon/tasks/task-new/card-session/heartbeat" {
			t.Fatalf("unexpected lease path: %s", r.URL.Path)
		}
		reboundHeartbeats.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CardSessionLease{LeaseEpoch: 2})
	}))
	defer srv.Close()

	oldInterval := cardSessionLeaseHeartbeatInterval
	cardSessionLeaseHeartbeatInterval = time.Millisecond
	defer func() { cardSessionLeaseHeartbeatInterval = oldInterval }()

	d := &Daemon{client: NewClient(srv.URL)}
	var oldLost, newLost atomic.Bool
	h := d.newCardSessionLeaseHandle(
		context.Background(),
		"task-old",
		1,
		func() { oldLost.Store(true) },
		slog.Default(),
	)
	if h == nil {
		t.Fatal("newCardSessionLeaseHandle returned nil")
	}
	defer h.stop(false)

	select {
	case <-firstHeartbeatStarted:
	case <-time.After(time.Second):
		t.Fatal("old heartbeat did not start")
	}
	if !h.update("task-new", 2, func() { newLost.Store(true) }) {
		t.Fatal("lease rebind was rejected")
	}
	close(releaseFirstHeartbeat)

	deadline := time.Now().Add(time.Second)
	for reboundHeartbeats.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if oldLost.Load() || newLost.Load() {
		t.Fatalf("rejected heartbeat for old generation invoked cleanup: old=%t new=%t", oldLost.Load(), newLost.Load())
	}
	if reboundHeartbeats.Load() < 1 {
		t.Fatal("rebound heartbeat was not attempted")
	}
}
