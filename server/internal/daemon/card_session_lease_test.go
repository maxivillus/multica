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
