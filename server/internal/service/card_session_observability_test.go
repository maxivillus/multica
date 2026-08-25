package service

import (
	"io"
	"log/slog"
	"testing"

	obsmetrics "github.com/multica-ai/multica/server/internal/metrics"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCardSessionObservabilityCanBeDisabled(t *testing.T) {
	m := obsmetrics.NewCardSessionMetrics()
	svc := &TaskService{
		CardSessionMetrics:              m,
		CardSessionObservabilityEnabled: false,
	}
	taskID := dbid.NewV7()
	svc.ObserveCardSessionProviderPin(t.Context(), taskID, "updated")

	if got := testutil.ToFloat64(m.Events.WithLabelValues("provider_pin", "updated")); got != 0 {
		t.Fatalf("disabled card-session diagnostics recorded %v events", got)
	}
}

func TestCardSessionObservabilityRecordsEnabledEvents(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	m := obsmetrics.NewCardSessionMetrics()
	svc := &TaskService{
		CardSessionMetrics:              m,
		CardSessionObservabilityEnabled: true,
	}
	taskID := dbid.NewV7()
	svc.ObserveCardSessionProviderPin(t.Context(), taskID, "updated")

	if got := testutil.ToFloat64(m.Events.WithLabelValues("provider_pin", "updated")); got != 1 {
		t.Fatalf("enabled card-session diagnostics recorded %v events, want 1", got)
	}
}
