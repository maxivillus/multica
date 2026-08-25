package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCardSessionMetricsRegisterAndRecord(t *testing.T) {
	m := NewCardSessionMetrics()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(m.Collectors()...)

	m.RecordEvent("terminal", "retained")
	m.RecordEvent("untrusted_event", "untrusted_result")
	m.ObserveOperation("reopen", "reopened", 25*time.Millisecond)
	if got := testutil.ToFloat64(m.Events.WithLabelValues("other", "other")); got != 1 {
		t.Fatalf("normalized diagnostic labels recorded %v events, want 1", got)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, family := range families {
		seen[family.GetName()] = true
	}
	for _, name := range []string{
		"multica_card_session_events_total",
		"multica_card_session_operation_duration_seconds",
	} {
		if !seen[name] {
			t.Fatalf("metric %s was not gathered", name)
		}
	}
}

func TestRegistryCardSessionMetricsCanBeDisabled(t *testing.T) {
	if got := NewRegistry(RegistryOptions{}).CardSession; got != nil {
		t.Fatal("card-session metrics must be absent when observability is disabled")
	}
	if got := NewRegistry(RegistryOptions{CardSessionObservability: true}).CardSession; got == nil {
		t.Fatal("card-session metrics must be wired when observability is enabled")
	}
}
