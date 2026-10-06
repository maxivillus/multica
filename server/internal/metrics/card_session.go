package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// CardSessionMetrics contains the bounded diagnostic signals for the
// experimental card-session lifecycle. IDs and provider session values stay in
// structured logs only; Prometheus labels are deliberately fixed enums.
type CardSessionMetrics struct {
	Events            *prometheus.CounterVec
	OperationDuration *prometheus.HistogramVec
}

func NewCardSessionMetrics() *CardSessionMetrics {
	return &CardSessionMetrics{
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "multica",
			Subsystem: "card_session",
			Name:      "events_total",
			Help:      "Card-session diagnostic events by bounded event and result.",
		}, []string{"event", "result"}),
		OperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "multica",
			Subsystem: "card_session",
			Name:      "operation_duration_seconds",
			Help:      "Duration of card-session lifecycle operations by bounded event and result.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"event", "result"}),
	}
}

func (m *CardSessionMetrics) RecordEvent(event, result string) {
	if m == nil {
		return
	}
	m.Events.WithLabelValues(normalizeCardSessionMetricEvent(event), normalizeCardSessionMetricResult(result)).Inc()
}

func (m *CardSessionMetrics) ObserveOperation(event, result string, duration time.Duration) {
	if m == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	m.OperationDuration.WithLabelValues(normalizeCardSessionMetricEvent(event), normalizeCardSessionMetricResult(result)).Observe(duration.Seconds())
}

func (m *CardSessionMetrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}
	return []prometheus.Collector{m.Events, m.OperationDuration}
}

func normalizeCardSessionMetricEvent(event string) string {
	switch event {
	case "ensure", "terminal", "reopen", "expire", "provider_pin", "token_stats", "capacity":
		return event
	default:
		return "other"
	}
}

func normalizeCardSessionMetricResult(result string) string {
	switch result {
	case "created", "reused", "reopened", "ready", "retained", "expired", "updated", "published", "empty", "rejected", "noop", "unavailable", "error":
		return result
	default:
		return "other"
	}
}
