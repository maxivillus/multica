package cardsession

import "testing"

func TestParseDefaults(t *testing.T) {
	got, err := Parse([]byte(`{}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got != Defaults() {
		t.Fatalf("settings = %#v, want %#v", got, Defaults())
	}
}

func TestParseConfiguredValues(t *testing.T) {
	got, err := Parse([]byte(`{"card_sessions":{"idle_timeout_hours":48,"max_open_sessions":12}}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.IdleTimeoutHours != 48 || got.MaxOpenSessions != 12 {
		t.Fatalf("settings = %#v", got)
	}
}

func TestParseLegacySettingsKeepsEffectiveTimeout(t *testing.T) {
	got, err := Parse([]byte(`{"card_sessions":{"post_done_retention_hours":72,"token_stats_interval_minutes":30,"max_open_sessions":12}}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.IdleTimeoutHours != 72 || got.MaxOpenSessions != 12 {
		t.Fatalf("settings = %#v, want legacy timeout mapped to idle timeout", got)
	}
}

func TestParseNewTimeoutTakesPrecedenceOverLegacyTimeout(t *testing.T) {
	got, err := Parse([]byte(`{"card_sessions":{"idle_timeout_hours":24,"post_done_retention_hours":72}}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.IdleTimeoutHours != 24 {
		t.Fatalf("idle timeout = %d, want explicit new value 24", got.IdleTimeoutHours)
	}
}

func TestParseRejectsUnsafeBounds(t *testing.T) {
	for _, raw := range []string{
		`{"card_sessions":{"idle_timeout_hours":0}}`,
		`{"card_sessions":{"idle_timeout_hours":1000}}`,
		`{"card_sessions":{"max_open_sessions":0}}`,
		`{"card_sessions":{"max_open_sessions":10001}}`,
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("Parse(%s) accepted unsafe settings", raw)
		}
	}
}

func TestCapacityAvailable(t *testing.T) {
	settings := Settings{MaxOpenSessions: 2, IdleTimeoutHours: 24}
	if !settings.CapacityAvailable(1) {
		t.Fatal("capacity should be available below the limit")
	}
	if settings.CapacityAvailable(2) {
		t.Fatal("capacity should be exhausted at the limit")
	}
}
