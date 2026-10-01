// Package cardsession owns the platform settings and lifecycle vocabulary for
// server-owned per-issue agent generations.
package cardsession

import (
	"encoding/json"
	"fmt"
)

const (
	DefaultIdleTimeoutHours = 24
	DefaultMaxOpenSessions  = 100
	MaxIdleTimeoutHours     = 999
	MaxOpenSessionsLimit    = 10_000
)

// Settings are stored under workspace.settings.card_sessions. Values are
// deliberately bounded because this setting controls server-held provider
// state and therefore memory, MCP processes, and workspace capacity.
type Settings struct {
	IdleTimeoutHours int
	MaxOpenSessions  int
}

type rawSettings struct {
	CardSessions *rawCardSessionSettings `json:"card_sessions"`
}

type rawCardSessionSettings struct {
	IdleTimeoutHours             *int `json:"idle_timeout_hours"`
	LegacyPostDoneRetentionHours *int `json:"post_done_retention_hours"`
	MaxOpenSessions              *int `json:"max_open_sessions"`
}

func Defaults() Settings {
	return Settings{
		IdleTimeoutHours: DefaultIdleTimeoutHours,
		MaxOpenSessions:  DefaultMaxOpenSessions,
	}
}

// Parse reads card-session settings while preserving backwards compatibility
// for workspaces that have no card_sessions object yet.
func Parse(raw []byte) (Settings, error) {
	settings := Defaults()
	if len(raw) == 0 {
		return settings, nil
	}
	var decoded rawSettings
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return settings, fmt.Errorf("workspace settings: %w", err)
	}
	if decoded.CardSessions == nil {
		return settings, nil
	}
	if decoded.CardSessions.IdleTimeoutHours != nil {
		settings.IdleTimeoutHours = *decoded.CardSessions.IdleTimeoutHours
	} else if decoded.CardSessions.LegacyPostDoneRetentionHours != nil {
		// Existing workspaces used post-done retention as their session lifetime.
		// Map that saved value to idle timeout until an explicit new value is set.
		settings.IdleTimeoutHours = *decoded.CardSessions.LegacyPostDoneRetentionHours
	}
	if decoded.CardSessions.MaxOpenSessions != nil {
		settings.MaxOpenSessions = *decoded.CardSessions.MaxOpenSessions
	}
	if err := settings.Validate(); err != nil {
		return Settings{}, err
	}
	return settings, nil
}

func (s Settings) Validate() error {
	if s.IdleTimeoutHours < 1 || s.IdleTimeoutHours > MaxIdleTimeoutHours {
		return fmt.Errorf("card_sessions.idle_timeout_hours must be between 1 and %d", MaxIdleTimeoutHours)
	}
	if s.MaxOpenSessions < 1 || s.MaxOpenSessions > MaxOpenSessionsLimit {
		return fmt.Errorf("card_sessions.max_open_sessions must be between 1 and %d", MaxOpenSessionsLimit)
	}
	return nil
}

// CapacityAvailable reports whether a new generation may be opened. Existing
// resumable generations do not consume another slot and are handled by the
// caller before this check.
func (s Settings) CapacityAvailable(openCount int64) bool {
	return openCount < int64(s.MaxOpenSessions)
}
