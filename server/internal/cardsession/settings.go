// Package cardsession owns the platform settings and lifecycle vocabulary for
// server-owned per-issue agent generations.
package cardsession

import (
	"encoding/json"
	"fmt"
)

const (
	DefaultPostDoneRetentionHours = 24
	DefaultMaxOpenSessions        = 100
	MaxPostDoneRetentionHours     = 24 * 30
	MaxOpenSessions               = 10_000
)

// Settings are stored under workspace.settings.card_sessions. Values are
// deliberately bounded because this setting controls server-held provider
// state and therefore memory, MCP processes, and workspace capacity.
type Settings struct {
	PostDoneRetentionHours int
	MaxOpenSessions        int
}

type rawSettings struct {
	CardSessions *rawCardSessionSettings `json:"card_sessions"`
}

type rawCardSessionSettings struct {
	PostDoneRetentionHours *int `json:"post_done_retention_hours"`
	MaxOpenSessions        *int `json:"max_open_sessions"`
}

func Defaults() Settings {
	return Settings{
		PostDoneRetentionHours: DefaultPostDoneRetentionHours,
		MaxOpenSessions:        DefaultMaxOpenSessions,
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
	if decoded.CardSessions.PostDoneRetentionHours != nil {
		settings.PostDoneRetentionHours = *decoded.CardSessions.PostDoneRetentionHours
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
	if s.PostDoneRetentionHours < 1 || s.PostDoneRetentionHours > MaxPostDoneRetentionHours {
		return fmt.Errorf("card_sessions.post_done_retention_hours must be between 1 and %d", MaxPostDoneRetentionHours)
	}
	if s.MaxOpenSessions < 1 || s.MaxOpenSessions > MaxOpenSessions {
		return fmt.Errorf("card_sessions.max_open_sessions must be between 1 and %d", MaxOpenSessions)
	}
	return nil
}

// CapacityAvailable reports whether a new generation may be opened. Existing
// resumable generations do not consume another slot and are handled by the
// caller before this check.
func (s Settings) CapacityAvailable(openCount int64) bool {
	return openCount < int64(s.MaxOpenSessions)
}
