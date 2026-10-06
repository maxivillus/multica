-- Preserve whether a card-session run used the daemon's persistent host or
-- the provider's ordinary one-shot/resume contract. NULL keeps usage written
-- by older daemons and non-card runs backward-compatible.
ALTER TABLE task_usage
    ADD COLUMN card_session_mode TEXT;

ALTER TABLE task_usage
    ADD CONSTRAINT task_usage_card_session_mode_check
    CHECK (card_session_mode IS NULL OR card_session_mode IN ('persistent', 'resume'));
