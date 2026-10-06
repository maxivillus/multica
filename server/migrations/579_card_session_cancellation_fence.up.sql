-- Cancellation changes the card session to paused in the same transaction as
-- the issue status write, while task cancellation is durable in a separate
-- outbox. Advancing the epoch when cancellation starts (or ends) closes that
-- interval: a stale terminal callback cannot use the NULL owner as proof after
-- the cancellation commit. An ordinary pause still clears the owner, but
-- keeps the epoch so the already-running task can finish through its normal
-- terminal callback; ordinary pause does not cancel the task.
CREATE OR REPLACE FUNCTION clear_card_session_lease_on_state_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state IS DISTINCT FROM NEW.state
       OR OLD.pause_reason IS DISTINCT FROM NEW.pause_reason THEN
        NEW.lease_owner := NULL;
        NEW.lease_heartbeat_at := NULL;
        IF (OLD.pause_reason IS DISTINCT FROM 'cancelled'
                AND NEW.pause_reason = 'cancelled')
           OR (OLD.pause_reason = 'cancelled'
                AND NEW.pause_reason IS DISTINCT FROM 'cancelled') THEN
            NEW.lease_epoch := OLD.lease_epoch + 1;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

-- Repair rows created or parked before the epoch-incrementing trigger existed.
-- Include rows whose owner is already NULL: their old owner+epoch proof is
-- still enough for the terminal CAS unless the generation is advanced.
UPDATE card_session
SET lease_owner = NULL,
    lease_heartbeat_at = NULL,
    lease_epoch = lease_epoch + 1,
    updated_at = now()
WHERE state <> 'open';

DROP TRIGGER IF EXISTS card_session_lease_state_fence ON card_session;
CREATE TRIGGER card_session_lease_state_fence
BEFORE UPDATE OF state, pause_reason ON card_session
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state
      OR OLD.pause_reason IS DISTINCT FROM NEW.pause_reason)
EXECUTE FUNCTION clear_card_session_lease_on_state_change();
