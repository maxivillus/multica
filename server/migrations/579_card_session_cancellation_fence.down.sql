-- Restore the pre-579 trigger semantics when rolling back this migration.
-- The epoch repair is intentionally not reversed: reusing an older epoch
-- could make a proof valid again after the cancellation fence was removed.
CREATE OR REPLACE FUNCTION clear_card_session_lease_on_state_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state IS DISTINCT FROM NEW.state THEN
        NEW.lease_owner := NULL;
        NEW.lease_heartbeat_at := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS card_session_lease_state_fence ON card_session;
CREATE TRIGGER card_session_lease_state_fence
BEFORE UPDATE OF state ON card_session
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION clear_card_session_lease_on_state_change();
