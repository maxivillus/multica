-- A provider host cannot remain the owner of a generation while the issue
-- lifecycle parks or closes that generation. Keep this invariant in the
-- database trigger path as well as in the application service path.
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

CREATE TRIGGER card_session_lease_state_fence
BEFORE UPDATE OF state ON card_session
FOR EACH ROW
WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION clear_card_session_lease_on_state_change();
