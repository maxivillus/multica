DROP TRIGGER IF EXISTS card_session_close_guard ON card_session;
DROP FUNCTION IF EXISTS guard_card_session_close();
DROP TRIGGER IF EXISTS card_session_issue_status_sync ON issue;
DROP FUNCTION IF EXISTS sync_card_session_issue_status();
DROP TABLE IF EXISTS card_session;
