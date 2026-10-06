package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type startAfterCommitTxStarter struct {
	pool        *pgxpool.Pool
	afterCommit func()
}

func (s *startAfterCommitTxStarter) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &startAfterCommitTx{Tx: tx, afterCommit: s.afterCommit}, nil
}

type startAfterCommitTx struct {
	pgx.Tx
	afterCommit func()
}

func (t *startAfterCommitTx) Commit(ctx context.Context) error {
	if err := t.Tx.Commit(ctx); err != nil {
		return err
	}
	if t.afterCommit != nil {
		t.afterCommit()
		t.afterCommit = nil
	}
	return nil
}

func startClaimFixture(t *testing.T, status string) (string, string, time.Time) {
	t.Helper()
	if testHandler == nil {
		t.Fatal("PostgreSQL is required for start claim tests")
	}
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id=$1 LIMIT 1`, testWorkspaceID).Scan(&agentID)
	issueID := dbfx.Issue(t, "start claim ownership")
	generation := time.Date(2026, 9, 17, 9, 0, 0, 123456000, time.UTC)
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id": issueID, "runtime_id": testRuntimeID,
		"status": status, "dispatched_at": generation,
	})
	return taskID, testRuntimeID, generation
}

func startClaimRequest(taskID, runtimeID string, generation time.Time) *http.Request {
	return withURLParam(newDaemonTokenRequest("POST", "/api/daemon/tasks/"+taskID+"/start", map[string]any{
		"runtime_id": runtimeID, "dispatched_at": generation.Format(time.RFC3339Nano),
		"capabilities": []string{protocol.DaemonCapabilityCardSessionLeaseV1},
	}, testWorkspaceID, "start-claim-test"), "taskId", taskID)
}

func TestStartClaimLostResponseAndOwnership(t *testing.T) {
	for _, state := range []string{"dispatched", "waiting_local_directory"} {
		t.Run(state, func(t *testing.T) {
			id, runtimeID, generation := startClaimFixture(t, state)
			// A stale generation on the same runtime cannot win the FIRST start.
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation.Add(-time.Microsecond))).Want(http.StatusConflict)
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, "00000000-0000-0000-0000-000000000001", generation)).Want(http.StatusConflict)
			// Commit the first request, then discard its response (lost acknowledgement).
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation)).Want(http.StatusOK)
			var before pgtype.Timestamptz
			dbfx.QueryRow(t, `SELECT started_at FROM agent_task_queue WHERE id=$1`, id).Scan(&before)
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation)).Want(http.StatusOK)
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation.Add(-time.Microsecond))).Want(http.StatusConflict)
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, "00000000-0000-0000-0000-000000000001", generation)).Want(http.StatusConflict)
			var after pgtype.Timestamptz
			dbfx.QueryRow(t, `SELECT started_at FROM agent_task_queue WHERE id=$1`, id).Scan(&after)
			if !before.Valid || !before.Time.Equal(after.Time) {
				t.Fatalf("replay changed started_at: %v -> %v", before, after)
			}
		})
	}
}

func TestStartClaimReclaimedGeneration(t *testing.T) {
	id, runtimeID, generation := startClaimFixture(t, "dispatched")
	// Reclaim refreshes dispatched_at even if the runtime stays the same.
	newGeneration := generation.Add(time.Microsecond)
	dbfx.Exec(t, `UPDATE agent_task_queue SET dispatched_at=$2 WHERE id=$1`, id, newGeneration)
	testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation)).Want(http.StatusConflict)
	testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, newGeneration)).Want(http.StatusOK)
	testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation)).Want(http.StatusConflict)
}

func TestStartClaimConcurrentReplay(t *testing.T) {
	id, runtimeID, generation := startClaimFixture(t, "dispatched")
	const workers = 12
	results := make(chan *httptest.ResponseRecorder, workers)
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			w := httptest.NewRecorder()
			testHandler.StartTask(w, startClaimRequest(id, runtimeID, generation))
			results <- w
		}()
	}
	close(gate)
	wg.Wait()
	close(results)
	var started string
	for w := range results {
		if w.Code != http.StatusOK {
			t.Fatalf("concurrent start: %d %s", w.Code, w.Body.String())
		}
		var task AgentTaskResponse
		if err := json.Unmarshal(w.Body.Bytes(), &task); err != nil {
			t.Fatal(err)
		}
		if task.StartedAt == nil {
			t.Fatal("missing started_at")
		}
		if started != "" && started != *task.StartedAt {
			t.Fatal("multiple start timestamps")
		}
		started = *task.StartedAt
	}
}

func TestStartClaimCancellationRace(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%t", replay), func(t *testing.T) {
			id, runtimeID, generation := startClaimFixture(t, "dispatched")
			if replay {
				testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation)).Want(http.StatusOK)
			}
			ctx := context.Background()
			tx, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, `UPDATE agent_task_queue SET status='cancelled' WHERE id=$1`, id); err != nil {
				t.Fatal(err)
			}
			result := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				testHandler.StartTask(w, startClaimRequest(id, runtimeID, generation))
				result <- w
			}()
			// Observe the actual PostgreSQL row-lock wait before committing cancellation.
			deadline := time.Now().Add(5 * time.Second)
			for {
				var blocked bool
				dbfx.QueryRow(t, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '-- name: LockAgentTaskStartClaim%')`).Scan(&blocked)
				if blocked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("start did not wait on cancellation lock")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case w := <-result:
				if w.Code != http.StatusConflict {
					t.Fatalf("cancelled start: %d %s", w.Code, w.Body.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("start blocked after cancellation committed")
			}
			testutil.Call(t, testHandler.StartTask, startClaimRequest(id, runtimeID, generation)).Want(http.StatusConflict)
			var status string
			dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, id).Scan(&status)
			if status != "cancelled" {
				t.Fatalf("cancellation overwritten: %s", status)
			}
		})
	}
}

func TestStartClaimInvalidBodiesAndLegacy(t *testing.T) {
	id, runtimeID, generation := startClaimFixture(t, "dispatched")
	for _, body := range []any{
		map[string]string{"runtime_id": runtimeID},
		map[string]string{"dispatched_at": generation.Format(time.RFC3339Nano)},
		map[string]string{"runtime_id": "invalid", "dispatched_at": generation.Format(time.RFC3339Nano)},
		map[string]string{"runtime_id": runtimeID, "dispatched_at": generation.Add(time.Nanosecond).Format(time.RFC3339Nano)},
	} {
		req := withURLParam(newDaemonTokenRequest("POST", "/start", body, testWorkspaceID, "legacy-test"), "taskId", id)
		testutil.Call(t, testHandler.StartTask, req).Want(http.StatusBadRequest)
	}
	modernNoCapabilityReq := withURLParam(newDaemonTokenRequest("POST", "/start", map[string]any{
		"runtime_id": runtimeID, "dispatched_at": generation.Format(time.RFC3339Nano),
	}, testWorkspaceID, "legacy-test"), "taskId", id)
	testutil.Call(t, testHandler.StartTask, modernNoCapabilityReq).Want(http.StatusConflict)
	legacyReq := withURLParam(newDaemonTokenRequest("POST", "/start", nil, testWorkspaceID, "legacy-test"), "taskId", id)
	testutil.Call(t, testHandler.StartTask, legacyReq).Want(http.StatusConflict)
	capabilityReq := withURLParam(newDaemonTokenRequest("POST", "/start", map[string]any{
		"capabilities": []string{protocol.DaemonCapabilityCardSessionLeaseV1},
	}, testWorkspaceID, "legacy-test"), "taskId", id)
	testutil.Call(t, testHandler.StartTask, capabilityReq).Want(http.StatusOK)
	replayReq := withURLParam(newDaemonTokenRequest("POST", "/start", nil, testWorkspaceID, "legacy-test"), "taskId", id)
	testutil.Call(t, testHandler.StartTask, replayReq).Want(http.StatusConflict)

	// A parked issue task is an ordinary comment/mention task. An old daemon
	// without the card-session capability may still start it; only active issue
	// tasks require the lease handshake.
	parkedID, parkedRuntimeID, parkedGeneration := startClaimFixture(t, "dispatched")
	dbfx.Exec(t, `UPDATE issue SET status = 'blocked' WHERE id = (SELECT issue_id FROM agent_task_queue WHERE id = $1)`, parkedID)
	parkedReq := withURLParam(newDaemonTokenRequest("POST", "/start", map[string]any{
		"runtime_id": parkedRuntimeID, "dispatched_at": parkedGeneration.Format(time.RFC3339Nano),
	}, testWorkspaceID, "legacy-test"), "taskId", parkedID)
	testutil.Call(t, testHandler.StartTask, parkedReq).Want(http.StatusOK)
}

func TestStartClaimWirePrecision(t *testing.T) {
	id, runtimeID, generation := startClaimFixture(t, "dispatched")
	task, err := testHandler.Queries.GetAgentTask(context.Background(), parseUUID(id))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := testHandler.Queries.GetAgentRuntime(context.Background(), parseUUID(runtimeID))
	if err != nil {
		t.Fatal(err)
	}
	req := newDaemonTokenRequest("POST", "/claim", nil, testWorkspaceID, "claim-wire-test")
	// Exercise non-UTC database timestamp locations even on a UTC CI runner.
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("UTC+08", 8*60*60), time.FixedZone("UTC-07", -7*60*60)} {
		t.Run(zone.String(), func(t *testing.T) {
			claimed := task
			claimed.DispatchedAt.Time = task.DispatchedAt.Time.In(zone)
			resp, _, _, _, _, failure := testHandler.buildClaimedTaskResponse(req, &claimed, runtime, runtimeID, testWorkspaceID)
			if failure != nil {
				t.Fatalf("build claim: %+v", failure)
			}
			want := generation.UTC().Format(time.RFC3339Nano)
			if !resp.StartClaimSupported || resp.DispatchedAt == nil {
				t.Fatalf("missing claim timestamp or capability: supported=%t timestamp=%v", resp.StartClaimSupported, resp.DispatchedAt)
			}
			if *resp.DispatchedAt != want {
				t.Fatalf("claim timestamp = %q, want canonical UTC with microseconds %q", *resp.DispatchedAt, want)
			}
		})
	}
}

func TestStartTaskPATFallbackUsesRuntimeDaemonLeaseOwner(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	ctx := context.Background()
	workspaceID, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	generation := time.Now().UTC().Truncate(time.Microsecond)
	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
		VALUES ($1, $2, $3, 'dispatched', 0, $4)
		RETURNING id`, agentID, runtimeID, issueID, generation).Scan(&taskID); err != nil {
		t.Fatalf("insert dispatched task: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID)
	})

	var daemonID string
	if err := testPool.QueryRow(ctx, `SELECT daemon_id FROM agent_runtime WHERE id = $1`, runtimeID).Scan(&daemonID); err != nil {
		t.Fatalf("load runtime daemon id: %v", err)
	}

	startReq := newRequestAsUser(testUserID, http.MethodPost, "/api/daemon/tasks/"+taskID+"/start", map[string]any{
		"runtime_id":    runtimeID,
		"dispatched_at": generation.Format(time.RFC3339Nano),
		"capabilities":  []string{protocol.DaemonCapabilityCardSessionLeaseV1},
	})
	startReq.Header.Set("X-Workspace-ID", workspaceID)
	startReq = withURLParam(startReq, "taskId", taskID)
	startResp := httptest.NewRecorder()
	testHandler.StartTask(startResp, startReq)
	if startResp.Code != http.StatusOK {
		t.Fatalf("PAT start status = %d: %s", startResp.Code, startResp.Body.String())
	}
	var started AgentTaskResponse
	if err := json.Unmarshal(startResp.Body.Bytes(), &started); err != nil {
		t.Fatalf("decode PAT start response: %v", err)
	}
	if started.CardSessionID == "" || started.CardSessionLeaseEpoch <= 0 {
		t.Fatalf("PAT start card lease = id %q epoch %d, want a fenced generation", started.CardSessionID, started.CardSessionLeaseEpoch)
	}

	var owner string
	var epoch int64
	if err := testPool.QueryRow(ctx, `SELECT COALESCE(lease_owner, ''), lease_epoch FROM card_session WHERE id = $1`, started.CardSessionID).Scan(&owner, &epoch); err != nil {
		t.Fatalf("read PAT start lease: %v", err)
	}
	if owner != daemonID || epoch != started.CardSessionLeaseEpoch {
		t.Fatalf("PAT start lease = owner %q epoch %d, want owner %q epoch %d", owner, epoch, daemonID, started.CardSessionLeaseEpoch)
	}

	for _, endpoint := range []string{"heartbeat", "release"} {
		req := newRequestAsUser(testUserID, http.MethodPost, "/api/daemon/tasks/"+taskID+"/card-session/"+endpoint, map[string]any{
			"lease_epoch": started.CardSessionLeaseEpoch,
		})
		req.Header.Set("X-Workspace-ID", workspaceID)
		req = withURLParam(req, "taskId", taskID)
		response := httptest.NewRecorder()
		if endpoint == "heartbeat" {
			testHandler.HeartbeatCardSessionLease(response, req)
		} else {
			testHandler.ReleaseCardSessionLease(response, req)
		}
		if response.Code != http.StatusOK {
			t.Fatalf("PAT %s status = %d: %s", endpoint, response.Code, response.Body.String())
		}
	}

	if err := testPool.QueryRow(ctx, `SELECT COALESCE(lease_owner, '') FROM card_session WHERE id = $1`, started.CardSessionID).Scan(&owner); err != nil {
		t.Fatalf("read released PAT lease: %v", err)
	}
	if owner != "" {
		t.Fatalf("released PAT lease owner = %q, want empty", owner)
	}
}

func TestStartTaskResponseKeepsTransactionalLeaseProofAcrossCancellation(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	workspaceID, runtimeID, agentID, _, issueID := createCardSessionCapacityFixture(t, ctx)
	generation := time.Now().UTC().Truncate(time.Microsecond)
	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
		VALUES ($1, $2, $3, 'dispatched', 0, $4)
		RETURNING id`, agentID, runtimeID, issueID, generation).Scan(&taskID); err != nil {
		t.Fatalf("insert dispatched task: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID)
	})

	previousStarter := testHandler.TaskService.TxStarter
	t.Cleanup(func() { testHandler.TaskService.TxStarter = previousStarter })
	testHandler.TaskService.TxStarter = &startAfterCommitTxStarter{
		pool: testPool,
		afterCommit: func() {
			if _, err := testPool.Exec(ctx, `
				UPDATE card_session
				SET state = 'paused', pause_reason = 'cancelled'
				WHERE issue_id = $1`, issueID); err != nil {
				t.Fatalf("cancel card session in commit barrier: %v", err)
			}
		},
	}

	startReq := newRequestAsUser(testUserID, http.MethodPost, "/api/daemon/tasks/"+taskID+"/start", map[string]any{
		"runtime_id":    runtimeID,
		"dispatched_at": generation.Format(time.RFC3339Nano),
		"capabilities":  []string{protocol.DaemonCapabilityCardSessionLeaseV1},
	})
	startReq.Header.Set("X-Workspace-ID", workspaceID)
	startReq = withURLParam(startReq, "taskId", taskID)
	startResp := httptest.NewRecorder()
	testHandler.StartTask(startResp, startReq)
	if startResp.Code != http.StatusOK {
		t.Fatalf("start status = %d: %s", startResp.Code, startResp.Body.String())
	}
	var started AgentTaskResponse
	if err := json.Unmarshal(startResp.Body.Bytes(), &started); err != nil {
		t.Fatalf("decode start response: %v", err)
	}
	if started.CardSessionLeaseEpoch <= 0 || started.CardSessionID == "" {
		t.Fatalf("start response lease = id %q epoch %d, want transactional proof", started.CardSessionID, started.CardSessionLeaseEpoch)
	}

	var currentOwner string
	var currentEpoch int64
	if err := testPool.QueryRow(ctx, `SELECT COALESCE(lease_owner, ''), lease_epoch FROM card_session WHERE id = $1`, started.CardSessionID).Scan(&currentOwner, &currentEpoch); err != nil {
		t.Fatalf("read cancelled card session: %v", err)
	}
	if currentOwner != "" || currentEpoch <= started.CardSessionLeaseEpoch {
		t.Fatalf("cancelled card session = owner %q epoch %d, want cleared owner and epoch > response %d", currentOwner, currentEpoch, started.CardSessionLeaseEpoch)
	}
	if _, _, err := testHandler.TaskService.CompleteTaskWithTransitionFenced(
		ctx, parseUUID(taskID), []byte(`{"output":"stale"}`), "", "", "", false, "", "", "start-claim-test", started.CardSessionLeaseEpoch,
	); !errors.Is(err, service.ErrCardSessionLeaseUnavailable) {
		t.Fatalf("terminal callback with pre-cancel proof error = %v, want ErrCardSessionLeaseUnavailable", err)
	}
}
