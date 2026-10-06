package agent

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCodexCredentialProxyRotatesAndClearsTaskToken(t *testing.T) {
	var seen []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	proxy, err := newCodexCredentialProxy(upstream.URL, "", "mat_task_a")
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	defer proxy.close()

	request := func(token string) int {
		req, _ := http.NewRequest(http.MethodGet, proxy.url+"/api/me", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("proxy request: %v", err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	if got := request(proxy.broker); got != http.StatusNoContent {
		t.Fatalf("active proxy status = %d", got)
	}
	proxy.clearToken()
	if got := request(proxy.broker); got != http.StatusUnauthorized {
		t.Fatalf("idle proxy status = %d, want 401", got)
	}
	if err := proxy.setToken("mat_task_b"); err != nil {
		t.Fatalf("rotate token: %v", err)
	}
	if got := request(proxy.broker); got != http.StatusNoContent {
		t.Fatalf("rotated proxy status = %d", got)
	}
	if got := request("mat_wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong broker status = %d, want 401", got)
	}
	if len(seen) != 2 || seen[0] != "Bearer mat_task_a" || seen[1] != "Bearer mat_task_b" {
		t.Fatalf("upstream authorization = %v", seen)
	}
}

func TestCodexPersistentProcessReceivesOnlyBrokerCredential(t *testing.T) {
	if testing.Short() {
		t.Skip("process fixture is not useful in short mode")
	}
	tokenFile := t.TempDir() + "/token"
	fakePath := writeFakeCodexAppServer(t, `printf '%s' "$MULTICA_TOKEN" > "`+tokenFile+`"
turn=0
while IFS= read -r line; do
  id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  method=$(printf '%s\n' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
  case "$method" in
    initialize) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    thread/start) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-broker"}}}\n' "$id" ;;
    turn/start)
      turn=$((turn+1))
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      printf '{"jsonrpc":"2.0","method":"turn/started","params":{"threadId":"thread-broker","turn":{"id":"turn-%s"}}}\n' "$turn"
      printf '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-broker","turn":{"id":"turn-%s","status":"completed"}}}\n' "$turn"
      ;;
  esac
done
`)
	backend, err := New("codex", Config{
		ExecutablePath: fakePath,
		Env: map[string]string{
			"MULTICA_TOKEN":      "mat_task_a",
			"MULTICA_SERVER_URL": "http://127.0.0.1:1",
		},
	})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	host, err := backend.(PersistentBackend).OpenPersistent(t.Context(), ExecOptions{
		TaskAuthToken:          "mat_task_a",
		HandshakeTimeout:       5 * time.Second,
		ThreadHandshakeTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open host: %v", err)
	}
	persistent := host.(*codexPersistentSession)
	defer persistent.Close()
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatalf("read process token: %v", err)
	}
	if string(raw) == "mat_task_a" || !strings.HasPrefix(string(raw), "mat_host_") {
		t.Fatalf("provider received %q; want opaque mat_host_ broker token", raw)
	}
	session, err := host.Execute(t.Context(), "prompt", ExecOptions{TaskAuthToken: "mat_task_b", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	for range session.Messages {
	}
	if result := <-session.Result; result.Status != "completed" {
		t.Fatalf("result = %+v", result)
	}
	persistent.proxy.mu.RLock()
	current := persistent.proxy.currentToken
	persistent.proxy.mu.RUnlock()
	if current != "" {
		t.Fatalf("credential remained after turn: %q", current)
	}
}

func TestCodexCredentialProxyRevocationCancelsInFlightRequest(t *testing.T) {
	requestStarted := make(chan struct{})
	requestCancelled := make(chan struct{})
	allowSideEffect := make(chan struct{})
	sideEffect := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-r.Context().Done():
			close(requestCancelled)
		case <-allowSideEffect:
			close(sideEffect)
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer upstream.Close()

	proxy, err := newCodexCredentialProxy(upstream.URL, "", "")
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	defer proxy.close()
	epoch, err := proxy.setTokenForTurn("mat_task_a")
	if err != nil {
		t.Fatalf("set token: %v", err)
	}

	response := make(chan struct {
		status int
		err    error
	}, 1)
	go func() {
		req, requestErr := http.NewRequest(http.MethodGet, proxy.url+"/api/me", nil)
		if requestErr != nil {
			response <- struct {
				status int
				err    error
			}{err: requestErr}
			return
		}
		req.Header.Set("Authorization", "Bearer "+proxy.broker)
		resp, requestErr := http.DefaultClient.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		response <- struct {
			status int
			err    error
		}{status: status, err: requestErr}
	}()

	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request did not start")
	}
	proxy.clearTokenForEpoch(epoch)
	select {
	case <-requestCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("revocation did not cancel the in-flight upstream request")
	}
	close(allowSideEffect)
	result := <-response
	if result.err != nil {
		t.Fatalf("proxy request after revocation: %v", result.err)
	}
	select {
	case <-sideEffect:
		t.Fatal("revoked request reached the upstream side effect")
	default:
	}
}
