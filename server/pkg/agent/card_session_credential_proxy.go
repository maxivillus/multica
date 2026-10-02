package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// codexCredentialProxy keeps the provider process free of a task credential.
// The process receives an opaque local broker token and points both the
// Multica API client and the repo helper at this listener. The daemon updates
// currentToken for exactly one active task turn; while idle, requests fail
// closed with 401. The upstream task token therefore remains only in daemon
// memory and is never inherited by the long-lived provider process.
type codexCredentialProxy struct {
	server   *http.Server
	listener net.Listener
	url      string
	port     string
	broker   string

	upstream *url.URL
	daemon   *url.URL

	mu           sync.RWMutex
	currentToken string
}

func newCodexCredentialProxy(upstreamURL, daemonPort, initialToken string) (*codexCredentialProxy, error) {
	var upstream *url.URL
	var err error
	if strings.TrimSpace(upstreamURL) != "" {
		upstream, err = url.Parse(strings.TrimRight(strings.TrimSpace(upstreamURL), "/"))
		if err != nil || upstream.Scheme == "" || upstream.Host == "" {
			return nil, fmt.Errorf("invalid Multica server URL for persistent session")
		}
	}
	var daemon *url.URL
	if port := strings.TrimSpace(daemonPort); port != "" {
		if _, err := strconv.Atoi(port); err != nil {
			return nil, fmt.Errorf("invalid daemon port for persistent session")
		}
		daemon, _ = url.Parse("http://127.0.0.1:" + port)
	}
	broker, err := newCodexBrokerToken()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen persistent credential broker: %w", err)
	}
	p := &codexCredentialProxy{
		listener:     listener,
		url:          "http://" + listener.Addr().String(),
		port:         strconv.Itoa(listener.Addr().(*net.TCPAddr).Port),
		broker:       broker,
		upstream:     upstream,
		daemon:       daemon,
		currentToken: strings.TrimSpace(initialToken),
	}
	p.server = &http.Server{Handler: http.HandlerFunc(p.serveHTTP)}
	go func() { _ = p.server.Serve(listener) }()
	return p, nil
}

func newCodexBrokerToken() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate persistent credential broker token: %w", err)
	}
	// Keep the mat_ prefix so the daemon-managed CLI still fails closed into
	// task-token mode. The value is meaningful only to this loopback broker.
	return "mat_host_" + hex.EncodeToString(raw[:]), nil
}

func (p *codexCredentialProxy) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+p.broker {
		http.Error(w, "persistent credential broker unauthorized", http.StatusUnauthorized)
		return
	}
	p.mu.RLock()
	token := p.currentToken
	p.mu.RUnlock()
	if token == "" {
		http.Error(w, "persistent credential broker idle", http.StatusUnauthorized)
		return
	}

	target := p.upstream
	if isCodexLocalDaemonPath(r.URL.Path) {
		target = p.daemon
	}
	if target == nil {
		http.Error(w, "persistent credential broker route unavailable", http.StatusServiceUnavailable)
		return
	}
	forward := r.Clone(r.Context())
	forward.URL.Scheme = target.Scheme
	forward.URL.Host = target.Host
	forward.URL.Path = joinProxyPath(target.Path, r.URL.Path)
	forward.URL.RawPath = ""
	forward.Host = target.Host
	// Requests received by net/http servers carry RequestURI; transports reject
	// that field on outbound client requests.
	forward.RequestURI = ""
	forward.Header.Set("Authorization", "Bearer "+token)
	// Never forward the broker credential or the listener's Host header to the
	// upstream. The current task token is the only authorization value allowed.
	resp, err := http.DefaultTransport.RoundTrip(forward)
	if err != nil {
		http.Error(w, "persistent credential broker upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func isCodexLocalDaemonPath(path string) bool {
	return path == "/health" || path == "/repo/checkout" || strings.HasPrefix(path, "/repo/checkout/")
}

func joinProxyPath(base, path string) string {
	base = strings.TrimRight(base, "/")
	if path == "" {
		return base + "/"
	}
	return base + "/" + strings.TrimLeft(path, "/")
}

func (p *codexCredentialProxy) setToken(token string) error {
	if p == nil {
		return nil
	}
	token = strings.TrimSpace(token)
	if token == "" || !strings.HasPrefix(token, "mat_") {
		return fmt.Errorf("persistent card session requires a task-scoped mat_ credential")
	}
	p.mu.Lock()
	p.currentToken = token
	p.mu.Unlock()
	return nil
}

func (p *codexCredentialProxy) clearToken() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.currentToken = ""
	p.mu.Unlock()
}

func (p *codexCredentialProxy) close() error {
	if p == nil {
		return nil
	}
	p.clearToken()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.server.Shutdown(ctx)
}
