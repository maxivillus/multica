//go:build unix

package agent

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProviderLogsDoNotExposeSessionIDsOrWorkdirs(t *testing.T) {
	tests := []struct {
		name             string
		provider         string
		sessionID        string
		safeFields       []string
		assertSessionID  bool
		resumeWithMarker bool
		new              func(t *testing.T, logger *slog.Logger, cwd string) Backend
	}{
		{
			name:            "traecli",
			provider:        "traecli",
			sessionID:       "ses_new",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "traecli")
				writeTestExecutable(t, path, []byte(fakeTraecliACPScript()))
				backend, err := New("traecli", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new traecli: %v", err)
				}
				return backend
			},
		},
		{
			name:            "reasonix",
			provider:        "reasonix",
			sessionID:       "reasonix-session",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "reasonix")
				writeTestExecutable(t, path, []byte(fakeReasonixACPScript()))
				backend, err := New("reasonix", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new reasonix: %v", err)
				}
				return backend
			},
		},
		{
			name:            "kimi",
			provider:        "kimi",
			sessionID:       "ses_fake",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "kimi")
				writeTestExecutable(t, path, []byte(fakeKimiACPScript()))
				backend, err := New("kimi", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new kimi: %v", err)
				}
				return backend
			},
		},
		{
			name:            "hermes",
			provider:        "hermes",
			sessionID:       "hermes-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "hermes")
				record := filepath.Join(t.TempDir(), "requests.jsonl")
				writeTestExecutable(t, path, []byte(fakeACPRecordingScript(record, "hermes-session-marker", "{}")))
				backend, err := New("hermes", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new hermes: %v", err)
				}
				return backend
			},
		},
		{
			name:            "qwen",
			provider:        "qwen",
			sessionID:       "sess-qwen-1",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "qwen")
				writeTestExecutable(t, path, []byte(fakeQwenScript()))
				return &qwenBackend{cfg: Config{ExecutablePath: path, Logger: logger}}
			},
		},
		{
			name:            "zeroclaw",
			provider:        "zeroclaw",
			sessionID:       "zero-private-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "zeroclaw")
				script := strings.ReplaceAll(fakeZeroclawACPScript(), "ses_zeroclaw_new", "zero-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("zeroclaw", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new zeroclaw: %v", err)
				}
				return backend
			},
		},
		{
			name:            "dim",
			provider:        "dim",
			sessionID:       "dim-private-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "dim")
				script := strings.ReplaceAll(fakeDimACPScript(), "ses_dim_new", "dim-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("dim", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new dim: %v", err)
				}
				return backend
			},
		},
		{
			name:            "kiro",
			provider:        "kiro",
			sessionID:       "kiro-private-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "kiro")
				script := strings.ReplaceAll(fakeKiroACPScript(), "ses_new", "kiro-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("kiro", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new kiro: %v", err)
				}
				return backend
			},
		},
		{
			name:            "grok",
			provider:        "grok",
			sessionID:       "grok-private-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "grok")
				script := strings.ReplaceAll(fakeGrokACPScript(), `"ses_new"`, `"grok-private-session-marker"`)
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("grok", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new grok: %v", err)
				}
				return backend
			},
		},
		{
			name:            "qoder",
			provider:        "qoder",
			sessionID:       "qoder-private-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "qoder")
				script := strings.ReplaceAll(fakeQoderACPScript(), `"ses_fake"`, `"qoder-private-session-marker"`)
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("qoder", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new qoder: %v", err)
				}
				return backend
			},
		},
		{
			name:            "qwenpaw",
			provider:        "qwenpaw",
			sessionID:       "qwenpaw-private-session-marker",
			safeFields:      []string{"cwd_present=true", "session_id_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := writeFakeQwenpawScript(t, strings.ReplaceAll(fakeQwenpawACPScript(), "ses_qwenpaw_new", "qwenpaw-private-session-marker"))
				backend, err := New("qwenpaw", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new qwenpaw: %v", err)
				}
				return backend
			},
		},
		{
			name:            "mcode",
			provider:        "mcode",
			sessionID:       "mcode-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path, _, _ := writeFakeMcodeACP(t, true)
				script, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read mcode fixture: %v", err)
				}
				if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(script), "mcode-session-new", "mcode-private-session-marker")), 0o755); err != nil {
					t.Fatalf("rewrite mcode fixture: %v", err)
				}
				backend, err := New("mcode", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new mcode: %v", err)
				}
				return backend
			},
		},
		{
			name:            "deveco",
			provider:        "deveco",
			sessionID:       "deveco-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "deveco")
				script := strings.ReplaceAll(fakeDevecoScript(), "ses_fake", "deveco-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("deveco", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new deveco: %v", err)
				}
				return backend
			},
		},
		{
			name:            "cursor",
			provider:        "cursor",
			sessionID:       "cursor-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "cursor-agent")
				script := `#!/bin/sh
cat > /dev/null
printf '%s\n' '{"type":"system","subtype":"init","session_id":"cursor-private-session-marker"}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"cursor-private-session-marker"}'
`
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("cursor", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new cursor: %v", err)
				}
				return backend
			},
		},
		{
			name:             "pi",
			provider:         "pi",
			sessionID:        "pi-private-session-marker",
			safeFields:       []string{"cwd_present=true"},
			resumeWithMarker: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "pi")
				script := piEventStreamScript([]string{
					`{"type":"agent_start"}`,
					`{"type":"turn_start"}`,
					`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"done"}}`,
					`{"type":"turn_end","message":{"role":"assistant","model":"test","stopReason":"stop"}}`,
				})
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("pi", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new pi: %v", err)
				}
				return backend
			},
		},
		{
			name:            "dsh",
			provider:        "dsh",
			sessionID:       "dsh-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := writeDshFixture(t, `
printf '%s\n' '{"v":1,"type":"ready","runtime":"dsh","plugin_version":"test","capabilities":{}}'
IFS= read -r command
printf '%s\n' '{"v":1,"type":"session","request_id":"privacy-task","session_id":"dsh-private-session-marker","resumed":false}'
printf '%s\n' '{"v":1,"type":"text","request_id":"privacy-task","content":"done"}'
printf '%s\n' '{"v":1,"type":"result","request_id":"privacy-task","status":"completed","session_id":"dsh-private-session-marker","output":"done","resume_rejected":false}'
`)
				backend, err := New("dsh", Config{ExecutablePath: path, Logger: logger, TaskID: "privacy-task"})
				if err != nil {
					t.Fatalf("new dsh: %v", err)
				}
				return backend
			},
		},
		{
			name:            "claude",
			provider:        "claude",
			sessionID:       "claude-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "claude")
				script := `#!/bin/sh
IFS= read -r _
printf '%s\n' '{"type":"system","subtype":"init","session_id":"claude-private-session-marker"}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"claude-private-session-marker"}'
`
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("claude", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new claude: %v", err)
				}
				return backend
			},
		},
		{
			name:            "codebuddy",
			provider:        "codebuddy",
			sessionID:       "codebuddy-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "codebuddy")
				script := `#!/bin/sh
IFS= read -r _
printf '%s\n' '{"type":"system","subtype":"init","session_id":"codebuddy-private-session-marker"}'
printf '%s\n' '{"type":"assistant","message":{"role":"assistant","model":"test","content":[{"type":"text","text":"done"}]}}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"session_id":"codebuddy-private-session-marker","result":"done"}'
`
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("codebuddy", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new codebuddy: %v", err)
				}
				return backend
			},
		},
		{
			name:            "copilot",
			provider:        "copilot",
			sessionID:       "copilot-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "copilot")
				script := `#!/bin/sh
printf '%s\n' '{"type":"result","sessionId":"copilot-private-session-marker","exitCode":0,"usage":{"premiumRequests":1}}'
`
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("copilot", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new copilot: %v", err)
				}
				return backend
			},
		},
		{
			name:            "opencode",
			provider:        "opencode",
			sessionID:       "opencode-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "opencode")
				script := strings.ReplaceAll(fakeOpencodeScript(), "ses_fake", "opencode-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("opencode", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new opencode: %v", err)
				}
				return backend
			},
		},
		{
			name:            "codearts",
			provider:        "codearts",
			sessionID:       "codearts-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "codearts")
				script := strings.ReplaceAll(fakeOpencodeScript(), "ses_fake", "codearts-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("codearts", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new codearts: %v", err)
				}
				return backend
			},
		},
		{
			name:            "openclaw",
			provider:        "openclaw",
			sessionID:       "openclaw-private-session-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				body := `{"payloads":[{"text":"done"}],"meta":{"durationMs":10,"agentMeta":{"sessionId":"openclaw-private-session-marker","model":"test"}}}`
				path := writeOpenclawStub(t, body, false)
				backend, err := New("openclaw", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new openclaw: %v", err)
				}
				return backend
			},
		},
		{
			name:       "antigravity",
			provider:   "antigravity",
			sessionID:  "agy-private-session-marker",
			safeFields: []string{"cwd_present=true"},
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				path := filepath.Join(t.TempDir(), "agy")
				script := strings.ReplaceAll(fakeAgyStreamJSONScript(), "07a6d8f2-8523-46fc-8fc9-87e630cbe295", "agy-private-session-marker")
				writeTestExecutable(t, path, []byte(script))
				backend, err := New("antigravity", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new antigravity: %v", err)
				}
				return backend
			},
		},
		{
			name:            "codex",
			provider:        "codex",
			sessionID:       "codex-private-thread-marker",
			safeFields:      []string{"cwd_present=true"},
			assertSessionID: true,
			new: func(t *testing.T, logger *slog.Logger, cwd string) Backend {
				body := `read line
echo '{"jsonrpc":"2.0","id":1,"result":{}}'
read line
read line
echo '{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"codex-private-thread-marker"}}}'
read line
echo '{"jsonrpc":"2.0","id":3,"result":{}}'
echo '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"codex-private-thread-marker","turn":{"id":"codex-private-turn-marker","status":"completed"}}}'
`
				path := writeFakeCodexAppServer(t, body)
				backend, err := New("codex", Config{ExecutablePath: path, Logger: logger})
				if err != nil {
					t.Fatalf("new codex: %v", err)
				}
				return backend
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			cwd := filepath.Join(t.TempDir(), "private-workdir-marker")
			if err := os.MkdirAll(cwd, 0o755); err != nil {
				t.Fatalf("create cwd: %v", err)
			}
			backend := tc.new(t, logger, cwd)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			opts := ExecOptions{Cwd: cwd, Timeout: 5 * time.Second}
			if tc.resumeWithMarker {
				opts.ResumeSessionID = filepath.Join(cwd, tc.sessionID)
			}
			session, err := backend.Execute(ctx, "privacy marker test", opts)
			if err != nil {
				t.Fatalf("execute %s: %v", tc.provider, err)
			}
			var result Result
			messagesDone := make(chan struct{})
			go func() {
				defer close(messagesDone)
				for range session.Messages {
				}
			}()
			select {
			case result = <-session.Result:
			case <-ctx.Done():
				t.Fatalf("%s result timed out: %v", tc.provider, ctx.Err())
			}
			select {
			case <-messagesDone:
			case <-ctx.Done():
				t.Fatalf("%s messages did not drain: %v", tc.provider, ctx.Err())
			}

			if tc.assertSessionID && result.SessionID != tc.sessionID {
				t.Fatalf("%s returned session id %q, want marker %q", tc.provider, result.SessionID, tc.sessionID)
			}
			if got := logs.String(); (tc.sessionID != "" && strings.Contains(got, tc.sessionID)) || strings.Contains(got, cwd) {
				t.Fatalf("%s provider log contains sensitive marker: %s", tc.provider, got)
			}
			for _, field := range tc.safeFields {
				if !strings.Contains(logs.String(), field) {
					t.Fatalf("%s log omitted safe field %q: %s", tc.provider, field, logs.String())
				}
			}
		})
	}
}

func TestProviderResumeRejectionLogsOmitSessionIDs(t *testing.T) {
	tests := []struct {
		name         string
		provider     string
		script       string
		env          map[string]string
		marker       string
		wantRejected bool
	}{
		{
			name:         "zeroclaw",
			provider:     "zeroclaw",
			script:       strings.ReplaceAll(fakeZeroclawACPScript(), "ses_gone", "zero-resume-private-marker"),
			env:          map[string]string{"ZEROCLAW_SESSION_NOT_FOUND": "1"},
			marker:       "zero-resume-private-marker",
			wantRejected: true,
		},
		{
			name:         "dim",
			provider:     "dim",
			script:       strings.ReplaceAll(fakeDimACPScript(), "ses_prior", "dim-resume-private-marker"),
			env:          map[string]string{"DIM_LOAD_NOT_FOUND": "1"},
			marker:       "dim-resume-private-marker",
			wantRejected: true,
		},
		{
			name:     "claude",
			provider: "claude",
			script: `#!/bin/sh
IFS= read -r _
echo "No conversation found with session ID: claude-resume-private-marker" >&2
exit 1
`,
			marker:       "claude-resume-private-marker",
			wantRejected: true,
		},
		{
			name:     "codebuddy",
			provider: "codebuddy",
			script: `#!/bin/sh
IFS= read -r _
echo "No conversation found with session ID: codebuddy-resume-private-marker" >&2
exit 1
`,
			marker:       "codebuddy-resume-private-marker",
			wantRejected: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			cwd := filepath.Join(t.TempDir(), tc.name+"-resume-workdir-marker")
			if err := os.MkdirAll(cwd, 0o755); err != nil {
				t.Fatalf("create cwd: %v", err)
			}
			path := filepath.Join(t.TempDir(), tc.provider)
			writeTestExecutable(t, path, []byte(tc.script))
			backend, err := New(tc.provider, Config{ExecutablePath: path, Logger: logger, Env: tc.env})
			if err != nil {
				t.Fatalf("new %s: %v", tc.provider, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			session, err := backend.Execute(ctx, "resume privacy marker test", ExecOptions{
				Cwd: cwd, Timeout: 5 * time.Second, ResumeSessionID: tc.marker,
			})
			if err != nil {
				t.Fatalf("execute %s: %v", tc.provider, err)
			}
			for range session.Messages {
			}
			select {
			case result := <-session.Result:
				if result.ResumeRejected != tc.wantRejected {
					t.Fatalf("%s ResumeRejected = %v, want %v (status=%s, error=%q)", tc.provider, result.ResumeRejected, tc.wantRejected, result.Status, result.Error)
				}
			case <-ctx.Done():
				t.Fatalf("%s result timed out: %v", tc.provider, ctx.Err())
			}
			got := logs.String()
			for _, secret := range []string{tc.marker, cwd} {
				if strings.Contains(got, secret) {
					t.Fatalf("%s resume log contains sensitive value %q: %s", tc.provider, secret, got)
				}
			}
			if !strings.Contains(got, "requested_session_id_present=true") {
				t.Fatalf("%s resume log omitted safe session presence field: %s", tc.provider, got)
			}
		})
	}
}

func TestProviderStderrLogOmitsRawContents(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	marker := "provider-error session_id=stderr-private-session cwd=/private/stderr-workdir"

	if _, err := newLogWriter(logger, "[provider:stderr] ").Write([]byte(marker)); err != nil {
		t.Fatalf("write provider stderr: %v", err)
	}
	got := logs.String()
	for _, secret := range []string{"stderr-private-session", "/private/stderr-workdir", marker} {
		if strings.Contains(got, secret) {
			t.Fatalf("provider stderr log contains raw contents %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "bytes=") {
		t.Fatalf("provider stderr log omitted safe byte count: %s", got)
	}
}
