package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// codexPersistentSession owns one app-server process and one provider thread.
// Codex's app-server protocol is explicitly multiplexed by threadId, so a
// later turn can use the same process and thread without replaying or
// reconstructing the previous prompt. The daemon serializes calls to Execute
// at the card-session host boundary; this type still checks the invariant so a
// future caller cannot accidentally interleave two turns on one thread.
type codexPersistentSession struct {
	cfg        Config
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stderrBuf  *stderrTail
	stop       context.CancelFunc
	proxy      *codexCredentialProxy
	client     *codexClient
	threadID   string
	readerDone chan struct{}
	waitDone   chan struct{}

	mu      sync.Mutex
	current *codexPersistentTurn
	closed  bool

	// beforeResultPublish is test-only synchronization for the terminal handoff
	// invariant: current must be cleared before a consumer can observe Result.
	// It remains nil in production.
	beforeResultPublish func()

	closeOnce sync.Once
	closeErr  error
}

type codexPersistentTurn struct {
	messages  chan Message
	result    chan Result
	turnDone  chan bool
	gate      *codexTurnNotificationGate
	startedAt time.Time

	mu           sync.Mutex
	finalAnswer  string
	lastMessage  string
	terminalSeen atomic.Bool
}

func (b *codexBackend) OpenPersistent(ctx context.Context, opts ExecOptions) (PersistentSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "codex"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("codex executable not found at %q: %w", execPath, err)
	}

	codexHome := strings.TrimSpace(b.cfg.Env["CODEX_HOME"])
	if codexHome != "" {
		if err := ensureCodexMcpConfig(filepath.Join(codexHome, "config.toml"), opts.McpConfig, b.cfg.Logger); err != nil {
			return nil, fmt.Errorf("apply codex mcp_config: %w", err)
		}
	} else if hasManagedCodexMcpConfig(opts.McpConfig) {
		return nil, fmt.Errorf("codex: mcp_config is set but CODEX_HOME env var is not configured; cannot apply managed MCP")
	}

	runtimeCmd := b.cfg.commandAt(execPath)
	if codexHome != "" {
		opts.ExtraArgs = filterCodexShellEnvConfigOverrides(opts.ExtraArgs, b.cfg.Logger)
		opts.CustomArgs = filterCodexShellEnvConfigOverrides(opts.CustomArgs, b.cfg.Logger)
		runtimeCmd = runtimeCmd.withFilteredPrefix(func(prefix []string) []string {
			return filterCodexShellEnvConfigOverrides(prefix, b.cfg.Logger)
		})
	}
	if hasManagedCodexMcpConfig(opts.McpConfig) {
		runtimeCmd = runtimeCmd.withFilteredPrefix(func(prefix []string) []string {
			return filterCodexCustomConfigOverrides(prefix, b.cfg.Logger)
		})
	}
	if opts.ServiceTier == codexFastServiceTier {
		runtimeCmd = runtimeCmd.withFilteredPrefix(func(prefix []string) []string {
			return stripCodexFastModeConflicts(prefix, b.cfg.Logger)
		})
	}
	var proxy *codexCredentialProxy
	var err error
	if strings.TrimSpace(opts.TaskAuthToken) != "" || strings.TrimSpace(b.cfg.Env["MULTICA_TOKEN"]) != "" {
		proxy, err = newCodexCredentialProxy(b.cfg.Env["MULTICA_SERVER_URL"], b.cfg.Env["MULTICA_DAEMON_PORT"], opts.TaskAuthToken)
		if err != nil {
			return nil, err
		}
	}
	proxyOwned := proxy != nil
	defer func() {
		if proxyOwned {
			_ = proxy.close()
		}
	}()

	handshakeTimeout, threadHandshakeTimeout := resolveCodexHandshakeTimeouts(opts)
	processCtx, stop := context.WithCancel(ctx)
	cmd := runtimeCmd.exec(processCtx, buildCodexArgs(opts, b.cfg.Logger)...)
	hideAgentWindow(cmd)
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			signalProcessGroup(cmd, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = codexProcessWaitDelay()
	b.cfg.logAgentCommand(cmd, newAgentCommandLogArgs(buildCodexArgs(opts, b.cfg.Logger), trustAgentCommandPositional(0, "app-server")))
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	processEnv := make(map[string]string, len(b.cfg.Env)+3)
	for key, value := range b.cfg.Env {
		processEnv[key] = value
	}
	if proxy != nil {
		processEnv["MULTICA_TOKEN"] = proxy.broker
		processEnv["MULTICA_SERVER_URL"] = proxy.url
		processEnv["MULTICA_DAEMON_PORT"] = proxy.port
	}
	cmd.Env = buildEnv(processEnv)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stop()
		return nil, fmt.Errorf("codex persistent stdout pipe: %w", err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		stop()
		return nil, fmt.Errorf("codex persistent stdin pipe: %w", err)
	}
	stderrBuf := newStderrTail(io.Discard, codexStderrTailBytes)
	cmd.Stderr = stderrBuf
	if err := startOwnedProcessTree(cmd, b.cfg.Logger); err != nil {
		stop()
		return nil, fmt.Errorf("start persistent codex: %w", err)
	}

	p := &codexPersistentSession{
		cfg:        b.cfg,
		cmd:        cmd,
		stdin:      stdin,
		stderrBuf:  stderrBuf,
		stop:       stop,
		proxy:      proxy,
		readerDone: make(chan struct{}),
		waitDone:   make(chan struct{}),
	}
	// The provider process has the broker environment above, while the host
	// object must not retain the first task's raw token after setup either.
	if p.cfg.Env != nil {
		p.cfg.Env = cloneCodexPersistentEnv(p.cfg.Env)
		delete(p.cfg.Env, "MULTICA_TOKEN")
	}
	p.client = p.newClient(handshakeTimeout, threadHandshakeTimeout, cmd.Process.Pid)
	go p.readLoop(stdout)
	go p.waitLoop()

	setupCtx := ctx
	if err := p.initializeAndStartThread(setupCtx, opts); err != nil {
		_ = p.Close()
		return nil, err
	}
	if proxy != nil {
		proxy.clearToken()
	}
	proxyOwned = false
	p.cfg.Logger.Info("codex persistent card-session host ready",
		"pid", cmd.Process.Pid,
		"thread_id_present", p.threadID != "",
	)
	return p, nil
}

func cloneCodexPersistentEnv(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	copyEnv := make(map[string]string, len(source))
	for key, value := range source {
		copyEnv[key] = value
	}
	return copyEnv
}

func (p *codexPersistentSession) newClient(handshakeTimeout, threadHandshakeTimeout time.Duration, pid int) *codexClient {
	if p.cfg.Logger == nil {
		p.cfg.Logger = slog.Default()
	}
	c := &codexClient{
		cfg:                    p.cfg,
		stdin:                  p.stdin,
		pending:                make(map[int]*pendingRPC),
		processDone:            make(chan struct{}),
		handshakeTimeout:       handshakeTimeout,
		threadHandshakeTimeout: threadHandshakeTimeout,
		pid:                    pid,
		notificationProtocol:   "unknown",
		acceptNotification: func(method string, params map[string]any) bool {
			p.mu.Lock()
			turn := p.current
			p.mu.Unlock()
			return turn != nil && turn.gate.accept(method, params)
		},
		onMessage: func(msg Message) {
			p.withTurn(func(turn *codexPersistentTurn) {
				msg.SessionID = p.threadID
				logCodexAgentMessage(p.cfg.Logger, msg)
				trySend(turn.messages, msg)
			})
		},
		onAgentMessageChunk: func(text string) bool {
			p.mu.Lock()
			turn := p.current
			p.mu.Unlock()
			if turn == nil {
				return false
			}
			return trySend(turn.messages, Message{Type: MessageText, Content: text, SessionID: p.threadID})
		},
		onAgentMessage: func(text string) {
			p.withTurn(func(turn *codexPersistentTurn) {
				turn.mu.Lock()
				turn.lastMessage = text
				turn.mu.Unlock()
			})
		},
		onFinalAnswer: func(text string) {
			p.withTurn(func(turn *codexPersistentTurn) {
				turn.mu.Lock()
				turn.finalAnswer = text
				turn.mu.Unlock()
			})
		},
		onSemanticActivity: func(description string) {
			// Semantic activity is already represented by status, text, and tool
			// messages. Keep this callback installed so the client continues to
			// recognize provider progress without exposing an extra transcript row.
			if description != "" {
				p.cfg.Logger.Debug("codex persistent semantic activity", "activity", description)
			}
		},
		onTurnDone: func(aborted bool) {
			p.withTurn(func(turn *codexPersistentTurn) {
				select {
				case turn.turnDone <- aborted:
				default:
				}
			})
		},
		onDiscardedNotification: func(string, map[string]any) {},
	}
	return c
}

func (p *codexPersistentSession) withTurn(fn func(*codexPersistentTurn)) {
	p.mu.Lock()
	turn := p.current
	p.mu.Unlock()
	if turn != nil {
		fn(turn)
	}
}

func (p *codexPersistentSession) readLoop(stdout io.Reader) {
	defer close(p.readerDone)
	scanner := newAgentStreamScanner(stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			p.client.handleLine(line)
		}
	}
	p.client.flushAgentMessageDeltas()
	if err := scanner.Err(); err != nil {
		p.client.markProcessExited(fmt.Errorf("%w: %w", errCodexProcessExited, err))
	} else {
		p.client.markProcessExited(errCodexProcessExited)
	}
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

func (p *codexPersistentSession) waitLoop() {
	err := p.cmd.Wait()
	if err != nil {
		p.client.markProcessExited(fmt.Errorf("codex persistent process exited: %w", err))
	}
	close(p.waitDone)
}

func (p *codexPersistentSession) initializeAndStartThread(ctx context.Context, opts ExecOptions) error {
	if _, err := p.client.request(ctx, "initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "multica-agent-sdk",
			"title":   "Multica Agent SDK",
			"version": "0.2.0",
		},
		"capabilities": map[string]any{"experimentalApi": true},
	}); err != nil {
		return fmt.Errorf("codex persistent initialize failed: %w", err)
	}
	p.client.notify("initialized")
	threadID, _, err := p.client.startOrResumeThread(ctx, opts, p.cfg.Logger)
	if err != nil {
		return fmt.Errorf("codex persistent thread setup failed: %w", err)
	}
	p.threadID = threadID
	p.client.setThreadID(threadID)
	return nil
}

func (p *codexPersistentSession) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("codex persistent session is closed")
	}
	if p.current != nil {
		p.mu.Unlock()
		return nil, errors.New("codex persistent session is already executing a turn")
	}
	turn := &codexPersistentTurn{
		messages:  make(chan Message, 256),
		result:    make(chan Result, 1),
		turnDone:  make(chan bool, 1),
		gate:      &codexTurnNotificationGate{},
		startedAt: time.Now(),
	}
	p.current = turn
	p.mu.Unlock()
	if p.proxy != nil {
		if err := p.proxy.setToken(opts.TaskAuthToken); err != nil {
			p.mu.Lock()
			p.current = nil
			p.mu.Unlock()
			return nil, err
		}
	}

	p.resetClientForTurn()
	go p.runTurn(ctx, prompt, opts, turn)
	return &Session{
		Messages: turn.messages,
		Result:   turn.result,
		TerminalObserved: func() bool {
			return turn.terminalSeen.Load()
		},
	}, nil
}

func (p *codexPersistentSession) resetClientForTurn() {
	c := p.client
	c.turnErrorMu.Lock()
	c.turnError = ""
	c.turnErrorMu.Unlock()
	c.turnIDMu.Lock()
	c.turnID = ""
	c.turnIDMu.Unlock()
	c.usageMu.Lock()
	c.usage = TokenUsage{}
	c.usageTotal = codexRawTokenUsage{}
	c.usageTotalSet = false
	c.usageMu.Unlock()
	c.turnCompleted = false
	c.agentMessageStreams = make(map[string]*codexAgentMessageStream)
	c.agentMessageOrder = nil
}

func (p *codexPersistentSession) runTurn(ctx context.Context, prompt string, opts ExecOptions, turn *codexPersistentTurn) {
	defer func() {
		if p.proxy != nil {
			p.proxy.clearToken()
		}
	}()
	start := time.Now()
	status := "completed"
	var finalErr string
	turn.gate.arm()
	turnParams := map[string]any{
		"threadId": p.threadID,
		"input":    codexTurnInput(prompt, false, true, ""),
	}
	applyCodexReasoningEffort(turnParams, opts.ThinkingLevel)
	applyCodexServiceTier(turnParams, opts.ServiceTier)
	runCtx, cancel := runContext(ctx, opts.Timeout)
	defer cancel()

	_, err := p.client.request(runCtx, "turn/start", turnParams)
	if err != nil {
		status = "failed"
		finalErr = err.Error()
		select {
		case aborted := <-turn.turnDone:
			status = "completed"
			if aborted {
				status = "aborted"
				finalErr = "turn was aborted"
			} else if turnErr := p.client.getTurnError(); turnErr != "" {
				status = "failed"
				finalErr = turnErr
			} else {
				finalErr = ""
			}
		default:
			aborted, completed := interruptCodexTurnWithOutcome(p.client, p.threadID, turn.turnDone, opts.TurnInterruptTimeout, p.cfg.Logger)
			if !completed {
				_ = p.Close()
			} else if aborted {
				status = "aborted"
				finalErr = "turn was aborted"
			} else if turnErr := p.client.getTurnError(); turnErr != "" {
				status = "failed"
				finalErr = turnErr
			} else {
				status = "completed"
				finalErr = ""
			}
		}
	} else {
		semanticTimeout := opts.SemanticInactivityTimeout
		if semanticTimeout <= 0 {
			semanticTimeout = defaultCodexSemanticInactivityTimeout
		}
		timer := time.NewTimer(semanticTimeout)
		select {
		case aborted := <-turn.turnDone:
			if aborted {
				status = "aborted"
				finalErr = "turn was aborted"
			} else if errMsg := p.client.getTurnError(); errMsg != "" {
				status = "failed"
				finalErr = errMsg
			}
		case <-timer.C:
			status = "timeout"
			finalErr = fmt.Sprintf("codex semantic inactivity timeout after %s", semanticTimeout)
			if !interruptCodexTurn(p.client, p.threadID, turn.turnDone, opts.TurnInterruptTimeout, p.cfg.Logger) {
				_ = p.Close()
			}
		case <-runCtx.Done():
			if runCtx.Err() == context.DeadlineExceeded {
				status = "timeout"
				finalErr = fmt.Sprintf("codex timed out after %s", opts.Timeout)
			} else {
				status = "aborted"
				finalErr = "execution cancelled"
			}
			if !interruptCodexTurn(p.client, p.threadID, turn.turnDone, opts.TurnInterruptTimeout, p.cfg.Logger) {
				_ = p.Close()
			}
		case <-p.client.processDone:
			status = "failed"
			if p.client.getProcessErr() != nil {
				finalErr = p.client.getProcessErr().Error()
			} else {
				finalErr = errCodexProcessExited.Error()
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	turn.mu.Lock()
	output := codexDeliverableOutput(turn.finalAnswer, turn.lastMessage)
	turn.mu.Unlock()
	p.client.usageMu.Lock()
	usage := p.client.usage
	p.client.usageMu.Unlock()
	var usageMap map[string]TokenUsage
	if usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.CacheReadTokens > 0 || usage.CacheWriteTokens > 0 {
		model := opts.Model
		if model == "" {
			model = "unknown"
		}
		usageMap = map[string]TokenUsage{model: usage}
	}
	turn.terminalSeen.Store(true)
	// Revoke the turn credential before publishing the terminal result. The
	// daemon can observe Result as soon as it is sent; clearing only in the
	// function defer would leave a small window where an idle host still held
	// the just-finished task token.
	if p.proxy != nil {
		p.proxy.clearToken()
	}
	p.mu.Lock()
	if p.current == turn {
		p.current = nil
	}
	p.mu.Unlock()
	if p.beforeResultPublish != nil {
		p.beforeResultPublish()
	}
	turn.result <- Result{
		Status:     status,
		Output:     output,
		Error:      finalErr,
		DurationMs: time.Since(start).Milliseconds(),
		SessionID:  p.threadID,
		Usage:      usageMap,
	}
	close(turn.messages)
	close(turn.result)
}

func (p *codexPersistentSession) ProcessID() int {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *codexPersistentSession) IsClosed() bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *codexPersistentSession) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.client.markProcessExited(errCodexProcessExited)
		if p.stdin != nil {
			_ = p.stdin.Close()
		}
		p.stop()
		grace := codexGracefulShutdown()
		select {
		case <-p.waitDone:
		case <-time.After(grace):
			signalProcessGroup(p.cmd, syscall.SIGKILL)
			select {
			case <-p.waitDone:
			case <-time.After(grace):
			}
		}
		select {
		case <-p.readerDone:
		case <-time.After(grace):
		}
		releaseProcessGroup(p.cmd)
		if p.proxy != nil {
			if err := p.proxy.close(); err != nil && p.closeErr == nil {
				p.closeErr = err
			}
		}
	})
	return p.closeErr
}
