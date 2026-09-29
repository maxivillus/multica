package execenv

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareWriteFailuresDoNotLogEnvRoot(t *testing.T) {
	workspacesRoot := t.TempDir()
	rootParams := RootDirParams{
		WorkspacesRoot: workspacesRoot,
		WorkspaceID:    "ws-private-path-log",
		TaskID:         "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
	}
	claim, err := ClaimEnvRoot(rootParams)
	if err != nil {
		t.Fatalf("ClaimEnvRoot: %v", err)
	}
	defer claim.Release()

	// A directory at each destination makes the real non-fatal WriteFile routes
	// fail with os.PathError values containing the private env root.
	for _, name := range []string{managedEnvProvenanceFile, sidecarManifestFile} {
		if err := os.Mkdir(filepath.Join(claim.RootDir(), name), 0o700); err != nil {
			t.Fatalf("seed blocked write destination %s: %v", name, err)
		}
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	env, err := Prepare(PrepareParams{
		WorkspacesRoot:    rootParams.WorkspacesRoot,
		WorkspaceID:       rootParams.WorkspaceID,
		TaskID:            rootParams.TaskID,
		EnvRootPreclaimed: true,
		Task: TaskContextForEnv{
			IssueID: "issue-private-path-log",
			AgentID: "agent-private-path-log",
		},
	}, logger)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	env.Cleanup(true)

	output := logs.String()
	if strings.Contains(output, claim.RootDir()) {
		t.Fatalf("filesystem path leaked into Prepare logs: %s", output)
	}
	if count := strings.Count(output, "error_present=true"); count != 2 {
		t.Fatalf("expected safe error indicators for provenance and sidecar manifest, got %d in:\n%s", count, output)
	}
	if count := strings.Count(output, "error_type=*fs.PathError"); count != 2 {
		t.Fatalf("expected filesystem error types without their paths, got %d in:\n%s", count, output)
	}
}

func TestReasonixPermissionWarningsDoNotLogPrivatePaths(t *testing.T) {
	t.Run("user config read failure", func(t *testing.T) {
		privateDir := filepath.Join(t.TempDir(), "APPSEC_PRIVATE_REASONIX_CONFIG_MARKER")
		if err := os.MkdirAll(privateDir, 0o700); err != nil {
			t.Fatalf("mkdir private config dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(privateDir, reasonixUserConfigFile), []byte("[permissions\n"), 0o600); err != nil {
			t.Fatalf("seed malformed user config: %v", err)
		}
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		if err := writeReasonixProjectConfig(t.TempDir(), map[string]string{"REASONIX_HOME": privateDir}, &sidecarManifest{}, logger); err != nil {
			t.Fatalf("writeReasonixProjectConfig: %v", err)
		}

		output := logs.String()
		if strings.Contains(output, privateDir) || strings.Contains(output, "APPSEC_PRIVATE_REASONIX_CONFIG_MARKER") {
			t.Fatalf("Reasonix user config path leaked into logs: %s", output)
		}
		for _, field := range []string{"user_config_present=true", "error_present=true", "error_type=*fmt.wrapError"} {
			if !strings.Contains(output, field) {
				t.Errorf("safe Reasonix diagnostic missing %q: %s", field, output)
			}
		}
	})

	t.Run("existing project config", func(t *testing.T) {
		workDir := filepath.Join(t.TempDir(), "APPSEC_PRIVATE_REASONIX_WORKDIR_MARKER")
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			t.Fatalf("mkdir workdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(workDir, reasonixProjectConfigFile), []byte("owner content"), 0o600); err != nil {
			t.Fatalf("seed project config: %v", err)
		}
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		if err := writeReasonixProjectConfig(workDir, nil, &sidecarManifest{}, logger); err != nil {
			t.Fatalf("writeReasonixProjectConfig: %v", err)
		}

		output := logs.String()
		if strings.Contains(output, workDir) || strings.Contains(output, "APPSEC_PRIVATE_REASONIX_WORKDIR_MARKER") {
			t.Fatalf("Reasonix project config path leaked into logs: %s", output)
		}
		if !strings.Contains(output, "path_present=true") {
			t.Fatalf("safe path indicator missing from warning: %s", output)
		}
	})
}
