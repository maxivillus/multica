//go:build windows

package agent

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWindowsInvocationLogsOmitLauncherPaths(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	dir := filepath.Join(t.TempDir(), "windows-launch-private-path-marker")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create launcher directory: %v", err)
	}
	fakePS := filepath.Join(dir, "powershell-private-marker.exe")
	writeFile(t, fakePS, "")
	stubPowerShell(t, fakePS, true)

	for _, tc := range []struct {
		name string
		fn   func(string, []string, *slog.Logger) (string, []string, bool)
	}{
		{name: "cursor-agent", fn: platformCursorInvocation},
		{name: "pi", fn: platformPiInvocation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			launcher := filepath.Join(dir, tc.name+"-original-private-marker.cmd")
			writeFile(t, launcher, "@echo off\r\n")
			writeFile(t, filepath.Join(dir, tc.name+".ps1"), "# fixture\r\n")
			if _, _, ok := tc.fn(launcher, []string{"--probe"}, logger); !ok {
				t.Fatal("expected PowerShell invocation route")
			}
		})
	}

	copilotShim := filepath.Join(dir, "copilot-shim-private-marker.cmd")
	writeFile(t, copilotShim, "@echo off\r\n")
	pkg := "copilot-win32-x64"
	if runtime.GOARCH == "arm64" {
		pkg = "copilot-win32-arm64"
	}
	native := filepath.Join(dir, "node_modules", "@github", "copilot", "node_modules", "@github", pkg, "copilot-native-private-marker.exe")
	if err := os.MkdirAll(filepath.Dir(native), 0o700); err != nil {
		t.Fatalf("create native binary directory: %v", err)
	}
	writeFile(t, native, "")
	if _, _, ok := platformCopilotInvocation(copilotShim, []string{"--probe"}, logger); !ok {
		t.Fatal("expected bundled Copilot native invocation")
	}

	got := logs.String()
	for _, marker := range []string{"windows-launch-private-path-marker", "powershell-private-marker.exe", "original-private-marker.cmd", "copilot-native-private-marker.exe"} {
		if strings.Contains(got, marker) {
			t.Errorf("Windows invocation log exposed path marker %q: %s", marker, got)
		}
	}
	for _, safe := range []string{`"powershell_present":true`, `"script_present":true`, `"launcher_present":true`, `"native_present":true`} {
		if !strings.Contains(got, safe) {
			t.Errorf("Windows invocation log omitted safe marker %s: %s", safe, got)
		}
	}
}
