//go:build !windows

package server

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

const shutdownHelperEnv = "STASH_SHUTDOWN_HELPER"

// TestNotifyContextHelper is the child process for TestSecondSignalEndsAStuckShutdown.
// It cancels on the first signal and then never finishes shutting down.
func TestNotifyContextHelper(t *testing.T) {
	if os.Getenv(shutdownHelperEnv) != "1" {
		t.Skip("runs only as a child of TestSecondSignalEndsAStuckShutdown")
	}

	ctx, stop := NotifyContext(context.Background())
	defer stop()
	_, _ = os.Stdout.WriteString("ready\n")
	<-ctx.Done()
	_, _ = os.Stdout.WriteString("shutting down\n")
	select {} // a shutdown that never completes
}

// SIGTERM is used for both signals because a process started with SIGINT
// ignored (a background job of a non-interactive shell, for one) goes back to
// ignoring it once Go stops catching it, which would make the test depend on how
// it was launched. The handlers for the two signals are removed together.
func TestSecondSignalEndsAStuckShutdown(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestNotifyContextHelper$")
	cmd.Env = append(os.Environ(), shutdownHelperEnv+"=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe() error = %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	lines := bufio.NewScanner(stdout)
	expectLine := func(want string) {
		t.Helper()
		got := make(chan string, 1)
		go func() {
			if lines.Scan() {
				got <- lines.Text()
			} else {
				got <- ""
			}
		}()
		select {
		case line := <-got:
			if !strings.Contains(line, want) {
				t.Fatalf("child printed %q, want %q", line, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for the child to print %q", want)
		}
	}

	expectLine("ready")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("first signal error = %v", err)
	}
	expectLine("shutting down")

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("second signal error = %v", err)
	}
	select {
	case err := <-done:
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("child exit = %v, want it killed by the second signal", err)
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGTERM {
			t.Fatalf("child exit status = %v, want terminated by SIGTERM", exitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second SIGTERM did not end a stuck shutdown")
	}
}
