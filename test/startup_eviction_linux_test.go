//go:build linux

package test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// With fileSizeChildAOFEnv set, TestServerRefusesToStartWhenItCannotLogStartupEvictions
// runs as the child process its parent starts: the variable names the
// append-only file to start on, and fileSizeChildPolicyEnv the --appendfsync
// policy.
const (
	fileSizeChildAOFEnv    = "STASH_FILE_SIZE_CHILD_AOF"
	fileSizeChildPolicyEnv = "STASH_FILE_SIZE_CHILD_APPENDFSYNC"
)

func TestServerRefusesToStartWhenItCannotLogStartupEvictions(t *testing.T) {
	// A server that serves after evicting keys at startup without their DEL in
	// the append-only file brings the keys back on its next start, so a failed
	// append stops startup instead.
	//
	// The keys come from the append-only file itself. Keys from an RDB snapshot
	// would first be written into an empty file, and a device that fails every
	// write, such as /dev/full, is refused at that step. A file size limit at the
	// file's size lets the file open and load and fails the first write that
	// would grow it: the DEL.
	//
	// The limit holds for every file the process writes, including the log that
	// go test has the test binary write when it caches results. A chunk of that
	// log written while the limit held failed, and the binary exited 2 after
	// every test passed. So the server starts in a child process: this test, run
	// again with fileSizeChildAOFEnv set and without that log.
	if aofPath := os.Getenv(fileSizeChildAOFEnv); aofPath != "" {
		startUnderAFileSizeLimit(t, aofPath, os.Getenv(fileSizeChildPolicyEnv))
		return
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable() error = %v", err)
	}
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run("appendfsync "+policy, func(t *testing.T) {
			aofPath := filepath.Join(t.TempDir(), "appendonly.aof")
			content := writeSetsAOF(t, aofPath, evictableKeys(50))

			cmd := exec.Command(executable, "-test.run=^TestServerRefusesToStartWhenItCannotLogStartupEvictions$", "-test.v", "-test.timeout=1m")
			cmd.Env = append(os.Environ(), fileSizeChildAOFEnv+"="+aofPath, fileSizeChildPolicyEnv+"="+policy)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child process error = %v, output:\n%s", err, out)
			}
			if !bytes.Contains(out, []byte("--- PASS: TestServerRefusesToStartWhenItCannotLogStartupEvictions ")) {
				t.Fatalf("child process passed no test, output:\n%s", out)
			}
			got, err := os.ReadFile(aofPath)
			if err != nil || !bytes.Equal(got, content) {
				t.Fatalf("the append-only file changed (read error %v)", err)
			}
		})
	}
}

// startUnderAFileSizeLimit is the child process's half of
// TestServerRefusesToStartWhenItCannotLogStartupEvictions. It starts a server on
// aofPath under policy and maxmemory 2000 with the file size limited to the
// file's size, and checks that the server refuses to start because it cannot
// append the DEL. The Go runtime ignores the SIGXFSZ the failed write raises.
func startUnderAFileSizeLimit(t *testing.T, aofPath, policy string) {
	t.Helper()

	info, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", aofPath, err)
	}
	cfg := testAOFConfig(aofPath)
	cfg.AppendFsync = policy
	cfg.MaxMemory = 2000
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewStore()
	srv := server.New(cfg, logger, store, command.NewExecutor(store, logger))

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatalf("Getrlimit(RLIMIT_FSIZE) error = %v", err)
	}
	lowered := limit
	lowered.Cur = uint64(info.Size())
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &lowered); err != nil {
		t.Fatalf("Setrlimit(RLIMIT_FSIZE, %d) error = %v", lowered.Cur, err)
	}
	// A server that wrongly starts would serve until the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = srv.ListenAndServe(ctx)
	// Lifted before anything else is written, such as coverage data at exit.
	if restoreErr := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); restoreErr != nil {
		t.Fatalf("Setrlimit(RLIMIT_FSIZE) restore error = %v", restoreErr)
	}

	if err == nil {
		t.Fatal("ListenAndServe() error = nil, want a refusal to start")
	}
	if msg := err.Error(); !strings.HasPrefix(msg, "server: ") || !strings.Contains(msg, "evicted at startup") || !strings.Contains(msg, strconv.Quote(aofPath)) {
		t.Fatalf("ListenAndServe() error = %q, want a server: error about the keys evicted at startup naming %q", msg, aofPath)
	}
	if !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("ListenAndServe() error = %v, want the failed write's EFBIG", err)
	}
}
