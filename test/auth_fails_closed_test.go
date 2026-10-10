package test

import (
	"context"
	"net"
	"testing"

	"github.com/maltemindedal/stash/internal/command"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

// passwordDroppingExecutor is the command executor with the password's wiring
// cut: server.New hands it the configured password and it is dropped, as it is
// when the method server.New looks for is renamed. Everything else is wired.
type passwordDroppingExecutor struct {
	*command.Executor
}

func (passwordDroppingExecutor) SetRequirePass(string) {}

func TestAuthFailsClosedWhenThePasswordNeverReachesTheExecutor(t *testing.T) {
	noAuth := protocol.ErrorValue{Message: "NOAUTH Authentication required."}
	authNotConfigured := protocol.ErrorValue{Message: "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct"}

	for _, eventLoop := range []bool{false, true} {
		name := "goroutine per connection"
		if eventLoop {
			name = "event loop"
		}
		t.Run(name, func(t *testing.T) {
			cfg := defaultTestConfig()
			cfg.EventLoop = eventLoop
			cfg.RequirePass = "secret"

			logger := stashlogger.New(cfg.LogLevel)
			store := storage.NewStore()
			srv := server.New(cfg, logger, store, passwordDroppingExecutor{command.NewExecutor(store, logger)})
			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() {
				errCh <- srv.ListenAndServe(ctx)
			}()
			defer func() {
				cancel()
				waitForServerStop(t, errCh)
			}()

			addr := waitForAddr(t, srv)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Dial(%q) error = %v", addr, err)
			}
			defer closeTestResource(t, conn)
			parser := protocol.NewParser(conn)

			assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
			assertCommandResponse(t, conn, parser, noAuth, "GET", "name")
			assertCommandResponse(t, conn, parser, noAuth, "SET", "name", "Stash")
			assertCommandResponse(t, conn, parser, authNotConfigured, "AUTH", "secret")
			assertCommandResponse(t, conn, parser, noAuth, "GET", "name")
		})
	}
}
