package server

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// NotifyContext returns a context cancelled by SIGINT or SIGTERM.
//
// Only the first signal is caught. The handlers are removed before the context
// is cancelled, so a second SIGINT or SIGTERM ends the process with the default
// action. Graceful shutdown has no deadline of its own, so this is how an
// operator abandons one that is stuck.
func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-signals:
		case <-ctx.Done():
		}
		signal.Stop(signals)
		cancel()
	}()

	return ctx, cancel
}
