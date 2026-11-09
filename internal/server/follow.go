package server

import (
	"context"
	"errors"
	"net"
	"time"
)

// How long a Replica waits before trying its Master again, and the most that
// wait grows to.
const (
	reconnectInitial = time.Second
	reconnectMax     = 30 * time.Second
)

// Follow keeps a Replica attached to its Master until ctx is cancelled: it
// dials, runs l over the connection, and when the link ends for any other
// reason it waits and starts again from a full resync. The wait doubles from
// one second up to thirty, and starts over after a link that got as far as a
// completed handshake.
//
// dial connects to the Master. sleep waits for a duration and reports false if
// ctx was cancelled first; nil waits on the real clock. Follow closes each
// connection it dialled, at once when ctx is cancelled, which is what ends a
// running link.
func Follow(ctx context.Context, dial func(context.Context) (net.Conn, error), l *Link, sleep func(context.Context, time.Duration) bool) {
	if sleep == nil {
		sleep = sleepContext
	}

	delay := reconnectInitial
	for {
		synchronised := followOnce(ctx, dial, l)
		if ctx.Err() != nil {
			return
		}
		if synchronised {
			delay = reconnectInitial
		}

		l.Logger.Warn("the link to the master ended; retrying", "retry_in", delay)
		if !sleep(ctx, delay) {
			return
		}
		delay = min(delay*2, reconnectMax)
	}
}

// followOnce makes one attempt to attach to the Master and applies its stream
// until the link ends. It reports whether the handshake completed.
func followOnce(ctx context.Context, dial func(context.Context) (net.Conn, error), l *Link) (synchronised bool) {
	conn, err := dial(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.Logger.Error("failed to connect to master", "error", err)
		}
		return false
	}
	closeConn := func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			l.Logger.Debug("failed to close replica upstream connection", "error", err)
		}
	}
	defer closeConn()
	// Run only returns once its connection does, so cancelling closes it.
	stopClose := context.AfterFunc(ctx, closeConn)
	defer stopClose()

	synchronised, err = l.Run(ctx, conn)
	if err != nil && ctx.Err() == nil {
		log := l.Logger.Error
		if synchronised {
			log = l.Logger.Warn
		}
		log("the link to the master failed", "error", err)
	}
	return synchronised
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
