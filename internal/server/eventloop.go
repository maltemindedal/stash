//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/record"
)

// pollerEventBatch bounds how many readiness events one Wait call returns.
const pollerEventBatch = 128

// eventLoopReadChunk is the size of the shared buffer used for socket reads.
const eventLoopReadChunk = 64 * 1024

// eventLoopReadBudget caps how many bytes one readiness event may read from a
// single connection before yielding, so one fast writer cannot starve the
// other connections. Level-triggered readiness re-reports leftover data.
const eventLoopReadBudget = 1 << 20

// eventLoopOutputHighWater is the pending-output level above which the loop
// stops reading and executing further requests for a connection until its
// output drains, as the goroutine mode stops in Output.Flush.
const eventLoopOutputHighWater = outputHighWater

// eventLoopAcceptRetryDelay is how long accepting pauses after the process
// runs out of file descriptors, preventing a level-triggered busy loop on a
// backlog the loop cannot drain.
const eventLoopAcceptRetryDelay = 100 * time.Millisecond

// eventLoopKeepaliveSeconds is both the idle time before the first TCP keepalive
// probe and the interval between probes, the values the net package uses for the
// connections the default networking path accepts.
const eventLoopKeepaliveSeconds = 15

// pollEvent is one readiness notification translated from the OS poller.
type pollEvent struct {
	fd       int
	readable bool
	writable bool
}

// poller abstracts the OS readiness-notification facility (epoll on Linux,
// kqueue on macOS). Wait and the mutation methods are owned by the event-loop
// goroutine; only Wake is safe to call from other goroutines.
type poller interface {
	// Add registers fd for read readiness.
	Add(fd int) error
	// Set replaces fd's readiness interest set.
	Set(fd int, readable, writable bool) error
	// Remove deregisters fd entirely.
	Remove(fd int) error
	// Wait blocks until readiness events arrive and translates them into
	// events, returning how many entries were filled.
	Wait(events []pollEvent) (int, error)
	// Wake unblocks a concurrent Wait call.
	Wake() error
	// Close releases the poller resources.
	Close() error
}

// errEventLoopWouldBlock reports that a socket write consumed only part of the
// pending output because the kernel send buffer is full.
var errEventLoopWouldBlock = errors.New("server: socket write would block")

// serveEventLoop serves every client connection through a single goroutine
// driven by OS readiness notifications, dispatching readable and writable
// sockets through per-connection ConnMachine state machines.
func (s *Server) serveEventLoop(ctx context.Context, listener net.Listener) error {
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		return fmt.Errorf("server: event loop requires a TCP listener, got %T", listener)
	}

	// Duplicate the listener socket so the event loop owns a raw descriptor.
	// The original listener stays bound; both descriptors share one accept
	// queue and shutdown closing the original does not disturb the duplicate.
	listenerFile, err := tcpListener.File()
	if err != nil {
		return fmt.Errorf("server: dup listener descriptor: %w", err)
	}
	listenFD := int(listenerFile.Fd())
	if err := syscall.SetNonblock(listenFD, true); err != nil {
		_ = listenerFile.Close()
		return fmt.Errorf("server: set listener nonblocking: %w", err)
	}

	p, err := newPoller()
	if err != nil {
		_ = listenerFile.Close()
		return fmt.Errorf("server: create poller: %w", err)
	}
	if err := p.Add(listenFD); err != nil {
		_ = p.Close()
		_ = listenerFile.Close()
		return fmt.Errorf("server: register listener: %w", err)
	}

	loop := &eventLoop{
		srv:          s,
		poller:       p,
		listenerFile: listenerFile,
		listenFD:     listenFD,
		conns:        make(map[int]*eventConn),
		events:       make([]pollEvent, pollerEventBatch),
		readBuf:      make([]byte, eventLoopReadChunk),
	}

	s.logger.Info("serving clients through the event loop", "poller", "os-readiness")
	return loop.run(ctx)
}

// connCommandRunner returns a ConnCommandRunner that executes one parsed
// request through the command pipeline shared with the goroutine path.
// Responses are returned for the ConnMachine to hand to the connection's
// Output rather than written directly.
func (s *Server) connCommandRunner(call Call, clientID uint64, conn ClientConn, logger *slog.Logger) ConnCommandRunner {
	return func(ctx context.Context, request protocol.Value) ([]protocol.Value, error) {
		return s.executeClientRequest(ctx, call, clientID, conn, logger, request)
	}
}

// eventLoop drives all client connections from one goroutine. Connection state
// (ConnMachine, interest flags) is owned exclusively by that goroutine; the
// mutex only guards the queues that other goroutines use to hand work to the
// loop (connections with output to write, close requests, and accept
// resumption). Whoever adds to a queue wakes the poller, but only while the
// loop is waiting in it (wakeIfWaiting).
type eventLoop struct {
	srv          *Server
	poller       poller
	listenerFile *os.File
	listenFD     int
	ctx          context.Context

	conns        map[int]*eventConn
	events       []pollEvent
	readBuf      []byte
	acceptPaused bool
	// draining is set once the server shuts down or accepting fails
	// (beginDrain). The loop then serves only the replicas, to send them what is
	// queued for them, and returns drainErr once no connection is left. The
	// server's shutdown owns the time that takes: it ends each replica's stream,
	// waits replicaShutdownGrace for the replicas to close their connections,
	// and then asks for every connection to be closed (Server.shutdown).
	draining bool
	drainErr error

	// waiting is set from just before the loop looks at its queues for the last
	// time ahead of a Wait until that Wait returns.
	waiting atomic.Bool

	mu sync.Mutex
	// dirty are the connections whose Output was given bytes the loop has not
	// been asked to write yet, each listed once (eventConn.dirty).
	dirty        []*eventConn
	spareDirty   []*eventConn
	closeReqs    []*eventConn
	resumeAccept bool
}

// eventConn tracks one accepted connection inside the event loop. All fields
// are owned by the loop goroutine except out, dirty and endStream, which are
// safe for concurrent use, and closeRequested and detached, which are guarded
// by eventLoop.mu.
type eventConn struct {
	fd         int
	clientID   uint64
	remoteAddr net.Addr
	machine    *ConnMachine
	out        *Output
	ctx        context.Context
	run        ConnCommandRunner
	logger     *slog.Logger
	wantRead   bool
	wantWrite  bool
	peerClosed bool
	// authTimer closes the connection if it has not authenticated in time; nil
	// where no password is required.
	authTimer *time.Timer
	// dirty is set while the connection is on eventLoop.dirty.
	dirty atomic.Bool
	// endStream is set when the stream to the peer is to end, as shutdown ends
	// a replica's (loopOutputDriver.closeWrite): once the connection's output
	// has reached the kernel, the loop shuts the write side (writeShut), and it
	// closes the connection when the peer closes its own.
	endStream atomic.Bool
	writeShut bool

	closeRequested bool
	detached       bool
}

func (l *eventLoop) run(ctx context.Context) error {
	l.ctx = ctx
	defer l.cleanup()

	stopWake := context.AfterFunc(ctx, func() {
		_ = l.poller.Wake()
	})
	defer stopWake()

	for {
		// What the events below queued for other connections on this goroutine,
		// such as the messages of a PUBLISH, is written here without a wakeup.
		l.applyQueuedWork()
		if l.draining && len(l.conns) == 0 {
			return l.drainErr
		}

		// Another goroutine wakes the poller only if it sees waiting set after it
		// queued its work. Work queued before that is seen here instead, after
		// waiting is set, so no wakeup is lost between the two.
		l.waiting.Store(true)
		if l.hasQueuedWork() {
			_ = l.poller.Wake()
		}
		n, err := l.poller.Wait(l.events)
		l.waiting.Store(false)
		if err != nil {
			return fmt.Errorf("server: event loop wait: %w", err)
		}

		for _, event := range l.events[:n] {
			if event.fd == l.listenFD {
				if event.readable && !l.draining {
					if err := l.acceptReady(); err != nil {
						l.beginDrain(err)
					}
				}
				continue
			}

			conn := l.conns[event.fd]
			if conn == nil {
				continue
			}
			if event.readable {
				l.connReadable(conn)
			}
			// The readable path may have closed and unregistered the
			// connection; re-check before resuming it.
			if event.writable && l.conns[event.fd] == conn {
				// Draining output may also unblock request execution that was
				// paused by the output high-water mark.
				l.processConn(conn)
			}
		}
	}
}

// hasQueuedWork reports whether another goroutine has handed the loop work it
// has not taken yet.
func (l *eventLoop) hasQueuedWork() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.dirty) > 0 || len(l.closeReqs) > 0 || l.resumeAccept
}

// wakeIfWaiting wakes the poller if the loop is waiting in it, or about to. A
// loop that is running takes the queues before it waits again.
func (l *eventLoop) wakeIfWaiting() {
	if l.waiting.Load() {
		_ = l.poller.Wake()
	}
}

// markDirty lists conn as having output the loop has not been asked to write,
// or a stream the loop has not been asked to end. Safe for concurrent use; it
// may be called with the connection's Output locked.
func (l *eventLoop) markDirty(conn *eventConn) {
	if !conn.dirty.CompareAndSwap(false, true) {
		return
	}
	l.mu.Lock()
	l.dirty = append(l.dirty, conn)
	l.mu.Unlock()
	l.wakeIfWaiting()
}

// applyQueuedWork writes the output other connections and goroutines queued,
// applies queued close requests, and resumes accepting after a
// descriptor-exhaustion pause.
func (l *eventLoop) applyQueuedWork() {
	l.mu.Lock()
	dirty := l.dirty
	l.dirty = l.spareDirty[:0]
	closes := append([]*eventConn(nil), l.closeReqs...)
	l.closeReqs = l.closeReqs[:0]
	resumeAccept := l.resumeAccept
	l.resumeAccept = false
	l.mu.Unlock()

	if l.ctx.Err() != nil && !l.draining {
		l.beginDrain(nil)
	}

	for i, conn := range dirty {
		dirty[i] = nil
		// Cleared before the write, so that output queued during it lists the
		// connection again.
		conn.dirty.Store(false)
		if l.conns[conn.fd] == conn {
			l.finishConnEvent(conn)
		}
	}
	l.spareDirty = dirty[:0]

	for _, conn := range closes {
		l.closeConn(conn, nil)
	}

	if resumeAccept && l.acceptPaused && !l.draining {
		if err := l.poller.Set(l.listenFD, true, false); err != nil {
			l.srv.logger.Warn("failed to resume accepting after descriptor exhaustion", "error", err)
			return
		}
		l.acceptPaused = false
	}
}

// beginDrain starts the loop's end, when the server shuts down (cause is nil) or
// accepting fails (cause, which run then returns). It stops accepting and closes
// every connection but the replicas'. The Master goes on pushing to those until
// shutdown stops it and ends their streams (replication.Master.EndStreams):
// each connection is sent what is queued for it, has its write side shut, and
// is closed once the replica closes its own (finishConnEvent). The loop keeps
// no time itself. Shutdown closes whatever is still open when its grace has
// passed (ClientRegistry.CloseAll), and run returns when no connection is
// left.
func (l *eventLoop) beginDrain(cause error) {
	l.draining = true
	l.drainErr = cause
	if err := l.poller.Set(l.listenFD, false, false); err != nil {
		l.srv.logger.Debug("failed to stop accepting connections for shutdown", "error", err)
	}
	for _, conn := range l.conns {
		if !conn.out.isReplica() {
			l.closeConn(conn, nil)
		}
	}
	if cause != nil {
		// Only a cancelled context starts shutdown on its own; serve would start
		// it once run returns, and run returns only once shutdown has closed the
		// replicas' connections.
		go l.srv.shutdown()
	}
}

// requestClose asks the loop goroutine to close conn, which it does whether or
// not it is draining and whoever the peer is. Safe for concurrent use.
func (l *eventLoop) requestClose(conn *eventConn) {
	l.mu.Lock()
	if conn.detached || conn.closeRequested {
		l.mu.Unlock()
		return
	}
	conn.closeRequested = true
	l.closeReqs = append(l.closeReqs, conn)
	l.mu.Unlock()

	l.wakeIfWaiting()
}

// acceptReady accepts queued connections until the listener would block. A
// fatal accept error is returned and terminates the event loop, matching the
// goroutine path; descriptor exhaustion pauses accepting briefly instead of
// busy-looping on level-triggered readiness.
func (l *eventLoop) acceptReady() error {
	for {
		fd, sa, err := syscall.Accept(l.listenFD)
		if err != nil {
			switch err {
			case syscall.EAGAIN:
				return nil
			case syscall.EINTR, syscall.ECONNABORTED, syscall.ECONNRESET:
				continue
			case syscall.EMFILE, syscall.ENFILE:
				l.srv.logger.Warn(
					"accept failed: file descriptor limit reached; pausing accepts",
					"error", err,
					"retry_after", eventLoopAcceptRetryDelay,
				)
				l.pauseAccepting()
				return nil
			default:
				return fmt.Errorf("server: accept connection: %w", err)
			}
		}

		syscall.CloseOnExec(fd)
		if err := syscall.SetNonblock(fd, true); err != nil {
			l.srv.logger.Warn("failed to set accepted connection nonblocking", "error", err)
			closeIgnoringError(fd)
			continue
		}
		// Match the latency behavior of the net package, which disables
		// Nagle's algorithm on accepted TCP connections.
		if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); err != nil {
			l.srv.logger.Debug("failed to set TCP_NODELAY on accepted connection", "error", err)
		}
		// The net package also enables keepalive on accepted connections. Without
		// it a peer that vanishes without a FIN (power loss, a dropped NAT entry)
		// leaves the connection, its client state and any replica registration
		// behind indefinitely.
		if err := enableKeepalive(fd); err != nil {
			l.srv.logger.Debug("failed to enable TCP keepalive on accepted connection", "error", err)
		}

		if l.srv.overConnectionLimit() {
			l.srv.logger.Warn("connection limit reached; refusing connection", "max_clients", l.srv.cfg.MaxClients)
			closeIgnoringError(fd)
			continue
		}

		l.registerConn(fd, sockaddrTCPAddr(sa))
	}
}

// pauseAccepting drops the listener's read interest and schedules a resumption
// wakeup, so descriptor exhaustion backs off instead of spinning.
func (l *eventLoop) pauseAccepting() {
	if l.acceptPaused {
		return
	}
	if err := l.poller.Set(l.listenFD, false, false); err != nil {
		l.srv.logger.Warn("failed to pause accepting", "error", err)
		return
	}
	l.acceptPaused = true

	time.AfterFunc(eventLoopAcceptRetryDelay, func() {
		l.mu.Lock()
		l.resumeAccept = true
		l.mu.Unlock()
		l.wakeIfWaiting()
	})
}

func (l *eventLoop) registerConn(fd int, remoteAddr net.Addr) {
	conn := &eventConn{fd: fd, remoteAddr: remoteAddr, wantRead: true}
	handle := &eventConnHandle{loop: l, conn: conn}

	clientID, state := l.srv.registerClient(handle)
	conn.clientID = clientID

	remoteAddrText := ""
	if remoteAddr != nil {
		remoteAddrText = remoteAddr.String()
	}
	state.SetRemoteAddr(remoteAddrText)

	conn.ctx = l.ctx
	conn.logger = l.srv.logger.With("client_id", clientID, "remote_addr", remoteAddrText)
	conn.out = newOutput(l.srv.outputs, &loopOutputDriver{loop: l, conn: conn}, conn.logger)
	state.BindOutput(conn.out)
	conn.machine = NewConnMachine(state)
	conn.machine.SetRequestLimits(func() protocol.Limits { return l.srv.requestLimits(state) })
	// Requests run inline on the loop goroutine, where whatever would block
	// returns an error instead.
	conn.run = l.srv.connCommandRunner(Call{Client: state, Origin: record.OriginClient, Inline: true}, clientID, handle, conn.logger)

	if err := l.poller.Add(fd); err != nil {
		conn.logger.Warn("failed to register accepted connection with poller", "error", err)
		conn.machine.Close(err)
		l.srv.teardownClient(clientID, conn.logger)
		closeIgnoringError(fd)
		return
	}

	l.conns[fd] = conn
	conn.logger.Debug("client connected")

	if l.srv.authTimeoutApplies(state) {
		// The timer runs on its own goroutine, so it only asks the loop to close
		// the connection; the loop does it on its next wakeup.
		conn.authTimer = time.AfterFunc(l.srv.cfg.AuthTimeout, func() {
			if !state.IsAuthenticated() {
				conn.logger.Info("closing a connection that did not authenticate in time", "timeout", l.srv.cfg.AuthTimeout)
				l.requestClose(conn)
			}
		})
	}
}

func (l *eventLoop) connReadable(conn *eventConn) {
	// Skip pulling more input while buffered output is above the high-water
	// mark; the interest set already dropped read readiness in that state, but
	// an event may still be in flight.
	if conn.machine.PendingOutputBytes() <= eventLoopOutputHighWater {
		budget := eventLoopReadBudget
		for budget > 0 {
			n, err := syscall.Read(conn.fd, l.readBuf)
			if n > 0 {
				budget -= n
				if conn.machine.State() == ConnStateActive {
					if feedErr := conn.machine.Feed(l.readBuf[:n]); feedErr != nil {
						l.closeConn(conn, feedErr)
						return
					}
				}
				// A machine in the closing state drains its buffered replies
				// but rejects new input, so bytes read past that point are
				// discarded.
			}
			if err != nil {
				if err == syscall.EAGAIN {
					break
				}
				if err == syscall.EINTR {
					continue
				}
				l.closeConn(conn, err)
				return
			}
			if n == 0 {
				// The peer finished sending. Serve the already-parsed pipeline
				// tail and drain its replies before closing, like the
				// goroutine path does.
				conn.peerClosed = true
				break
			}
			if n < len(l.readBuf) {
				break
			}
		}
	}

	l.processConn(conn)
}

// processConn executes parsed requests, interleaving flushes so buffered
// output stays near the high-water mark, then settles the connection's
// readiness interest. Execution pauses while output is backed up and resumes
// from writable events once the socket drains.
func (l *eventLoop) processConn(conn *eventConn) {
	// On the goroutine path a panic costs one connection (see handleConnection).
	// Every client shares this goroutine, so without the same recovery here a
	// single malformed frame would take down every connected client at once.
	defer func() {
		if r := recover(); r != nil {
			conn.logger.Error("recovered from panic in event loop connection", "panic", r)
			l.closeConn(conn, fmt.Errorf("server: panic processing connection: %v", r))
		}
	}()

	for conn.machine.State() != ConnStateClosed {
		if conn.machine.PendingOutputBytes() > eventLoopOutputHighWater {
			if !l.flushOnce(conn) {
				return
			}
			if conn.machine.PendingOutputBytes() > eventLoopOutputHighWater {
				break
			}
		}

		more, err := conn.machine.ProcessNext(conn.ctx, conn.run)
		if err != nil {
			l.closeConn(conn, err)
			return
		}
		if !more {
			break
		}
	}

	l.finishConnEvent(conn)
}

// flushOnce writes pending output once and reports whether the connection is
// still usable. A full kernel send buffer is not an error; any other write
// failure closes the connection.
func (l *eventLoop) flushOnce(conn *eventConn) bool {
	if err := conn.machine.Flush(fdWriter{fd: conn.fd}); err != nil && !errors.Is(err, errEventLoopWouldBlock) {
		l.closeConn(conn, err)
		return false
	}
	return true
}

// finishConnEvent flushes pending output, finishes connections whose machine
// closed or whose peer stopped sending and has been fully served, and settles
// the readiness interest set for the next Wait.
func (l *eventLoop) finishConnEvent(conn *eventConn) {
	if conn.machine.State() == ConnStateClosed {
		l.closeConn(conn, conn.machine.Err())
		return
	}
	if !l.flushOnce(conn) {
		return
	}
	if conn.machine.State() == ConnStateClosed {
		l.closeConn(conn, conn.machine.Err())
		return
	}

	if conn.peerClosed && !conn.machine.HasPendingOutput() && conn.machine.PendingRequests() == 0 {
		l.closeConn(conn, nil)
		return
	}
	if conn.endStream.Load() && !conn.writeShut && !conn.machine.HasPendingOutput() {
		// The end of the replica's stream may still be in the kernel's send
		// queue, and closing the socket now would let the replica's next
		// request (an acknowledgement) draw a reset that discards it. Shutting
		// the write side sends it, then the end of the stream; the replica's own
		// close is read as peerClosed above.
		if err := syscall.Shutdown(conn.fd, syscall.SHUT_WR); err != nil {
			l.closeConn(conn, err)
			return
		}
		conn.writeShut = true
	}

	// Reading stays off after EOF (level-triggered readiness would spin) and
	// while output is backed up beyond the high-water mark.
	wantRead := !conn.peerClosed && conn.machine.PendingOutputBytes() <= eventLoopOutputHighWater
	l.setInterest(conn, wantRead, conn.machine.HasPendingOutput())
}

func (l *eventLoop) setInterest(conn *eventConn, wantRead, wantWrite bool) {
	if conn.wantRead == wantRead && conn.wantWrite == wantWrite {
		return
	}
	if err := l.poller.Set(conn.fd, wantRead, wantWrite); err != nil {
		l.closeConn(conn, err)
		return
	}
	conn.wantRead = wantRead
	conn.wantWrite = wantWrite
}

func (l *eventLoop) closeConn(conn *eventConn, cause error) {
	if l.conns[conn.fd] != conn {
		return
	}
	delete(l.conns, conn.fd)
	if conn.authTimer != nil {
		conn.authTimer.Stop()
	}

	l.mu.Lock()
	conn.detached = true
	l.mu.Unlock()

	conn.machine.Close(cause)
	if err := l.poller.Remove(conn.fd); err != nil {
		conn.logger.Debug("failed to deregister connection from poller", "error", err)
	}
	closeIgnoringError(conn.fd)

	l.srv.teardownClient(conn.clientID, conn.logger)
}

func (l *eventLoop) cleanup() {
	conns := make([]*eventConn, 0, len(l.conns))
	for _, conn := range l.conns {
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		l.closeConn(conn, nil)
	}

	if err := l.poller.Close(); err != nil {
		l.srv.logger.Debug("failed to close poller", "error", err)
	}
	if err := l.listenerFile.Close(); err != nil {
		l.srv.logger.Debug("failed to close event loop listener descriptor", "error", err)
	}
}

// fdWriter adapts a non-blocking socket descriptor to io.Writer for
// ConnMachine.Flush. A full kernel send buffer surfaces as
// errEventLoopWouldBlock with a partial byte count, leaving the remainder
// buffered in the machine until the poller reports the socket writable.
type fdWriter struct {
	fd int
}

// Write pushes p to the socket, retrying short writes and EINTR. It reports
// errEventLoopWouldBlock with a partial count when the kernel send buffer fills.
func (w fdWriter) Write(p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := syscall.Write(w.fd, p[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			if err == syscall.EAGAIN {
				return total, errEventLoopWouldBlock
			}
			return total, err
		}
	}
	return total, nil
}

// loopOutputDriver is the Event loop's side of a connection's Output: the loop
// writes every connection itself (ConnMachine.Flush), so the driver only tells
// it which connections were given output from elsewhere, and asks it to close
// one or to end its stream.
type loopOutputDriver struct {
	loop *eventLoop
	conn *eventConn
}

func (d *loopOutputDriver) schedule(*Output) { d.loop.markDirty(d.conn) }

func (d *loopOutputDriver) flush(*Output) error { return nil }

func (d *loopOutputDriver) shut() { d.loop.requestClose(d.conn) }

// closeWrite has the loop end the stream to the peer, now that everything
// queued for it was written (Output.EndStream): the loop shuts the
// connection's write side and closes it once the peer has closed its own
// (finishConnEvent). An Output calls it again each time it finds nothing left
// to write, so only the first call lists the connection.
func (d *loopOutputDriver) closeWrite() {
	if d.conn.endStream.CompareAndSwap(false, true) {
		d.loop.markDirty(d.conn)
	}
}

func (d *loopOutputDriver) peerClosed() bool { return false }

// eventConnHandle implements the ClientConn operations registered with the client
// registry so shutdown and INFO connected-client counts keep working for
// event-loop connections. Close hands the actual teardown to the loop
// goroutine, which owns all socket I/O.
type eventConnHandle struct {
	loop *eventLoop
	conn *eventConn
}

// Close asks the loop goroutine to tear the connection down and returns
// immediately; the socket is still open when it returns.
func (h *eventConnHandle) Close() error {
	h.loop.requestClose(h.conn)
	return nil
}

// RemoteAddr returns the peer address captured when the connection was accepted.
func (h *eventConnHandle) RemoteAddr() net.Addr { return h.conn.remoteAddr }

func sockaddrTCPAddr(sa syscall.Sockaddr) net.Addr {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		ip := make(net.IP, len(sa.Addr))
		copy(ip, sa.Addr[:])
		return &net.TCPAddr{IP: ip, Port: sa.Port}
	case *syscall.SockaddrInet6:
		ip := make(net.IP, len(sa.Addr))
		copy(ip, sa.Addr[:])
		return &net.TCPAddr{IP: ip, Port: sa.Port}
	default:
		return nil
	}
}

func closeIgnoringError(fd int) {
	_ = syscall.Close(fd)
}
