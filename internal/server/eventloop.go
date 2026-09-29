//go:build linux || darwin

package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
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
// buffered responses drain, so a pipelining client that reads slowly cannot
// amplify small requests into unbounded response memory.
const eventLoopOutputHighWater = 1 << 20

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
// Responses are returned for the ConnMachine to buffer rather than written
// directly. Replica registration happens before the responses are buffered,
// which is safe only because the loop is single-threaded: no other command can
// propagate to the new peer between registration and the handshake responses
// reaching the machine's write buffer.
func (s *Server) connCommandRunner(clientID uint64, conn ClientConn, logger *slog.Logger) ConnCommandRunner {
	return func(ctx context.Context, request protocol.Value) ([]protocol.Value, error) {
		responses, registerReplica, err := s.executeClientRequest(ctx, clientID, conn, logger, request)
		if err != nil {
			return nil, err
		}
		if registerReplica {
			s.registerReplicaPeer(clientID, conn)
		}
		return responses, nil
	}
}

// eventLoop drives all client connections from one goroutine. Connection state
// (ConnMachine, interest flags) is owned exclusively by that goroutine; the
// mutex only guards the queues that other goroutines use to hand work to the
// loop (async push frames, close requests, and accept resumption), which are
// paired with a poller wakeup.
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

	mu           sync.Mutex
	pushed       []*eventConn
	closeReqs    []*eventConn
	resumeAccept bool
}

// eventConn tracks one accepted connection inside the event loop. All fields
// are owned by the loop goroutine except pushBuf, pushQueued, closeRequested,
// and detached, which are guarded by eventLoop.mu.
type eventConn struct {
	fd         int
	clientID   uint64
	remoteAddr net.Addr
	machine    *ConnMachine
	ctx        context.Context
	run        ConnCommandRunner
	logger     *slog.Logger
	wantRead   bool
	wantWrite  bool
	peerClosed bool

	pushBuf        []byte
	pushQueued     bool
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
		n, err := l.poller.Wait(l.events)
		if err != nil {
			return fmt.Errorf("server: event loop wait: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}

		l.applyQueuedWork()

		for _, event := range l.events[:n] {
			if event.fd == l.listenFD {
				if event.readable {
					if err := l.acceptReady(); err != nil {
						return err
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

// applyQueuedWork moves cross-goroutine push frames into connection write
// buffers, applies queued close requests, and resumes accepting after a
// descriptor-exhaustion pause.
func (l *eventLoop) applyQueuedWork() {
	type pushWork struct {
		conn *eventConn
		data []byte
	}

	l.mu.Lock()
	pushes := make([]pushWork, 0, len(l.pushed))
	for _, conn := range l.pushed {
		if conn.detached || len(conn.pushBuf) == 0 {
			conn.pushQueued = false
			continue
		}
		pushes = append(pushes, pushWork{conn: conn, data: conn.pushBuf})
		conn.pushBuf = nil
		conn.pushQueued = false
	}
	l.pushed = l.pushed[:0]
	closes := append([]*eventConn(nil), l.closeReqs...)
	l.closeReqs = l.closeReqs[:0]
	resumeAccept := l.resumeAccept
	l.resumeAccept = false
	l.mu.Unlock()

	for _, push := range pushes {
		if l.conns[push.conn.fd] != push.conn {
			continue
		}
		if err := push.conn.machine.BufferEncoded(push.data); err != nil {
			l.closeConn(push.conn, err)
			continue
		}
		l.finishConnEvent(push.conn)
	}

	for _, conn := range closes {
		if l.conns[conn.fd] == conn {
			l.closeConn(conn, nil)
		}
	}

	if resumeAccept && l.acceptPaused {
		if err := l.poller.Set(l.listenFD, true, false); err != nil {
			l.srv.logger.Warn("failed to resume accepting after descriptor exhaustion", "error", err)
			return
		}
		l.acceptPaused = false
	}
}

// queuePush appends an async push frame (pub/sub message, monitor event, or
// replication payload) for delivery by the loop goroutine. Safe for concurrent
// use; the per-client responseMu already serializes whole frames. A
// connection whose push queue exceeds the write-buffer limit is closed, so a
// consumer that stopped draining its socket cannot grow server memory without
// bound.
func (l *eventLoop) queuePush(conn *eventConn, payload []byte) (int, error) {
	l.mu.Lock()
	if conn.detached {
		l.mu.Unlock()
		return 0, net.ErrClosed
	}
	if len(conn.pushBuf)+len(payload) > defaultMaxWriteBuffer {
		l.mu.Unlock()
		l.requestClose(conn)
		return 0, fmt.Errorf("server: push buffer exceeds %d byte write-buffer limit", defaultMaxWriteBuffer)
	}
	conn.pushBuf = append(conn.pushBuf, payload...)
	alreadyQueued := conn.pushQueued
	conn.pushQueued = true
	if !alreadyQueued {
		l.pushed = append(l.pushed, conn)
	}
	l.mu.Unlock()

	if err := l.poller.Wake(); err != nil {
		return 0, err
	}
	return len(payload), nil
}

// requestClose asks the loop goroutine to close conn. Safe for concurrent use.
func (l *eventLoop) requestClose(conn *eventConn) {
	l.mu.Lock()
	if conn.detached || conn.closeRequested {
		l.mu.Unlock()
		return
	}
	conn.closeRequested = true
	l.closeReqs = append(l.closeReqs, conn)
	l.mu.Unlock()

	_ = l.poller.Wake()
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
		_ = l.poller.Wake()
	})
}

func (l *eventLoop) registerConn(fd int, remoteAddr net.Addr) {
	conn := &eventConn{fd: fd, remoteAddr: remoteAddr, wantRead: true}
	handle := &eventConnHandle{loop: l, conn: conn}

	clientID, state := l.srv.registerClient(handle)
	conn.clientID = clientID
	state.BindResponseWriter(bufio.NewWriter(&eventConnPushWriter{loop: l, conn: conn}))

	remoteAddrText := ""
	if remoteAddr != nil {
		remoteAddrText = remoteAddr.String()
	}
	state.SetRemoteAddr(remoteAddrText)

	conn.machine = NewConnMachine(state)
	conn.machine.SetRequestLimits(func() protocol.Limits { return l.srv.requestLimits(state) })
	conn.ctx = WithInlineExecution(WithClientState(l.ctx, state))
	conn.logger = l.srv.logger.With("client_id", clientID, "remote_addr", remoteAddrText)
	conn.run = l.srv.connCommandRunner(clientID, handle, conn.logger)

	if err := l.poller.Add(fd); err != nil {
		conn.logger.Warn("failed to register accepted connection with poller", "error", err)
		conn.machine.Close(err)
		l.srv.teardownClient(clientID, conn.logger)
		closeIgnoringError(fd)
		return
	}

	l.conns[fd] = conn
	conn.logger.Debug("client connected")
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

	l.mu.Lock()
	conn.detached = true
	conn.pushBuf = nil
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

// eventConnPushWriter is the io.Writer bound (through a bufio.Writer) as the
// connection's ClientState response writer. Async deliveries from other
// goroutines land here and are handed to the loop goroutine for ordered
// flushing.
type eventConnPushWriter struct {
	loop *eventLoop
	conn *eventConn
}

// Write queues p for the loop goroutine to flush. It is safe to call from any
// goroutine; the bytes are not on the wire when it returns.
func (w *eventConnPushWriter) Write(p []byte) (int, error) {
	return w.loop.queuePush(w.conn, p)
}

// eventConnHandle implements the ClientConn operations registered with the client
// registry so shutdown, INFO connected-client counts, and replica bookkeeping
// keep working for event-loop connections. Close hands the actual teardown to
// the loop goroutine, which owns all socket I/O.
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
