package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

func (s *Server) handleConnection(ctx context.Context, clientID uint64, conn net.Conn) {
	defer s.handlerWG.Done()
	// A malformed frame must never take the whole process down. The per-request
	// cleanup defers below still run during the panic unwind; recover only stops
	// the crash and lets this one connection die.
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("recovered from panic in connection handler", "client_id", clientID, "panic", r)
		}
	}()

	state := s.getClientState(clientID)
	var input io.Reader = conn
	if state != nil {
		// Replies are queued while requests keep arriving and are sent when the
		// handler is about to wait for more input.
		input = flushBeforeRead{conn: conn, state: state}
	}
	parser := protocol.NewParser(input)
	writer := bufio.NewWriter(conn)

	remoteAddr := ""
	if addr := conn.RemoteAddr(); addr != nil {
		remoteAddr = addr.String()
	}

	if state != nil {
		state.BindResponseWriter(writer)
		state.BindResponseConn(conn)
		state.SetRemoteAddr(remoteAddr)
		ctx = WithClientState(ctx, state)
	}

	logger := s.logger.With("client_id", clientID, "remote_addr", remoteAddr)
	logger.Debug("client connected")
	defer s.teardownClient(clientID, logger)
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Debug("failed to close connection", "error", err)
		}
	}()

	stopClose := context.AfterFunc(ctx, func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Debug("failed to close connection after cancellation", "error", err)
		}
	})
	defer stopClose()
	// Runs before the connection is closed (deferred after it): whatever replies
	// are still queued when the handler leaves go out first, if the socket allows.
	defer func() { _ = state.TryFlushResponses() }()

	// On a server that requires a password, a connection gets a limited time to
	// authenticate. The deadline covers reads, including one that is half way
	// through a frame, so a client cannot hold a connection slot by trickling
	// bytes; it is lifted once the client authenticates.
	authDeadlineSet := false
	if s.authTimeoutApplies(state) {
		if err := conn.SetReadDeadline(time.Now().Add(s.cfg.AuthTimeout)); err == nil {
			authDeadlineSet = true
		}
	}

	for {
		// A client that has not authenticated may send only small frames; the
		// limit is read before every frame because an AUTH lifts it.
		parser.SetLimits(s.requestLimits(state))
		value, err := parser.Parse()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			if authDeadlineSet && errors.Is(err, os.ErrDeadlineExceeded) {
				logger.Info("closing a connection that did not authenticate in time", "timeout", s.cfg.AuthTimeout)
				return
			}

			// After a frame that cannot be parsed the stream is out of step and
			// nothing that follows can be trusted, so answer once and close, as
			// the event loop and Redis do. Carrying on made every further byte of
			// the garbage draw its own error reply and log line.
			logger.Warn("failed to parse request", "error", err)
			if writeErr := s.writeClientResponses(ctx, writer, []protocol.Value{protocol.ErrorValue{Message: "ERR " + err.Error()}}); writeErr != nil {
				logger.Warn("failed to write parser error", "parse_error", err, "write_error", writeErr)
			}
			return
		}

		responses, registerReplica, execErr := s.executeClientRequest(ctx, clientID, conn, logger, value)
		if execErr != nil {
			return
		}

		if authDeadlineSet && state.IsAuthenticated() {
			_ = conn.SetReadDeadline(time.Time{})
			authDeadlineSet = false
		}

		if err := s.writeClientResponses(ctx, writer, responses); err != nil {
			logger.Warn("failed to write response", "error", err)
			return
		}
		// PSYNC registered the replica at its attach cut, and its feed has
		// been holding every write since. Start the feed only once the
		// handshake response has reached the socket, so a propagated command
		// cannot precede the FULLRESYNC frames in the replica's stream.
		// Replies are queued, so send them first.
		if registerReplica {
			if err := state.FlushResponses(); err != nil {
				logger.Warn("failed to write handshake response", "error", err)
				return
			}
			s.registerReplicaPeer(clientID, conn)
		}
	}
}

// authTimeoutApplies reports whether a new connection must authenticate within
// the configured time: only where a password is required and a timeout is set.
func (s *Server) authTimeoutApplies(state *ClientState) bool {
	return s.cfg.RequirePass != "" && s.cfg.AuthTimeout > 0 && state != nil && !state.IsAuthenticated()
}

// requestLimits returns what the client's next frame may declare. On a server
// that requires a password, a client that has not authenticated gets the small
// pre-AUTH limits, so it cannot make the server buffer large requests for
// commands it is not allowed to run.
func (s *Server) requestLimits(state *ClientState) protocol.Limits {
	if s.cfg.RequirePass == "" || state == nil || state.IsAuthenticated() {
		return protocol.Limits{}
	}
	return protocol.UnauthenticatedLimits
}

// flushBeforeRead is what the parser reads from. It sends the client's queued
// replies before every read of the socket, so a reply is never held back while
// the handler waits for the client's next request, which is when a client that
// waits for its reply would deadlock with us. A read that finds the parser's
// buffer empty is the only time the handler can block on input.
type flushBeforeRead struct {
	conn  net.Conn
	state *ClientState
}

func (r flushBeforeRead) Read(p []byte) (int, error) {
	if err := r.state.TryFlushResponses(); err != nil {
		return 0, err
	}
	return r.conn.Read(p)
}

// executeClientRequest runs one parsed request through the command pipeline
// shared by both networking modes: monitor observation, execution, durability
// preparation and its persistence-failure downgrade, mutation-effect
// finalization, and the processed-command counter. It returns the RESP
// responses to deliver and whether the caller must start the feed of the
// replica peer PSYNC registered (registerReplicaPeer) once those responses have
// been written or buffered. A non-nil error is fatal for the connection.
func (s *Server) executeClientRequest(ctx context.Context, clientID uint64, conn ClientConn, logger *slog.Logger, request protocol.Value) ([]protocol.Value, bool, error) {
	if s.monitorRegistry.HasSubscribers() {
		s.broadcastMonitorEvent(observeCommand(request, clientID, conn))
	}

	result, execErr := s.executeRequest(ctx, request)
	// Until the result's frames have been logged and sent to the replicas, no
	// other write may run; see sequencer in the command package.
	if result.Release != nil {
		defer result.Release()
	}
	if execErr != nil {
		if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) || errors.Is(execErr, ErrClientDisconnected) {
			return nil, false, execErr
		}
		logger.Debug("command execution failed", "error", execErr)
		result = SingleResponse(responseError(execErr))
	}

	durabilityPayload, durableErr := s.prepareDurabilityBeforeResponse(result.Durability, logger)
	if durableErr != nil {
		result = SingleResponse(persistenceFailureResponse())
		result.Propagation = nil
		result.Durability = nil
		durabilityPayload = nil
	}

	s.finalizeMutationEffects(ctx, result.Durability, durabilityPayload, result.Propagation, logger)
	s.commandsProcessed.Add(1)

	return result.Responses, result.RegisterReplica, nil
}

// executeRequest runs one request through the executor, ordered against other
// requests when the executor supports it.
func (s *Server) executeRequest(ctx context.Context, request protocol.Value) (ExecuteResult, error) {
	if sequenced, ok := s.executor.(sequencedExecutor); ok {
		return sequenced.ExecuteSequenced(ctx, request)
	}
	return s.executor.ExecuteDetailed(ctx, request)
}

func (s *Server) writeClientResponses(ctx context.Context, writer *bufio.Writer, values []protocol.Value) error {
	if state, ok := ClientStateFromContext(ctx); ok && state != nil {
		return state.QueueResponses(values)
	}

	return s.writeResponses(writer, values)
}

func (s *Server) writeResponses(writer *bufio.Writer, values []protocol.Value) error {
	if len(values) == 1 {
		if err := protocol.WriteValue(writer, values[0]); err != nil {
			return err
		}
		return writer.Flush()
	}

	payload, err := protocol.EncodeValues(values)
	if err != nil {
		return err
	}
	if _, err := writer.Write(payload); err != nil {
		return err
	}
	return writer.Flush()
}

func responseError(err error) protocol.ErrorValue {
	prefix := "ERR"

	type respError interface {
		RESPErrorPrefix() string
	}

	var typed respError
	if errors.As(err, &typed) {
		prefix = typed.RESPErrorPrefix()
	}

	return protocol.ErrorValue{Message: prefix + " " + err.Error()}
}
