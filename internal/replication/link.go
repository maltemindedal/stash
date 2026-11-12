package replication

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/maltemindedal/stash/internal/protocol"
)

// ReplicaStore is the Store of a Replica, as its Link changes it.
type ReplicaStore interface {
	// Replace replaces the whole keyspace with the RDB snapshot of a full
	// resync.
	Replace(snapshot []byte) error
	// Apply applies one command of the Master's replication stream. An error
	// ends the link.
	Apply(ctx context.Context, command protocol.Value) error
}

// Link is a Replica's link to its Master: the handshake, the full resync and
// the replication stream, over any connection (Run). It counts the Replica's
// replication offset and answers the Master's REPLCONF GETACK itself.
type Link struct {
	// ListeningPort is the port the Replica announces with REPLCONF
	// listening-port.
	ListeningPort int
	// MasterAuth is the password sent with AUTH, or empty to send none.
	MasterAuth string
	Store      ReplicaStore
	Logger     *slog.Logger

	// offset is how many bytes of the stream the Replica has processed since
	// its last full resync.
	offset atomic.Int64
}

// Offset reports the Replica's replication offset: the bytes of the stream it
// has processed since its last full resync.
func (l *Link) Offset() int64 {
	return l.offset.Load()
}

// Run attaches to the Master over upstream and applies its stream until the
// link ends: it sends PING, AUTH when MasterAuth is set, REPLCONF
// listening-port and PSYNC, replaces the Store with the snapshot, and then
// applies each command of the stream, answering REPLCONF GETACK with the
// offset counted so far. It reports whether the handshake completed, and why
// the link ended. The error is nil when ctx was cancelled or the Master closed
// the connection after the handshake. Closing upstream ends Run; the caller
// does that when ctx is cancelled.
func (l *Link) Run(ctx context.Context, upstream io.ReadWriter) (synchronised bool, err error) {
	parser := protocol.NewParser(upstream)
	writer := bufio.NewWriter(upstream)

	if err := writeLinkCommand(writer, "PING"); err != nil {
		return false, fmt.Errorf("failed to send replica PING: %w", err)
	}
	if err := expectSimpleString(parser, "PONG"); err != nil {
		return false, fmt.Errorf("invalid PING response from master: %w", err)
	}

	if l.MasterAuth != "" {
		if err := writeLinkCommand(writer, "AUTH", l.MasterAuth); err != nil {
			return false, fmt.Errorf("failed to send replica AUTH: %w", err)
		}
		if err := expectAuthOK(parser); err != nil {
			return false, fmt.Errorf("replica AUTH failed: %w", err)
		}
	}

	if err := writeLinkCommand(writer, "REPLCONF", "listening-port", strconv.Itoa(l.ListeningPort)); err != nil {
		return false, fmt.Errorf("failed to send replica REPLCONF: %w", err)
	}
	if err := expectReplConfOK(parser); err != nil {
		return false, fmt.Errorf("replica REPLCONF failed: %w", err)
	}

	if err := writeLinkCommand(writer, "PSYNC", "?", "-1"); err != nil {
		return false, fmt.Errorf("failed to send replica PSYNC: %w", err)
	}
	if err := l.fullResync(parser); err != nil {
		return false, fmt.Errorf("failed to consume FULLRESYNC from master: %w", err)
	}

	// The stream that follows a full resync counts from zero.
	l.offset.Store(0)
	l.Logger.Info("replica handshake completed", "listening_port", l.ListeningPort)

	for {
		value, err := parser.Parse()
		if err != nil {
			// A Master that goes away inside a frame is still a lost link: the Parser
			// reports a bulk payload cut short as io.ErrUnexpectedEOF, not io.EOF.
			if ctx.Err() != nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
				return true, nil
			}
			return true, fmt.Errorf("replication stream parse failed: %w", err)
		}

		encodedLen, err := protocol.EncodedLen(value)
		if err != nil {
			return true, fmt.Errorf("failed to size replication stream value: %w", err)
		}
		offset := l.offset.Add(int64(encodedLen))
		name, subcommand := commandWords(value)
		l.Logger.Debug(
			"replication stream command received",
			"command", name,
			"payload_size", encodedLen,
			"replica_offset", offset,
		)

		// The Master asks how far the Replica has got. The answer counts the
		// request itself, and is the only thing a Replica writes on the link.
		if name == "REPLCONF" && subcommand == "GETACK" {
			if err := writeLinkCommand(writer, "REPLCONF", "ACK", strconv.FormatInt(offset, 10)); err != nil {
				return true, fmt.Errorf("failed to write replication upstream reply: %w", err)
			}
			continue
		}

		if err := l.Store.Apply(ctx, value); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return true, nil
			}
			return true, fmt.Errorf("replication stream command failed: %w", err)
		}
	}
}

// fullResync reads the Master's answer to PSYNC, the FULLRESYNC line and the
// RDB snapshot, and replaces the Store with the snapshot.
func (l *Link) fullResync(parser *protocol.Parser) error {
	value, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("read FULLRESYNC line: %w", err)
	}

	if refusal, ok := value.(protocol.ErrorValue); ok {
		return fmt.Errorf("master refused PSYNC: %s", refusal.Message)
	}
	line, ok := value.(protocol.SimpleString)
	if !ok {
		return fmt.Errorf("unexpected FULLRESYNC response type %T", value)
	}
	parts := strings.Fields(line.Value)
	if len(parts) != 3 || !strings.EqualFold(parts[0], "FULLRESYNC") || parts[1] == "" || parts[2] != "0" {
		return fmt.Errorf("invalid FULLRESYNC response %q", line.Value)
	}

	snapshotValue, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("read replication snapshot: %w", err)
	}

	snapshot, ok := snapshotValue.(protocol.BulkString)
	if !ok || snapshot.Null {
		return fmt.Errorf("unexpected replication snapshot type %T", snapshotValue)
	}

	if err := l.Store.Replace(snapshot.Data); err != nil {
		return fmt.Errorf("load replication snapshot: %w", err)
	}

	return nil
}

// commandWords returns the first two words of a command of the stream, in
// upper case: its name and, for a command that has one, its subcommand. A value
// that is not a command is named by its type.
func commandWords(value protocol.Value) (name, subcommand string) {
	array, ok := value.(protocol.Array)
	if !ok || array.Null || len(array.Elements) == 0 {
		return fmt.Sprintf("%T", value), ""
	}
	rawName, err := protocol.Bytes(array.Elements[0])
	if err != nil {
		return fmt.Sprintf("%T", value), ""
	}
	name = strings.ToUpper(string(rawName))
	if len(array.Elements) > 1 {
		if raw, err := protocol.Bytes(array.Elements[1]); err == nil {
			subcommand = strings.ToUpper(string(raw))
		}
	}
	return name, subcommand
}

func writeLinkCommand(writer *bufio.Writer, parts ...string) error {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	if err := protocol.WriteValue(writer, protocol.Array{Elements: elements}); err != nil {
		return err
	}

	return writer.Flush()
}

func expectSimpleString(parser *protocol.Parser, expected string) error {
	value, err := parser.Parse()
	if err != nil {
		return err
	}

	switch typed := value.(type) {
	case protocol.SimpleString:
		if typed.Value != expected {
			return fmt.Errorf("unexpected response %q, want %q", typed.Value, expected)
		}
		return nil
	case protocol.ErrorValue:
		return fmt.Errorf("unexpected error response %q, want %q", typed.Message, expected)
	default:
		return fmt.Errorf("unexpected response type %T", value)
	}
}

func expectAuthOK(parser *protocol.Parser) error {
	value, err := parser.Parse()
	if err != nil {
		return err
	}

	switch typed := value.(type) {
	case protocol.SimpleString:
		if typed.Value != "OK" {
			return fmt.Errorf("unexpected AUTH response %q, want %q", typed.Value, "OK")
		}
		return nil
	case protocol.ErrorValue:
		switch respErrorCode(typed.Message) {
		case "WRONGPASS":
			return fmt.Errorf("replica AUTH rejected by master: %s", typed.Message)
		case "NOAUTH":
			return fmt.Errorf("master reported NOAUTH during replica AUTH: %s", typed.Message)
		default:
			return fmt.Errorf("replica AUTH failed with error response: %s", typed.Message)
		}
	default:
		return fmt.Errorf("unexpected AUTH response type %T", value)
	}
}

func expectReplConfOK(parser *protocol.Parser) error {
	value, err := parser.Parse()
	if err != nil {
		return err
	}

	switch typed := value.(type) {
	case protocol.SimpleString:
		if typed.Value != "OK" {
			return fmt.Errorf("unexpected REPLCONF response %q, want %q", typed.Value, "OK")
		}
		return nil
	case protocol.ErrorValue:
		switch respErrorCode(typed.Message) {
		case "NOAUTH":
			return fmt.Errorf("protected master requires replica authentication (--masterauth): %s", typed.Message)
		default:
			return fmt.Errorf("replica REPLCONF failed with error response: %s", typed.Message)
		}
	default:
		return fmt.Errorf("unexpected REPLCONF response type %T", value)
	}
}

func respErrorCode(message string) string {
	code, _, _ := strings.Cut(message, " ")
	return code
}
