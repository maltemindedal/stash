package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

const monitorWriteTimeout = 100 * time.Millisecond

// Stats is a snapshot of server-level state used by INFO.
type Stats struct {
	ConnectedClients    int
	MonitoringClients   int
	CommandsProcessed   int64
	Role                string
	MasterReplicationID string
	MasterOffset        int64
	ReplicaOffset       int64
	Replicas            []ReplicaInfo
	// AOFEnabled reports whether an append-only file is being written, and
	// AOFLastWriteOK whether its most recent write and fsync succeeded (true when
	// there is no AOF).
	AOFEnabled     bool
	AOFLastWriteOK bool
}

// ReplicaInfo describes one connected replica peer for INFO replication.
type ReplicaInfo struct {
	ID            uint64
	ListeningPort int
	// AckedMasterOffset is how far the replica has acknowledged the stream, in
	// the master's offsets as Redis reports it: 0 until it first acknowledges.
	// The replica's own acknowledged offset, which counts from zero when it
	// attached, is ReplicaPeer.AckOffset.
	AckedMasterOffset int64
}

type observedCommand struct {
	OK         bool
	Parts      []string
	ClientID   uint64
	ClientAddr string
	Timestamp  time.Time
}

func observeCommand(value protocol.Value, clientID uint64, conn ClientConn) observedCommand {
	parts, ok := commandParts(value)
	if !ok {
		return observedCommand{}
	}
	if len(parts) > 0 {
		parts[0] = strings.ToUpper(parts[0])
	}
	if len(parts) > 0 && parts[0] == "AUTH" {
		// SLOWLOG redacts AUTH arguments for the same reason: this stream reaches
		// other clients, and it includes attempts that failed.
		for i := 1; i < len(parts); i++ {
			parts[i] = "[redacted]"
		}
	}

	addr := ""
	if conn != nil && conn.RemoteAddr() != nil {
		addr = conn.RemoteAddr().String()
	}

	return observedCommand{
		OK:         true,
		Parts:      parts,
		ClientID:   clientID,
		ClientAddr: addr,
		Timestamp:  time.Now(),
	}
}

func commandParts(value protocol.Value) ([]string, bool) {
	array, ok := value.(protocol.Array)
	if !ok || array.Null || len(array.Elements) == 0 {
		return nil, false
	}

	parts := make([]string, 0, len(array.Elements))
	for _, element := range array.Elements {
		raw, err := protocol.Bytes(element)
		if err != nil {
			return nil, false
		}
		parts = append(parts, string(raw))
	}

	return parts, true
}

func (s *Server) broadcastMonitorEvent(command observedCommand) {
	if s == nil || s.monitorRegistry == nil || !command.OK {
		return
	}

	payload, err := protocol.Encode(protocol.SimpleString{Value: monitorLine(command)})
	if err != nil {
		s.logger.Warn("failed to encode monitor event", "error", err)
		return
	}

	subscribers := s.monitorRegistry.AppendSubscribers(nil)
	for _, subscriber := range subscribers {
		if subscriber == nil {
			continue
		}
		if err := subscriber.WriteEncodedWithDeadline(payload, monitorWriteTimeout); err != nil {
			s.logger.Warn("failed to deliver monitor event", "subscriber_id", subscriber.ID, "error", err)
			subscriber.Disconnect()
		}
	}
}

func monitorLine(command observedCommand) string {
	seconds := command.Timestamp.Unix()
	micros := command.Timestamp.Nanosecond() / int(time.Microsecond)
	var b strings.Builder
	b.WriteString(strconv.FormatInt(seconds, 10))
	b.WriteByte('.')
	microText := strconv.Itoa(micros)
	for i := len(microText); i < 6; i++ {
		b.WriteByte('0')
	}
	b.WriteString(microText)
	b.WriteString(" [")
	b.WriteString(strconv.FormatUint(command.ClientID, 10))
	b.WriteByte(' ')
	b.WriteString(command.ClientAddr)
	b.WriteByte(']')
	for _, part := range command.Parts {
		b.WriteByte(' ')
		b.WriteString(strconv.QuoteToASCII(part))
	}

	return b.String()
}
