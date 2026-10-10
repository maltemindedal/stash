package test

import (
	"net"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// blockOn connects a client and leaves it blocked in BLPOP on key. The PING
// ahead of the BLPOP proves it: BLPOP flushes earlier replies only once it is
// waiting for a push.
func blockOn(t *testing.T, addr, key string) (net.Conn, *protocol.Parser) {
	t.Helper()

	conn, parser := dialClient(t, addr)
	if _, err := conn.Write(encodeCommands(t, []string{"PING"}, []string{"BLPOP", key})); err != nil {
		t.Fatalf("Write(PING, BLPOP) error = %v", err)
	}
	assertValuesEqual(t, readReplyWithin(t, conn, parser, 2*time.Second, "PONG ahead of BLPOP"), protocol.SimpleString{Value: "PONG"})
	return conn, parser
}

func TestAPushOfSeveralElementsServesBlockedClientsInWaitingOrder(t *testing.T) {
	// A push used to wake one blocked client, however many elements it added:
	// the second client stayed blocked with an element in the list. As in Redis,
	// the longest-waiting client gets the list's head, and the next client the
	// element after it, for as long as elements remain.
	tests := []struct {
		name   string
		push   []string
		first  string
		second string
		left   []string
	}{
		{name: "RPUSH of two", push: []string{"RPUSH", "q", "a", "b"}, first: "a", second: "b", left: nil},
		{name: "LPUSH of two", push: []string{"LPUSH", "q", "a", "b"}, first: "b", second: "a", left: nil},
		{name: "RPUSH of three", push: []string{"RPUSH", "q", "a", "b", "c"}, first: "a", second: "b", left: []string{"c"}},
		{name: "LPUSH of three", push: []string{"LPUSH", "q", "a", "b", "c"}, first: "c", second: "b", left: []string{"a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, stop, errCh := startTestServer(t, defaultTestConfig())
			defer func() {
				stop()
				waitForServerStop(t, errCh)
			}()
			admin, adminParser := dialClient(t, addr)

			first, firstParser := blockOn(t, addr, "q")
			second, secondParser := blockOn(t, addr, "q")
			assertCommandResponse(t, admin, adminParser, protocol.Integer{Value: int64(len(tt.push) - 2)}, tt.push...)

			assertValuesEqual(t, readReplyWithin(t, first, firstParser, 2*time.Second, "BLPOP reply to the longest-waiting client"), blpopReply("q", tt.first))
			assertValuesEqual(t, readReplyWithin(t, second, secondParser, 2*time.Second, "BLPOP reply to the next client"), blpopReply("q", tt.second))

			left := make([]protocol.Value, 0, len(tt.left))
			for _, element := range tt.left {
				left = append(left, protocol.BulkString{Data: []byte(element)})
			}
			assertValuesEqual(t, roundTrip(t, admin, adminParser, "LRANGE", "q", "0", "-1"), protocol.Array{Elements: left})
		})
	}
}

func blpopReply(key, element string) protocol.Value {
	return protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte(key)},
		protocol.BulkString{Data: []byte(element)},
	}}
}
