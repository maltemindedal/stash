package test

import (
	"net"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// blockOn connects a new client and sends BLPOP key on it, returning once the
// BLPOP waits. The PING sent ahead of it proves that: BLPOP sends the replies
// queued ahead of it only after it has registered to be woken by a push to key
// and found the list empty, so clients that block one after another wait in that
// order.
func blockOn(t *testing.T, addr, key string) (net.Conn, *protocol.Parser) {
	t.Helper()

	conn, parser := dialClient(t, addr)
	if _, err := conn.Write(encodeCommands(t, []string{"PING"}, []string{"BLPOP", key})); err != nil {
		t.Fatalf("Write(PING, BLPOP) error = %v", err)
	}
	assertValuesEqual(t, readReplyWithin(t, conn, parser, 5*time.Second, "PONG ahead of BLPOP"), protocol.SimpleString{Value: "PONG"})
	return conn, parser
}

// blpopReply is BLPOP's reply when it pops element from key.
func blpopReply(key, element string) protocol.Value {
	return protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte(key)},
		protocol.BulkString{Data: []byte(element)},
	}}
}

// listReply is LRANGE's reply for elements.
func listReply(elements ...string) protocol.Value {
	values := make([]protocol.Value, 0, len(elements))
	for _, element := range elements {
		values = append(values, protocol.BulkString{Data: []byte(element)})
	}
	return protocol.Array{Elements: values}
}

func TestAPushOfSeveralElementsServesBlockedClientsInWaitingOrder(t *testing.T) {
	// A push used to wake one blocked client however many elements it added, so
	// the other clients stayed blocked with elements in the list. The longest
	// waiting client gets the list's head, as in Redis, and the next gets the
	// next element.
	tests := []struct {
		name   string
		push   []string
		first  string
		second string
		left   []string
	}{
		{"RPUSH of two elements", []string{"RPUSH", "queue", "a", "b"}, "a", "b", nil},
		{"LPUSH of two elements", []string{"LPUSH", "queue", "a", "b"}, "b", "a", nil},
		{"RPUSH of three elements", []string{"RPUSH", "queue", "a", "b", "c"}, "a", "b", []string{"c"}},
		{"LPUSH of three elements", []string{"LPUSH", "queue", "a", "b", "c"}, "c", "b", []string{"a"}},
	}
	for _, tc := range tests {
		tc := tc // the subtest closure captures it (go 1.21 shares the loop variable)
		t.Run(tc.name, func(t *testing.T) {
			addr, stop, errCh := startTestServer(t, defaultTestConfig())
			defer func() {
				stop()
				waitForServerStop(t, errCh)
			}()
			admin, adminParser := dialClient(t, addr)

			first, firstParser := blockOn(t, addr, "queue")
			second, secondParser := blockOn(t, addr, "queue")
			assertCommandResponse(t, admin, adminParser, protocol.Integer{Value: int64(len(tc.push) - 2)}, tc.push...)

			assertValuesEqual(t, readReplyWithin(t, first, firstParser, 5*time.Second, "BLPOP reply for the client that waited longest"), blpopReply("queue", tc.first))
			assertValuesEqual(t, readReplyWithin(t, second, secondParser, 5*time.Second, "BLPOP reply for the client that waited next"), blpopReply("queue", tc.second))
			assertValuesEqual(t, roundTrip(t, admin, adminParser, "LRANGE", "queue", "0", "-1"), listReply(tc.left...))
		})
	}
}
