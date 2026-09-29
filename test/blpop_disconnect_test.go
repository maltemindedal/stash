package test

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
)

// connectedClients asks the server how many connections it currently serves.
func connectedClients(t *testing.T, conn net.Conn, parser *protocol.Parser) int {
	t.Helper()

	if err := protocol.WriteValue(conn, request("INFO", "clients")); err != nil {
		t.Fatalf("WriteValue(INFO) error = %v", err)
	}
	reply, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse(INFO) error = %v", err)
	}
	text, _, ok := integrationBulkStringContent(reply)
	if !ok {
		t.Fatalf("INFO reply type = %T, want a bulk string", reply)
	}
	for _, line := range strings.Split(text, "\r\n") {
		if value, found := strings.CutPrefix(line, "connected_clients:"); found {
			count := 0
			for _, digit := range value {
				count = count*10 + int(digit-'0')
			}
			return count
		}
	}
	t.Fatalf("INFO clients = %q, missing connected_clients", text)
	return 0
}

// waitForClients polls until the server serves exactly want connections.
func waitForClients(t *testing.T, conn net.Conn, parser *protocol.Parser, want int, within time.Duration, why string) {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		got := connectedClients(t, conn, parser)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connected_clients = %d, want %d within %v: %s", got, want, within, why)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBlockedBLPopEndsWhenTheClientDisconnects(t *testing.T) {
	// A BLPOP that is waiting for an element used to notice nothing about its
	// client. When the client had gone, the next push woke it, it popped the
	// element, and the reply went nowhere: the element was lost, and until then
	// the connection and its goroutine were held.
	addr, stop, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	admin, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, admin)
	adminParser := protocol.NewParser(admin)

	disconnects := map[string]func(*net.TCPConn){
		"the client closes the connection": func(c *net.TCPConn) { _ = c.Close() },
		"the client half-closes its side":  func(c *net.TCPConn) { _ = c.CloseWrite() },
	}
	for name, disconnect := range disconnects {
		t.Run(name, func(t *testing.T) {
			raw, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Dial(%q) error = %v", addr, err)
			}
			blocked := raw.(*net.TCPConn)
			defer closeTestResource(t, blocked)
			if err := protocol.WriteValue(blocked, request("BLPOP", "jobs")); err != nil {
				t.Fatalf("WriteValue(BLPOP) error = %v", err)
			}
			waitForClients(t, admin, adminParser, 2, 2*time.Second, "the blocked client should be connected")

			disconnect(blocked)

			waitForClients(t, admin, adminParser, 1, 3*time.Second, "the server should drop a blocked BLPOP whose client has gone")

			// Nobody is waiting any more, so a push must stay in the list.
			assertCommandResponse(t, admin, adminParser, protocol.Integer{Value: 1}, "RPUSH", "jobs", "job-1")
			assertCommandResponse(t, admin, adminParser, protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: []byte("job-1")},
			}}, "LRANGE", "jobs", "0", "-1")
			assertCommandResponse(t, admin, adminParser, protocol.BulkString{Data: []byte("job-1")}, "LPOP", "jobs")
		})
	}
}

func TestBlockedBLPopStillWakesForAClientThatIsThere(t *testing.T) {
	addr, stop, errCh := startTestServer(t, defaultTestConfig())
	defer func() {
		stop()
		waitForServerStop(t, errCh)
	}()

	admin, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, admin)
	adminParser := protocol.NewParser(admin)

	blocked, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, blocked)
	blockedParser := protocol.NewParser(blocked)

	// A request pipelined behind the BLPOP sits unread in the socket while the
	// BLPOP waits. That is a client that is still there, not one that has gone.
	if _, err := blocked.Write([]byte("*2\r\n$5\r\nBLPOP\r\n$4\r\njobs\r\n*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	waitForClients(t, admin, adminParser, 2, 2*time.Second, "the blocked client should be connected")

	// Long enough for the server to have checked on the client several times.
	time.Sleep(600 * time.Millisecond)
	if got := connectedClients(t, admin, adminParser); got != 2 {
		t.Fatalf("connected_clients = %d after a blocked BLPOP with a pipelined request behind it, want 2", got)
	}

	assertCommandResponse(t, admin, adminParser, protocol.Integer{Value: 1}, "RPUSH", "jobs", "job-1")

	if err := blocked.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	first, err := blockedParser.Parse()
	if err != nil {
		t.Fatalf("Parse(BLPOP reply) error = %v", err)
	}
	assertValuesEqual(t, first, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("jobs")},
		protocol.BulkString{Data: []byte("job-1")},
	}})
	second, err := blockedParser.Parse()
	if err != nil {
		t.Fatalf("Parse(PING reply) error = %v", err)
	}
	assertValuesEqual(t, second, protocol.SimpleString{Value: "PONG"})
}
