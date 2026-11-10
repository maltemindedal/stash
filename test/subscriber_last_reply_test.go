package test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
)

// TestASubscriberBehindOnItsMessagesStillReceivesItsLastReplies has a
// subscriber fall megabytes behind on its messages, send one more request, and
// then end its requests: it shuts its write side, or it sends a frame that
// cannot be parsed. It then reads to the end of the stream, pausing between
// reads. The server writes what it queued for the subscriber before it closes
// the connection: every message published before the request, the reply to
// the request, and the error reply to the frame.
//
// In the default networking mode the pump writes the messages. The
// connection's goroutine used to leave its replies to the pump and close the
// connection, which drops what is queued, once no more than 1 MiB was waiting,
// and for a time however much was. The subscriber lost the end of its messages
// and its replies. The last message here is half a megabyte and the subscriber
// reads slowly, so that the pump is still waiting on the socket with most of
// the last megabyte when the server reads the end of the subscriber's
// requests.
func TestASubscriberBehindOnItsMessagesStillReceivesItsLastReplies(t *testing.T) {
	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		tests := []struct {
			name string
			// unparsable is sent after the PING, in place of shutting the
			// subscriber's write side.
			unparsable string
		}{
			{name: "it shuts its write side after a PING"},
			{name: "it sends a frame that cannot be parsed after a PING", unparsable: "!garbage\r\n"},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				addr, stop, errCh := startTestServer(t, cfg)
				defer func() {
					stop()
					waitForServerStop(t, errCh)
				}()

				subscriber, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
				}
				defer closeTestResource(t, subscriber)
				if err := subscriber.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
					t.Fatalf("SetDeadline() error = %v", err)
				}
				// A small receive buffer, so that the server's writes follow the
				// subscriber's reads closely.
				if err := subscriber.(*net.TCPConn).SetReadBuffer(64 << 10); err != nil {
					t.Fatalf("SetReadBuffer() error = %v", err)
				}
				if err := protocol.WriteValue(subscriber, request("SUBSCRIBE", "news")); err != nil {
					t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
				}
				subscribed := "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
				acknowledgement := make([]byte, len(subscribed))
				if _, err := io.ReadFull(subscriber, acknowledgement); err != nil || string(acknowledgement) != subscribed {
					t.Fatalf("the SUBSCRIBE reply = %q, %v, want %q", acknowledgement, err, subscribed)
				}

				// More than the socket buffers between the two take: the server
				// is left with megabytes that the subscriber's socket has not
				// taken.
				payloads := make([]string, 0, 17)
				for i := 0; i < 16; i++ {
					payloads = append(payloads, strings.Repeat("m", 1<<20))
				}
				payloads = append(payloads, strings.Repeat("l", 512<<10))
				publisher, publisherParser := dialClient(t, addr)
				for _, payload := range payloads {
					assertCommandResponse(t, publisher, publisherParser, protocol.Integer{Value: 1}, "PUBLISH", "news", payload)
				}

				if err := protocol.WriteValue(subscriber, request("PING")); err != nil {
					t.Fatalf("WriteValue(PING) error = %v", err)
				}
				if tt.unparsable != "" {
					if _, err := io.WriteString(subscriber, tt.unparsable); err != nil {
						t.Fatalf("Write(%q) error = %v", tt.unparsable, err)
					}
				} else if err := subscriber.(*net.TCPConn).CloseWrite(); err != nil {
					t.Fatalf("CloseWrite() error = %v", err)
				}

				var received bytes.Buffer
				for {
					_, err := io.CopyN(&received, subscriber, 64<<10)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatalf("reading to the end of the stream, after %d bytes: %v", received.Len(), err)
					}
					time.Sleep(time.Millisecond)
				}

				parser := protocol.NewParser(&received)
				for i, payload := range payloads {
					got, err := parser.Parse()
					if err != nil {
						t.Fatalf("the subscriber received %d of the %d messages published before its PING, and then: %v", i, len(payloads), err)
					}
					assertValuesEqual(t, got, protocol.Array{Elements: []protocol.Value{
						protocol.TextBulkString{Value: "message"},
						protocol.BulkString{Data: []byte("news")},
						protocol.BulkString{Data: []byte(payload)},
					}})
				}
				pong, err := parser.Parse()
				if err != nil {
					t.Fatalf("the subscriber received every message and no reply to its PING: %v", err)
				}
				assertValuesEqual(t, pong, protocol.SimpleString{Value: "PONG"})
				if tt.unparsable != "" {
					refusal, err := parser.Parse()
					if err != nil {
						t.Fatalf("the subscriber received the reply to its PING and no reply to %q: %v", tt.unparsable, err)
					}
					if reply, ok := refusal.(protocol.ErrorValue); !ok || !strings.HasPrefix(reply.Message, "ERR ") {
						t.Fatalf("the reply to %q = %#v, want an ERR reply", tt.unparsable, refusal)
					}
				}
				if received.Len() != 0 {
					t.Fatalf("the subscriber received %d more bytes after its last reply, want the end of the stream", received.Len())
				}
			})
		}
	})
}
