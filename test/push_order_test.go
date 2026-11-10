package test

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/config"
	"github.com/maltemindedal/stash/internal/protocol"
)

// TestAMessagePublishedBeforeUnsubscribeArrivesBeforeItsReply races a PUBLISH
// against the subscriber's UNSUBSCRIBE. Whenever PUBLISH counts the subscriber,
// its message must reach the subscriber before the UNSUBSCRIBE reply: arriving
// after it, the message would be read as the reply to the PING that follows.
// The goroutine mode wrote the message after it had released the registry, so
// the reply could overtake it.
func TestAMessagePublishedBeforeUnsubscribeArrivesBeforeItsReply(t *testing.T) {
	forEachNetworkingMode(t, defaultTestConfig(), testAMessagePublishedBeforeUnsubscribeArrivesBeforeItsReply)
}

func testAMessagePublishedBeforeUnsubscribeArrivesBeforeItsReply(t *testing.T, cfg config.Config) {
	addr, stop, errCh := startTestServer(t, cfg)
	defer stop()

	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberConn)
	subscriberParser := protocol.NewParser(subscriberConn)

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	message := protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("news")},
		protocol.BulkString{Data: []byte("hello")},
	}}
	unsubscribeReply := protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "unsubscribe"},
		protocol.BulkString{Data: []byte("news")},
		protocol.Integer{Value: 0},
	}}

	const trials = 300
	for trial := 0; trial < trials; trial++ {
		if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "news")); err != nil {
			t.Fatalf("trial %d: WriteValue(SUBSCRIBE) error = %v", trial, err)
		}
		assertParsedValue(t, subscriberParser, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "subscribe"},
			protocol.BulkString{Data: []byte("news")},
			protocol.Integer{Value: 1},
		}})

		if err := protocol.WriteValue(publisherConn, request("PUBLISH", "news", "hello")); err != nil {
			t.Fatalf("trial %d: WriteValue(PUBLISH) error = %v", trial, err)
		}
		if err := protocol.WriteValue(subscriberConn, request("UNSUBSCRIBE", "news")); err != nil {
			t.Fatalf("trial %d: WriteValue(UNSUBSCRIBE) error = %v", trial, err)
		}

		messagesBeforeReply := 0
		for {
			got, err := subscriberParser.Parse()
			if err != nil {
				t.Fatalf("trial %d: Parse() subscriber error = %v", trial, err)
			}
			if isPubSubMessage(got) {
				assertValuesEqual(t, got, message)
				messagesBeforeReply++
				continue
			}
			assertValuesEqual(t, got, unsubscribeReply)
			break
		}
		assertCommandResponse(t, subscriberConn, subscriberParser, protocol.SimpleString{Value: "PONG"}, "PING")

		published, err := publisherParser.Parse()
		if err != nil {
			t.Fatalf("trial %d: Parse() PUBLISH reply error = %v", trial, err)
		}
		receivers, ok := published.(protocol.Integer)
		if !ok {
			t.Fatalf("trial %d: PUBLISH reply = %#v, want protocol.Integer", trial, published)
		}
		if receivers.Value != int64(messagesBeforeReply) {
			t.Fatalf("trial %d: PUBLISH reached %d subscribers, but the subscriber received %d messages before the UNSUBSCRIBE reply", trial, receivers.Value, messagesBeforeReply)
		}
	}

	stop()
	waitForServerStop(t, errCh)
}

// isPubSubMessage reports whether value is a pub/sub message frame rather than
// a reply.
func isPubSubMessage(value protocol.Value) bool {
	array, ok := value.(protocol.Array)
	if !ok || len(array.Elements) == 0 {
		return false
	}
	kind, _, ok := integrationBulkStringContent(array.Elements[0])
	return ok && kind == "message"
}

func assertParsedValue(t *testing.T, parser *protocol.Parser, want protocol.Value) {
	t.Helper()

	got, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	assertValuesEqual(t, got, want)
}

// TestNoMessageArrivesBeforeTheSubscribeAcknowledgement subscribes while
// another client publishes to the channel without pause. The first frame a
// subscriber reads must be its acknowledgement. The goroutine mode registered
// the subscriber before it queued the acknowledgement, so a message published
// in between was written first.
func TestNoMessageArrivesBeforeTheSubscribeAcknowledgement(t *testing.T) {
	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		addr, stop, errCh := startTestServer(t, cfg)
		defer func() {
			stop()
			waitForServerStop(t, errCh)
		}()

		publisherConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) publisher error = %v", addr, err)
		}
		done := make(chan struct{})
		var publisher sync.WaitGroup
		publisher.Add(1)
		go func() {
			defer publisher.Done()
			parser := protocol.NewParser(publisherConn)
			for {
				select {
				case <-done:
					return
				default:
				}
				if err := protocol.WriteValue(publisherConn, request("PUBLISH", "news", "hello")); err != nil {
					return
				}
				if _, err := parser.Parse(); err != nil {
					return
				}
			}
		}()
		defer func() {
			close(done)
			_ = publisherConn.Close()
			publisher.Wait()
		}()

		acknowledgement := protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "subscribe"},
			protocol.BulkString{Data: []byte("news")},
			protocol.Integer{Value: 1},
		}}
		const trials = 500
		for trial := 0; trial < trials; trial++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("trial %d: Dial(%q) error = %v", trial, addr, err)
			}
			if err := protocol.WriteValue(conn, request("SUBSCRIBE", "news")); err != nil {
				t.Fatalf("trial %d: WriteValue(SUBSCRIBE) error = %v", trial, err)
			}
			parser := protocol.NewParser(conn)
			first, err := parser.Parse()
			if err != nil {
				t.Fatalf("trial %d: Parse() error = %v", trial, err)
			}
			if isPubSubMessage(first) {
				t.Fatalf("trial %d: the subscriber's first frame is a message, want its SUBSCRIBE acknowledgement", trial)
			}
			assertValuesEqual(t, first, acknowledgement)
			// The publisher is still publishing, so a message follows.
			if next, err := parser.Parse(); err != nil || !isPubSubMessage(next) {
				t.Fatalf("trial %d: the frame after the acknowledgement = %#v (error %v), want a message", trial, next, err)
			}
			closeTestResource(t, conn)
		}
	})
}

// TestPublishToStalledSubscribersDoesNotHoldUpOtherClients publishes more to
// subscribers that read nothing than their sockets can take. PUBLISH only
// queues, so it keeps counting every subscriber, as Redis does, and another
// client is answered meanwhile. The goroutine mode wrote to each subscriber's
// socket from the publisher's request, holding the sequencer's gate: every
// stalled subscriber cost the publisher 100 ms and was then dropped, so the
// count fell to zero.
func TestPublishToStalledSubscribersDoesNotHoldUpOtherClients(t *testing.T) {
	forEachNetworkingMode(t, defaultTestConfig(), func(t *testing.T, cfg config.Config) {
		addr, stop, errCh := startTestServer(t, cfg)
		defer func() {
			stop()
			waitForServerStop(t, errCh)
		}()

		const subscribers = 3
		for i := 0; i < subscribers; i++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
			}
			defer closeTestResource(t, conn)
			parser := protocol.NewParser(conn)
			if err := protocol.WriteValue(conn, request("SUBSCRIBE", "news")); err != nil {
				t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
			}
			if _, err := parser.Parse(); err != nil {
				t.Fatalf("Parse() subscribe acknowledgement error = %v", err)
			}
			// From here on the subscriber reads nothing.
		}

		publisherConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) publisher error = %v", addr, err)
		}
		defer closeTestResource(t, publisherConn)
		publisherParser := protocol.NewParser(publisherConn)
		otherConn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
		defer closeTestResource(t, otherConn)
		otherParser := protocol.NewParser(otherConn)

		// 20 MiB for each subscriber: more than the sockets between here and a
		// peer that reads nothing can hold, and less than a subscriber's 32 MiB
		// Push limit.
		message := string(bytes.Repeat([]byte("m"), 1<<20))
		for sent := 0; sent < 20; sent++ {
			assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: subscribers}, "PUBLISH", "news", message)
			if err := protocol.WriteValue(otherConn, request("PING")); err != nil {
				t.Fatalf("WriteValue(PING) error = %v", err)
			}
			assertValuesEqual(t, readReplyWithin(t, otherConn, otherParser, 3*time.Second, "reply to PING beside a PUBLISH to stalled subscribers"), protocol.SimpleString{Value: "PONG"})
		}
	})
}
