package test

import (
	"context"
	"net"
	"sort"
	"testing"
	"time"

	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestServerHandlesHashSetAndListPopCommands(t *testing.T) {
	cfg := defaultTestConfig()

	logger := stashlogger.New(cfg.LogLevel)
	store := storage.NewStore()
	srv := newServer(t, cfg, logger, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	addr := waitForAddr(t, srv)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", addr, err)
	}
	defer closeTestResource(t, conn)
	parser := protocol.NewParser(conn)

	// LPOP / RPOP (single + count form)
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 3}, "RPUSH", "letters", "a", "b", "c")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("a")}, "LPOP", "letters")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("c")}, "RPOP", "letters")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("b")},
	}}, "LPOP", "letters", "5")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "LPOP", "letters")
	assertCommandResponse(t, conn, parser, protocol.Array{Null: true}, "LPOP", "letters", "2")

	// HSET / HGET / HDEL / HGETALL
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "HSET", "h", "f1", "v1", "f2", "v2")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("v1")}, "HGET", "h", "f1")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "HGET", "h", "missing")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "HSET", "h", "f1", "updated")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("updated")}, "HGET", "h", "f1")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "HDEL", "h", "f1", "missing")

	// HGETALL order-independent check
	if err := protocol.WriteValue(conn, request("HGETALL", "h")); err != nil {
		t.Fatalf("WriteValue(HGETALL) error = %v", err)
	}
	got, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() HGETALL error = %v", err)
	}
	array, ok := got.(protocol.Array)
	if !ok || len(array.Elements) != 2 {
		t.Fatalf("HGETALL = %+v, want 2-element array", got)
	}
	pairs := collectBulkStrings(t, array)
	if len(pairs) != 2 || pairs[0] != "f2" || pairs[1] != "v2" {
		t.Fatalf("HGETALL pairs = %v, want [f2 v2]", pairs)
	}

	// SADD / SISMEMBER / SREM / SMEMBERS
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 3}, "SADD", "s", "a", "b", "c")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "SADD", "s", "a")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "SISMEMBER", "s", "a")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "SISMEMBER", "s", "z")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "SREM", "s", "a", "missing")

	if err := protocol.WriteValue(conn, request("SMEMBERS", "s")); err != nil {
		t.Fatalf("WriteValue(SMEMBERS) error = %v", err)
	}
	got, err = parser.Parse()
	if err != nil {
		t.Fatalf("Parse() SMEMBERS error = %v", err)
	}
	members := collectBulkStrings(t, got)
	sort.Strings(members)
	want := []string{"b", "c"}
	if len(members) != len(want) || members[0] != want[0] || members[1] != want[1] {
		t.Fatalf("SMEMBERS = %v, want %v", members, want)
	}

	// WRONGTYPE enforcement across types
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "plain", "x")
	wrongType := protocol.ErrorValue{Message: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	assertCommandResponse(t, conn, parser, wrongType, "LPOP", "plain")
	assertCommandResponse(t, conn, parser, wrongType, "RPOP", "plain")
	assertCommandResponse(t, conn, parser, wrongType, "HSET", "plain", "f", "v")
	assertCommandResponse(t, conn, parser, wrongType, "HGET", "plain", "f")
	assertCommandResponse(t, conn, parser, wrongType, "HDEL", "plain", "f")
	assertCommandResponse(t, conn, parser, wrongType, "HGETALL", "plain")
	assertCommandResponse(t, conn, parser, wrongType, "SADD", "plain", "m")
	assertCommandResponse(t, conn, parser, wrongType, "SISMEMBER", "plain", "m")
	assertCommandResponse(t, conn, parser, wrongType, "SREM", "plain", "m")
	assertCommandResponse(t, conn, parser, wrongType, "SMEMBERS", "plain")

	// Hash on list / set on hash
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "HSET", "ohash", "f", "v")
	assertCommandResponse(t, conn, parser, wrongType, "GET", "ohash")
	assertCommandResponse(t, conn, parser, wrongType, "SADD", "ohash", "m")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "SADD", "oset", "m")
	assertCommandResponse(t, conn, parser, wrongType, "HGET", "oset", "f")
	assertCommandResponse(t, conn, parser, wrongType, "LPOP", "oset")

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop within timeout")
	}
}

func collectBulkStrings(t *testing.T, value protocol.Value) []string {
	t.Helper()
	array, ok := value.(protocol.Array)
	if !ok {
		t.Fatalf("expected array, got %T", value)
	}
	out := make([]string, 0, len(array.Elements))
	for _, el := range array.Elements {
		text, _, ok := integrationBulkStringContent(el)
		if !ok {
			t.Fatalf("element %T is not a bulk string", el)
		}
		out = append(out, text)
	}
	return out
}
