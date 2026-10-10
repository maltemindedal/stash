package test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/command"
	"github.com/maltemindedal/stash/internal/config"
	stashlogger "github.com/maltemindedal/stash/internal/logger"
	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func defaultTestConfig() config.Config {
	cfg := config.Default()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.LogLevel = "error"
	cfg.EvictionInterval = 5 * time.Millisecond
	cfg.EvictionSampleSize = 10
	cfg.DumpPath = ""
	return cfg
}

// newServer builds a server from cfg with the command executor, as cmd/stash
// does.
func newServer(t *testing.T, cfg config.Config, logger *slog.Logger, store *storage.Store) *server.Server {
	t.Helper()

	srv, err := server.New(cfg, logger, store, newCommandExecutor)
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	return srv
}

// newCommandExecutor is command.New in the form server.New takes, as cmd/stash
// adapts it.
func newCommandExecutor(services server.Services) (server.CommandExecutor, error) {
	return command.New(services)
}

func startTestServer(t *testing.T, cfg config.Config) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	return startTestServerWithLogger(t, cfg, stashlogger.New(cfg.LogLevel))
}

// startTestServerWithLogger is startTestServer for a test that reads the server's
// log output.
func startTestServerWithLogger(t *testing.T, cfg config.Config, logger *slog.Logger) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	store := storage.NewStore()
	srv := newServer(t, cfg, logger, store)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	return waitForAddr(t, srv), cancel, errCh
}

func waitForServerStop(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop within timeout")
	}
}

func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()

	if err := resource.Close(); err != nil {
		t.Logf("failed to close test resource: %v", err)
	}
}

func TestServerHandlesPhaseOneCommands(t *testing.T) {
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
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("failed to close test connection: %v", closeErr)
		}
	}()

	parser := protocol.NewParser(conn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("hello")}, "ECHO", "hello")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "name", "Stash")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("Stash")}, "GET", "name")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "DEL", "name")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "name")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "INCR", "counter")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "bad", "hello")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR value is not an integer or out of range"}, "INCR", "bad")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "SETBIT", "bits", "16", "1")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "GETBIT", "bits", "16")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "BITCOUNT", "bits")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "PFADD", "visitors", "alice", "bob")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 0}, "PFADD", "visitors", "alice")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "PFCOUNT", "visitors")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "WRONGTYPE Key is not a valid HyperLogLog string value."}, "PFADD", "bad", "x")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "temp", "1", "PX", "15")

	time.Sleep(25 * time.Millisecond)
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "temp")

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

func TestServerRequiresAuthWhenConfigured(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.RequirePass = "secret"

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
	authedConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) authed client error = %v", addr, err)
	}
	defer closeTestResource(t, authedConn)
	authedParser := protocol.NewParser(authedConn)

	blockedConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) blocked client error = %v", addr, err)
	}
	defer closeTestResource(t, blockedConn)
	blockedParser := protocol.NewParser(blockedConn)

	assertCommandResponse(t, authedConn, authedParser, protocol.SimpleString{Value: "PONG"}, "PING")
	assertCommandResponse(t, authedConn, authedParser, protocol.BulkString{Data: []byte("hello")}, "PING", "hello")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "SET", "name", "Stash")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "GET", "name")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "MULTI")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "SUBSCRIBE", "news")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "WRONGPASS invalid username-password pair or user is disabled."}, "AUTH", "wrong")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "GET", "name")
	assertCommandResponse(t, authedConn, authedParser, protocol.SimpleString{Value: "OK"}, "AUTH", "secret")
	assertCommandResponse(t, authedConn, authedParser, protocol.SimpleString{Value: "OK"}, "SET", "name", "Stash")
	assertCommandResponse(t, authedConn, authedParser, protocol.BulkString{Data: []byte("Stash")}, "GET", "name")
	assertCommandResponse(t, authedConn, authedParser, protocol.ErrorValue{Message: "WRONGPASS invalid username-password pair or user is disabled."}, "AUTH", "wrong")
	assertCommandResponse(t, authedConn, authedParser, protocol.BulkString{Data: []byte("Stash")}, "GET", "name")
	assertCommandResponse(t, blockedConn, blockedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "REPLCONF", "listening-port", "6380")
	assertCommandResponse(t, blockedConn, blockedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "PSYNC", "?", "-1")
	assertCommandResponse(t, blockedConn, blockedParser, protocol.ErrorValue{Message: "NOAUTH Authentication required."}, "GET", "name")

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

func TestServerHandlesListCommands(t *testing.T) {
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

	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 2}, "LPUSH", "letters", "a", "b")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 3}, "RPUSH", "letters", "c")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("b")},
		protocol.BulkString{Data: []byte("a")},
		protocol.BulkString{Data: []byte("c")},
	}}, "LRANGE", "letters", "0", "-1")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "WRONGTYPE Operation against a key holding the wrong kind of value"}, "GET", "letters")

	blockedConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) blocked client error = %v", addr, err)
	}
	defer closeTestResource(t, blockedConn)
	blockedParser := protocol.NewParser(blockedConn)

	if err := protocol.WriteValue(blockedConn, request("BLPOP", "jobs")); err != nil {
		t.Fatalf("WriteValue(BLPOP) error = %v", err)
	}

	pushErrCh := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := protocol.WriteValue(conn, request("RPUSH", "jobs", "build")); err != nil {
			pushErrCh <- err
			return
		}

		response, err := parser.Parse()
		if err != nil {
			pushErrCh <- err
			return
		}

		if integer, ok := response.(protocol.Integer); !ok || integer.Value != 1 {
			pushErrCh <- net.InvalidAddrError("unexpected RPUSH response")
			return
		}

		pushErrCh <- nil
	}()

	got, err := blockedParser.Parse()
	if err != nil {
		t.Fatalf("Parse() BLPOP error = %v", err)
	}
	assertValuesEqual(t, got, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("jobs")},
		protocol.BulkString{Data: []byte("build")},
	}})
	if err := <-pushErrCh; err != nil {
		t.Fatalf("RPUSH during BLPOP wake-up error = %v", err)
	}

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

func TestServerHandlesSortedSetCommands(t *testing.T) {
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

	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 3}, "ZADD", "leaders", "2", "beta", "1", "alpha", "2", "aardvark")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("alpha")},
		protocol.BulkString{Data: []byte("aardvark")},
		protocol.BulkString{Data: []byte("beta")},
	}}, "ZRANGE", "leaders", "0", "-1")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("alpha")},
		protocol.BulkString{Data: []byte("1")},
		protocol.BulkString{Data: []byte("aardvark")},
		protocol.BulkString{Data: []byte("2")},
		protocol.BulkString{Data: []byte("beta")},
		protocol.BulkString{Data: []byte("2")},
	}}, "ZRANGE", "leaders", "0", "-1", "WITHSCORES")
	assertCommandResponse(t, conn, parser, protocol.Integer{Value: 1}, "ZADD", "leaders", "0.5", "beta", "3", "gamma")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.BulkString{Data: []byte("beta")},
		protocol.BulkString{Data: []byte("0.5")},
		protocol.BulkString{Data: []byte("alpha")},
		protocol.BulkString{Data: []byte("1")},
		protocol.BulkString{Data: []byte("aardvark")},
		protocol.BulkString{Data: []byte("2")},
		protocol.BulkString{Data: []byte("gamma")},
		protocol.BulkString{Data: []byte("3")},
	}}, "ZRANGE", "leaders", "0", "-1", "WITHSCORES")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "SET", "plain", "hello")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "WRONGTYPE Operation against a key holding the wrong kind of value"}, "ZADD", "plain", "1", "alpha")

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

func TestServerHandlesStreamCommands(t *testing.T) {
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

	assertCommandResponse(t, conn, parser, protocol.BulkString{Data: []byte("1-0")}, "XADD", "events", "1-0", "type", "start")

	if err := protocol.WriteValue(conn, request("XADD", "events", "*", "type", "finish", "user", "42")); err != nil {
		t.Fatalf("WriteValue(XADD *) error = %v", err)
	}
	secondRaw, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() XADD * error = %v", err)
	}
	secondID, ok := secondRaw.(protocol.BulkString)
	if !ok {
		t.Fatalf("XADD * response type = %T, want protocol.BulkString", secondRaw)
	}

	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("events")},
			protocol.Array{Elements: []protocol.Value{
				protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("1-0")},
					protocol.Array{Elements: []protocol.Value{
						protocol.BulkString{Data: []byte("type")},
						protocol.BulkString{Data: []byte("start")},
					}},
				}},
				protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: secondID.Data},
					protocol.Array{Elements: []protocol.Value{
						protocol.BulkString{Data: []byte("type")},
						protocol.BulkString{Data: []byte("finish")},
						protocol.BulkString{Data: []byte("user")},
						protocol.BulkString{Data: []byte("42")},
					}},
				}},
			}},
		}},
	}}, "XREAD", "STREAMS", "events", "0-0")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{}}, "XREAD", "STREAMS", "events", "$")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR invalid stream ID specified as stream command argument"}, "XADD", "events", "bad-id", "field", "value")

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

func TestServerHandlesTransactionCommands(t *testing.T) {
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

	otherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) second client error = %v", addr, err)
	}
	defer closeTestResource(t, otherConn)
	otherParser := protocol.NewParser(otherConn)

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "name", "1")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "INCR", "name")
	assertCommandResponse(t, otherConn, otherParser, protocol.BulkString{Null: true}, "GET", "name")
	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.SimpleString{Value: "OK"},
		protocol.Integer{Value: 2},
	}}, "EXEC")
	assertCommandResponse(t, otherConn, otherParser, protocol.BulkString{Data: []byte("2")}, "GET", "name")

	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "temp", "discarded")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "DISCARD")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "temp")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR EXEC without MULTI"}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR unknown command \"NOPE\""}, "NOPE")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "QUEUED"}, "SET", "after-abort", "1")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "EXECABORT Transaction discarded because of previous errors."}, "EXEC")
	assertCommandResponse(t, conn, parser, protocol.BulkString{Null: true}, "GET", "after-abort")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR invalid stream ID specified as stream command argument"}, "XADD", "events", "bad-id", "field", "value")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "EXECABORT Transaction discarded because of previous errors."}, "EXEC")

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

func TestServerHandlesWatchOptimisticLocking(t *testing.T) {
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
	watcherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) watcher error = %v", addr, err)
	}
	defer closeTestResource(t, watcherConn)
	watcherParser := protocol.NewParser(watcherConn)

	writerConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) writer error = %v", addr, err)
	}
	defer closeTestResource(t, writerConn)
	writerParser := protocol.NewParser(writerConn)

	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "balance")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "balance", "2")
	assertCommandResponse(t, writerConn, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "balance", "1")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.Array{Null: true}, "EXEC")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.BulkString{Data: []byte("1")}, "GET", "balance")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "balance")
	assertCommandResponse(t, writerConn, writerParser, protocol.SimpleString{Value: "OK"}, "SET", "balance", "3")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "QUEUED"}, "SET", "balance", "4")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.Array{Null: true}, "EXEC")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.BulkString{Data: []byte("3")}, "GET", "balance")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "WATCH", "balance")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "MULTI")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.ErrorValue{Message: "ERR WATCH inside MULTI is not allowed"}, "WATCH", "balance")
	assertCommandResponse(t, watcherConn, watcherParser, protocol.SimpleString{Value: "OK"}, "DISCARD")

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

func TestServerHandlesPubSubCommands(t *testing.T) {
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

	if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "updates")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	got, err := subscriberParser.Parse()
	if err != nil {
		t.Fatalf("Parse() SUBSCRIBE ack error = %v", err)
	}
	assertValuesEqual(t, got, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}})

	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 1}, "PUBLISH", "updates", "hello")
	message, err := subscriberParser.Parse()
	if err != nil {
		t.Fatalf("Parse() pushed pubsub message error = %v", err)
	}
	assertValuesEqual(t, message, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

	assertCommandResponse(t, subscriberConn, subscriberParser, protocol.BulkString{Data: []byte("still-here")}, "PING", "still-here")
	assertCommandResponse(t, subscriberConn, subscriberParser, protocol.ErrorValue{Message: "ERR only PING, SUBSCRIBE, and UNSUBSCRIBE are allowed in this context"}, "GET", "updates")

	if err := protocol.WriteValue(subscriberConn, request("UNSUBSCRIBE")); err != nil {
		t.Fatalf("WriteValue(UNSUBSCRIBE) error = %v", err)
	}
	got, err = subscriberParser.Parse()
	if err != nil {
		t.Fatalf("Parse() UNSUBSCRIBE ack error = %v", err)
	}
	assertValuesEqual(t, got, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "unsubscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 0},
	}})

	assertCommandResponse(t, subscriberConn, subscriberParser, protocol.SimpleString{Value: "OK"}, "SET", "updates", "value")
	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 0}, "PUBLISH", "updates", "bye")

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

func TestServerPubSubPublishesToEverySubscriber(t *testing.T) {
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
	subscriberOneConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber one error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberOneConn)
	subscriberOneParser := protocol.NewParser(subscriberOneConn)

	subscriberTwoConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber two error = %v", addr, err)
	}
	defer closeTestResource(t, subscriberTwoConn)
	subscriberTwoParser := protocol.NewParser(subscriberTwoConn)

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	assertCommandResponse(t, subscriberOneConn, subscriberOneParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}}, "SUBSCRIBE", "updates")
	assertCommandResponse(t, subscriberTwoConn, subscriberTwoParser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}}, "SUBSCRIBE", "updates")

	assertCommandResponse(t, publisherConn, publisherParser, protocol.Integer{Value: 2}, "PUBLISH", "updates", "hello")

	messageOne, err := subscriberOneParser.Parse()
	if err != nil {
		t.Fatalf("Parse() subscriber one message error = %v", err)
	}
	assertValuesEqual(t, messageOne, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

	messageTwo, err := subscriberTwoParser.Parse()
	if err != nil {
		t.Fatalf("Parse() subscriber two message error = %v", err)
	}
	assertValuesEqual(t, messageTwo, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "message"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.BulkString{Data: []byte("hello")},
	}})

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

func TestServerSubscribedClientsRejectTransactionCommands(t *testing.T) {
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

	assertCommandResponse(t, conn, parser, protocol.Array{Elements: []protocol.Value{
		protocol.TextBulkString{Value: "subscribe"},
		protocol.BulkString{Data: []byte("updates")},
		protocol.Integer{Value: 1},
	}}, "SUBSCRIBE", "updates")

	blocked := protocol.ErrorValue{Message: "ERR only PING, SUBSCRIBE, and UNSUBSCRIBE are allowed in this context"}
	assertCommandResponse(t, conn, parser, blocked, "WATCH", "updates")
	assertCommandResponse(t, conn, parser, blocked, "MULTI")
	assertCommandResponse(t, conn, parser, blocked, "EXEC")
	assertCommandResponse(t, conn, parser, blocked, "DISCARD")

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

func TestServerPubSubDisconnectCleanup(t *testing.T) {
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
	subscriberConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) subscriber error = %v", addr, err)
	}
	subscriberParser := protocol.NewParser(subscriberConn)
	if err := protocol.WriteValue(subscriberConn, request("SUBSCRIBE", "updates")); err != nil {
		t.Fatalf("WriteValue(SUBSCRIBE) error = %v", err)
	}
	if _, err := subscriberParser.Parse(); err != nil {
		t.Fatalf("Parse() SUBSCRIBE ack error = %v", err)
	}
	if err := subscriberConn.Close(); err != nil {
		t.Fatalf("Close() subscriber error = %v", err)
	}

	publisherConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) publisher error = %v", addr, err)
	}
	defer closeTestResource(t, publisherConn)
	publisherParser := protocol.NewParser(publisherConn)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("subscriber cleanup did not complete before timeout")
		}

		if err := protocol.WriteValue(publisherConn, request("PUBLISH", "updates", "hello")); err != nil {
			t.Fatalf("WriteValue(PUBLISH) error = %v", err)
		}
		got, err := publisherParser.Parse()
		if err != nil {
			t.Fatalf("Parse() PUBLISH response error = %v", err)
		}
		integer, ok := got.(protocol.Integer)
		if !ok {
			t.Fatalf("PUBLISH response type = %T, want protocol.Integer", got)
		}
		if integer.Value == 0 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

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

func TestServerMonitorStreamsCommands(t *testing.T) {
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
	monitorConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) monitor error = %v", addr, err)
	}
	defer closeTestResource(t, monitorConn)
	monitorParser := protocol.NewParser(monitorConn)
	assertCommandResponse(t, monitorConn, monitorParser, protocol.SimpleString{Value: "OK"}, "MONITOR")

	clientConn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial(%q) client error = %v", addr, err)
	}
	defer closeTestResource(t, clientConn)
	clientParser := protocol.NewParser(clientConn)

	assertCommandResponse(t, clientConn, clientParser, protocol.SimpleString{Value: "OK"}, "SET", "observed", "value")
	message, err := monitorParser.Parse()
	if err != nil {
		t.Fatalf("Parse() monitor message error = %v", err)
	}
	line, ok := message.(protocol.SimpleString)
	if !ok {
		t.Fatalf("monitor message type = %T, want SimpleString", message)
	}
	for _, want := range []string{"\"SET\"", "\"observed\"", "\"value\""} {
		if !strings.Contains(line.Value, want) {
			t.Fatalf("monitor line = %q, missing %q", line.Value, want)
		}
	}

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

func TestServerRejectsEmptyPubSubChannelNames(t *testing.T) {
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

	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR syntax error"}, "SUBSCRIBE", "")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR syntax error"}, "PUBLISH", "", "hello")
	assertCommandResponse(t, conn, parser, protocol.ErrorValue{Message: "ERR syntax error"}, "UNSUBSCRIBE", "")
	assertCommandResponse(t, conn, parser, protocol.SimpleString{Value: "PONG"}, "PING")

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

func waitForAddr(t *testing.T, srv *server.Server) string {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("SplitHostPort(%q) error = %v", addr, err)
			}
			return net.JoinHostPort("127.0.0.1", port)
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("server address not available before timeout")
	return ""
}

func assertCommandResponse(t *testing.T, conn net.Conn, parser *protocol.Parser, want protocol.Value, parts ...string) {
	t.Helper()

	if err := protocol.WriteValue(conn, request(parts...)); err != nil {
		t.Fatalf("WriteValue(%v) error = %v", parts, err)
	}

	got, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	assertValuesEqual(t, got, want)
}

func request(parts ...string) protocol.Value {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	return protocol.Array{Elements: elements}
}

func assertValuesEqual(t *testing.T, got protocol.Value, want protocol.Value) {
	t.Helper()

	switch typedWant := want.(type) {
	case protocol.SimpleString:
		typedGot, ok := got.(protocol.SimpleString)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("simple string = %q, want %q", typedGot.Value, typedWant.Value)
		}
	case protocol.BulkString:
		gotText, gotNull, ok := integrationBulkStringContent(got)
		if !ok {
			t.Fatalf("response type = %T, want bulk-string-compatible type", got)
		}
		if gotNull != typedWant.Null {
			t.Fatalf("bulk null = %v, want %v", gotNull, typedWant.Null)
		}
		if gotText != string(typedWant.Data) {
			t.Fatalf("bulk string = %q, want %q", gotText, string(typedWant.Data))
		}
	case protocol.TextBulkString:
		gotText, gotNull, ok := integrationBulkStringContent(got)
		if !ok {
			t.Fatalf("response type = %T, want bulk-string-compatible type", got)
		}
		if gotNull != typedWant.Null {
			t.Fatalf("bulk null = %v, want %v", gotNull, typedWant.Null)
		}
		if gotText != typedWant.Value {
			t.Fatalf("bulk string = %q, want %q", gotText, typedWant.Value)
		}
	case protocol.Integer:
		typedGot, ok := got.(protocol.Integer)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("integer = %d, want %d", typedGot.Value, typedWant.Value)
		}
	case protocol.ErrorValue:
		typedGot, ok := got.(protocol.ErrorValue)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Message != typedWant.Message {
			t.Fatalf("error message = %q, want %q", typedGot.Message, typedWant.Message)
		}
	case protocol.Array:
		typedGot, ok := got.(protocol.Array)
		if !ok {
			t.Fatalf("response type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null {
			t.Fatalf("array null = %v, want %v", typedGot.Null, typedWant.Null)
		}
		if len(typedGot.Elements) != len(typedWant.Elements) {
			t.Fatalf("len(array) = %d, want %d", len(typedGot.Elements), len(typedWant.Elements))
		}
		for i := range typedWant.Elements {
			assertValuesEqual(t, typedGot.Elements[i], typedWant.Elements[i])
		}
	default:
		t.Fatalf("unsupported wanted type %T", want)
	}
}

func integrationBulkStringContent(value protocol.Value) (string, bool, bool) {
	switch typed := value.(type) {
	case protocol.BulkString:
		return string(typed.Data), typed.Null, true
	case protocol.TextBulkString:
		return typed.Value, typed.Null, true
	default:
		return "", false, false
	}
}
