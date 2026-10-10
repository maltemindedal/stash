package command

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func TestExecutorExecute(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*testing.T, *Executor)
		request protocol.Value
		wait    time.Duration
		assert  func(*testing.T, protocol.Value, error)
	}{
		{
			name:    "PING returns PONG",
			request: requestValue("PING"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.SimpleString{Value: "PONG"})
			},
		},
		{
			name:    "PING with message echoes as bulk string",
			request: requestValue("PING", "hello"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("hello")})
			},
		},
		{
			name:    "ECHO returns payload",
			request: requestValue("ECHO", "hello"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("hello")})
			},
		},
		{
			name: "GET returns stored value",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "name", "Stash")); err != nil {
					t.Fatalf("SET error = %v", err)
				}
			},
			request: requestValue("GET", "name"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte("Stash")})
			},
		},
		{
			name: "GET returns null after PX expiration",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "temp", "1", "PX", "10")); err != nil {
					t.Fatalf("SET error = %v", err)
				}
			},
			request: requestValue("GET", "temp"),
			wait:    20 * time.Millisecond,
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Null: true})
			},
		},
		{
			name: "GET rejects list values with wrong type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("RPUSH", "queue", "job-1")); err != nil {
					t.Fatalf("RPUSH error = %v", err)
				}
			},
			request: requestValue("GET", "queue"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name:    "GETBIT returns zero for missing keys",
			request: requestValue("GETBIT", "missing", "12"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name: "SETBIT sets sparse bits and returns previous bit",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				value, err := executor.Execute(context.Background(), requestValue("SETBIT", "bitmap", "16", "1"))
				if err != nil {
					t.Fatalf("SETBIT error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
			request: requestValue("GET", "bitmap"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.BulkString{Data: []byte{0, 0, 0x80}})
			},
		},
		{
			name: "SETBIT returns overwritten bit",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SETBIT", "bitmap", "7", "1")); err != nil {
					t.Fatalf("SETBIT setup error = %v", err)
				}
			},
			request: requestValue("SETBIT", "bitmap", "7", "0"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 1})
			},
		},
		{
			name: "GETBIT reads existing and unset bits",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SETBIT", "bitmap", "0", "1")); err != nil {
					t.Fatalf("SETBIT setup error = %v", err)
				}
			},
			request: requestValue("GETBIT", "bitmap", "1"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name: "BITCOUNT counts full and ranged strings",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				_, _ = executor.store.Set("bitmap", []byte{0xff, 0x00, 0x0f}, 0)
			},
			request: requestValue("BITCOUNT", "bitmap", "1", "2"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 4})
			},
		},
		{
			name:    "BITCOUNT returns zero for missing keys",
			request: requestValue("BITCOUNT", "missing"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name:    "SETBIT rejects invalid bit value",
			request: requestValue("SETBIT", "bitmap", "0", "2"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("Execute() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name:    "GETBIT rejects negative offset",
			request: requestValue("GETBIT", "bitmap", "-1"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("Execute() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name:    "BITCOUNT rejects invalid range argument",
			request: requestValue("BITCOUNT", "bitmap", "0", "bad"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("Execute() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name: "Bitmap commands reject wrong value type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("RPUSH", "queue", "job-1")); err != nil {
					t.Fatalf("RPUSH error = %v", err)
				}
			},
			request: requestValue("BITCOUNT", "queue"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name: "PFADD reports changed estimate and PFCOUNT counts unique elements",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				value, err := executor.Execute(context.Background(), requestValue("PFADD", "visitors", "alice", "bob", "alice"))
				if err != nil {
					t.Fatalf("PFADD error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 1})
			},
			request: requestValue("PFCOUNT", "visitors"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 2})
			},
		},
		{
			name: "PFADD returns zero for repeated elements",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("PFADD", "visitors", "alice")); err != nil {
					t.Fatalf("PFADD setup error = %v", err)
				}
			},
			request: requestValue("PFADD", "visitors", "alice"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name:    "PFCOUNT returns zero for missing keys",
			request: requestValue("PFCOUNT", "missing", "also-missing"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name: "PFCOUNT unions multiple HyperLogLog keys",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("PFADD", "morning", "alice", "bob")); err != nil {
					t.Fatalf("PFADD morning error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("PFADD", "evening", "bob", "carol")); err != nil {
					t.Fatalf("PFADD evening error = %v", err)
				}
			},
			request: requestValue("PFCOUNT", "morning", "evening"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 3})
			},
		},
		{
			name:    "PFADD rejects missing key argument",
			request: requestValue("PFADD"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), "wrong number of arguments") {
					t.Fatalf("Execute() error = %v, want wrong number of arguments", err)
				}
			},
		},
		{
			name: "HyperLogLog commands reject wrong value type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("RPUSH", "queue", "job-1")); err != nil {
					t.Fatalf("RPUSH error = %v", err)
				}
			},
			request: requestValue("PFADD", "queue", "alice"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name: "HyperLogLog commands reject plain string values",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "greeting", "hello")); err != nil {
					t.Fatalf("SET error = %v", err)
				}
			},
			request: requestValue("PFCOUNT", "greeting"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrNotHyperLogLog) {
					t.Fatalf("Execute() error = %v, want ErrNotHyperLogLog", err)
				}
			},
		},
		{
			name: "DEL removes existing keys and returns count",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("SET", "one", "1")); err != nil {
					t.Fatalf("SET one error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("SET", "two", "2")); err != nil {
					t.Fatalf("SET two error = %v", err)
				}
			},
			request: requestValue("DEL", "one", "missing", "two"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 2})
			},
		},
		{
			name: "DEL ignores expired keys",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				_, _ = executor.store.Set("expired", []byte("gone"), time.Now().Add(-time.Millisecond).UnixMilli())
			},
			request: requestValue("DEL", "expired"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 0})
			},
		},
		{
			name: "LPUSH and LRANGE return list contents",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("LPUSH", "letters", "a", "b")); err != nil {
					t.Fatalf("LPUSH error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("RPUSH", "letters", "c")); err != nil {
					t.Fatalf("RPUSH error = %v", err)
				}
			},
			request: requestValue("LRANGE", "letters", "0", "-1"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("b")},
					protocol.BulkString{Data: []byte("a")},
					protocol.BulkString{Data: []byte("c")},
				}})
			},
		},
		{
			name: "ZADD and ZRANGE WITHSCORES return sorted contents",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("ZADD", "leaders", "2", "beta", "1", "alpha", "2", "aardvark")); err != nil {
					t.Fatalf("ZADD error = %v", err)
				}
			},
			request: requestValue("ZRANGE", "leaders", "0", "-1", "WITHSCORES"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
					protocol.BulkString{Data: []byte("alpha")},
					protocol.BulkString{Data: []byte("1")},
					protocol.BulkString{Data: []byte("aardvark")},
					protocol.BulkString{Data: []byte("2")},
					protocol.BulkString{Data: []byte("beta")},
					protocol.BulkString{Data: []byte("2")},
				}})
			},
		},
		{
			name: "ZADD returns count of newly added members only",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("ZADD", "leaders", "1", "alpha")); err != nil {
					t.Fatalf("initial ZADD error = %v", err)
				}
			},
			request: requestValue("ZADD", "leaders", "2", "alpha", "3", "beta"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 1})
			},
		},
		{
			name:    "ZADD rejects invalid score",
			request: requestValue("ZADD", "leaders", "oops", "alpha"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotFloat) {
					t.Fatalf("Execute() error = %v, want ErrValueNotFloat", err)
				}
			},
		},
		{
			name: "XADD and XREAD return stream entries",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("XADD", "events", "1-0", "type", "start")); err != nil {
					t.Fatalf("first XADD error = %v", err)
				}
				if _, err := executor.Execute(context.Background(), requestValue("XADD", "events", "2-0", "type", "finish", "user", "42")); err != nil {
					t.Fatalf("second XADD error = %v", err)
				}
			},
			request: requestValue("XREAD", "STREAMS", "events", "0-0"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
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
								protocol.BulkString{Data: []byte("2-0")},
								protocol.Array{Elements: []protocol.Value{
									protocol.BulkString{Data: []byte("type")},
									protocol.BulkString{Data: []byte("finish")},
									protocol.BulkString{Data: []byte("user")},
									protocol.BulkString{Data: []byte("42")},
								}},
							}},
						}},
					}},
				}})
			},
		},
		{
			name:    "XREAD returns empty array for missing stream",
			request: requestValue("XREAD", "STREAMS", "missing", "0-0"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{}})
			},
		},
		{
			name:    "XADD rejects malformed IDs",
			request: requestValue("XADD", "events", "bad-id", "field", "value"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrInvalidStreamID) {
					t.Fatalf("Execute() error = %v, want ErrInvalidStreamID", err)
				}
			},
		},
		{
			name: "XADD rejects non monotonic explicit IDs",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				if _, err := executor.Execute(context.Background(), requestValue("XADD", "events", "1-0", "field", "value")); err != nil {
					t.Fatalf("initial XADD error = %v", err)
				}
			},
			request: requestValue("XADD", "events", "1-0", "field", "value"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrStreamIDTooSmall) {
					t.Fatalf("Execute() error = %v, want ErrStreamIDTooSmall", err)
				}
			},
		},
		{
			name: "XREAD rejects wrong value type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				_, _ = executor.store.Set("events", []byte("plain"), 0)
			},
			request: requestValue("XREAD", "STREAMS", "events", "0-0"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name:    "XREAD rejects invalid syntax",
			request: requestValue("XREAD", "COUNT", "2", "STREAMS", "events", "0-0"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrSyntax) {
					t.Fatalf("Execute() error = %v, want ErrSyntax", err)
				}
			},
		},
		{
			name:    "ZRANGE rejects invalid trailing option",
			request: requestValue("ZRANGE", "leaders", "0", "-1", "REV"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrSyntax) {
					t.Fatalf("Execute() error = %v, want ErrSyntax", err)
				}
			},
		},
		{
			name: "LPUSH rejects wrong value type",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				_, _ = executor.store.Set("letters", []byte("hello"), 0)
			},
			request: requestValue("LPUSH", "letters", "a"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrWrongType) {
					t.Fatalf("Execute() error = %v, want ErrWrongType", err)
				}
			},
		},
		{
			name:    "INCR initializes missing key",
			request: requestValue("INCR", "counter"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 1})
			},
		},
		{
			name: "INCR increments existing integer",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				_, _ = executor.store.Set("counter", []byte("41"), 0)
			},
			request: requestValue("INCR", "counter"),
			assert: func(t *testing.T, value protocol.Value, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
				assertValueEqual(t, value, protocol.Integer{Value: 42})
			},
		},
		{
			name: "INCR rejects non-integer strings",
			setup: func(t *testing.T, executor *Executor) {
				t.Helper()
				_, _ = executor.store.Set("counter", []byte("hello"), 0)
			},
			request: requestValue("INCR", "counter"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrValueNotInteger) {
					t.Fatalf("Execute() error = %v, want ErrValueNotInteger", err)
				}
			},
		},
		{
			name:    "unknown command returns error",
			request: requestValue("NOPE"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("Execute() error = nil, want unknown command error")
				}
				if got := err.Error(); got != "unknown command \"NOPE\"" {
					t.Fatalf("error = %q, want %q", got, "unknown command \"NOPE\"")
				}
			},
		},
		{
			name:    "SET with invalid PX returns sentinel error",
			request: requestValue("SET", "temp", "1", "PX", "0"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrInvalidExpireTime) {
					t.Fatalf("Execute() error = %v, want ErrInvalidExpireTime", err)
				}
			},
		},
		{
			name:    "SET with an EX that overflows returns the invalid-expire sentinel",
			request: requestValue("SET", "temp", "1", "EX", "9223372036854775807"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrInvalidExpireTime) {
					t.Fatalf("Execute() error = %v, want ErrInvalidExpireTime (the deadline used to wrap into the past)", err)
				}
			},
		},
		{
			name:    "SET with a PX that overflows returns the invalid-expire sentinel",
			request: requestValue("SET", "temp", "1", "PX", "9223372036854775807"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrInvalidExpireTime) {
					t.Fatalf("Execute() error = %v, want ErrInvalidExpireTime (the deadline used to wrap into the past)", err)
				}
			},
		},
		{
			name:    "SET with invalid option returns syntax sentinel",
			request: requestValue("SET", "temp", "1", "NX", "10"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if !errors.Is(err, ErrSyntax) {
					t.Fatalf("Execute() error = %v, want ErrSyntax", err)
				}
			},
		},
		{
			name:    "ECHO wrong number of arguments returns error",
			request: requestValue("ECHO"),
			assert: func(t *testing.T, _ protocol.Value, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("Execute() error = nil, want wrong-argument-count error")
				}
				if got := err.Error(); got != "wrong number of arguments for 'ECHO' command" {
					t.Fatalf("error = %q, want %q", got, "wrong number of arguments for 'ECHO' command")
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			executor := newTestExecutor()
			if tt.setup != nil {
				tt.setup(t, executor)
			}
			if tt.wait > 0 {
				time.Sleep(tt.wait)
			}

			value, err := executor.Execute(context.Background(), tt.request)
			tt.assert(t, value, err)
		})
	}
}

func TestExecutorDetailedPropagation(t *testing.T) {
	t.Run("SET returns propagation frame", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("SET", "name", "Stash"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.SimpleString{Value: "OK"})
		assertPropagationFrames(t, result.Propagation, requestValue("SET", "name", "Stash"))
	})

	t.Run("INCR returns propagation frame", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("INCR", "counter"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 1})
		assertPropagationFrames(t, result.Propagation, requestValue("INCR", "counter"))
	})

	t.Run("SETBIT returns propagation frame", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("SETBIT", "bitmap", "0", "1"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 0})
		assertPropagationFrames(t, result.Propagation, requestValue("SETBIT", "bitmap", "0", "1"))
		assertPropagationFrames(t, result.Durability, requestValue("SETBIT", "bitmap", "0", "1"))
	})

	t.Run("PFADD returns propagation frame", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("PFADD", "visitors", "alice"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 1})
		assertPropagationFrames(t, result.Propagation, requestValue("PFADD", "visitors", "alice"))
		assertPropagationFrames(t, result.Durability, requestValue("PFADD", "visitors", "alice"))
	})

	t.Run("PUBLISH returns propagation frame", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("PUBLISH", "news", "hello"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 0})
		assertPropagationFrames(t, result.Propagation, requestValue("PUBLISH", "news", "hello"))
	})

	t.Run("DEL propagates even when it removes no keys", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("DEL", "missing"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 0})
		assertPropagationFrames(t, result.Propagation, requestValue("DEL", "missing"))
	})

	t.Run("replication-origin commands do not re-propagate", func(t *testing.T) {
		executor := newTestExecutor()

		result, err := executor.ExecuteDetailed(server.WithReplicationOrigin(context.Background()), requestValue("PUBLISH", "news", "replica"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Propagation) != 0 {
			t.Fatalf("len(result.Propagation) = %d, want 0", len(result.Propagation))
		}
	})

	t.Run("EXEC aggregates child SET and DEL propagation", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "name", "Stash")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("DEL", "missing")); err != nil {
			t.Fatalf("queued DEL error = %v", err)
		}

		result, err := executor.ExecuteDetailed(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Array{Elements: []protocol.Value{
			protocol.SimpleString{Value: "OK"},
			protocol.Integer{Value: 0},
		}})
		assertPropagationFrames(t, result.Propagation,
			requestValue("SET", "name", "Stash"),
			requestValue("DEL", "missing"),
		)
		assertPropagationFrames(t, result.Durability,
			requestValue("SET", "name", "Stash"),
			requestValue("DEL", "missing"),
		)
	})

	t.Run("empty EXEC returns no propagation or durability", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 2)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}

		result, err := executor.ExecuteDetailed(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Array{Elements: []protocol.Value{}})
		if len(result.Propagation) != 0 {
			t.Fatalf("len(result.Propagation) = %d, want 0", len(result.Propagation))
		}
		if len(result.Durability) != 0 {
			t.Fatalf("len(result.Durability) = %d, want 0", len(result.Durability))
		}
	})

	t.Run("mixed-success EXEC only aggregates successful propagation and durability", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 3)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "bad", "hello")); err != nil {
			t.Fatalf("queued SET bad error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("INCR", "bad")); err != nil {
			t.Fatalf("queued INCR error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "good", "1")); err != nil {
			t.Fatalf("queued SET good error = %v", err)
		}

		result, err := executor.ExecuteDetailed(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}

		assertValueEqual(t, result.Responses[0], protocol.Array{Elements: []protocol.Value{
			protocol.SimpleString{Value: "OK"},
			protocol.ErrorValue{Message: "ERR value is not an integer or out of range"},
			protocol.SimpleString{Value: "OK"},
		}})
		assertPropagationFrames(t, result.Propagation,
			requestValue("SET", "bad", "hello"),
			requestValue("SET", "good", "1"),
		)
		assertPropagationFrames(t, result.Durability,
			requestValue("SET", "bad", "hello"),
			requestValue("SET", "good", "1"),
		)
	})
}

func TestExecutorMaxMemory(t *testing.T) {
	t.Run("SET returns OOM when an oversized protected update cannot fit", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(context.Background(), requestValue("SET", "item", strings.Repeat("a", 64))); err != nil {
			t.Fatalf("initial SET error = %v", err)
		}

		executor.store.ConfigureMaxMemory(1<<30, 16)
		baseline := executor.store.UsedMemory()
		executor.store.ConfigureMaxMemory(baseline, 16)

		if _, err := executor.Execute(context.Background(), requestValue("SET", "item", strings.Repeat("b", 4096))); !errors.Is(err, ErrOutOfMemory) {
			t.Fatalf("oversized SET error = %v, want ErrOutOfMemory", err)
		}

		value, err := executor.Execute(context.Background(), requestValue("GET", "item"))
		if err != nil {
			t.Fatalf("GET after OOM error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte(strings.Repeat("a", 64))})
	})

	t.Run("SET evicts stale keys and emits DEL side effects", func(t *testing.T) {
		executor := newTestExecutor()
		payload := strings.Repeat("x", 96)
		if _, err := executor.Execute(context.Background(), requestValue("SET", "cold", payload)); err != nil {
			t.Fatalf("initial SET error = %v", err)
		}

		executor.store.ConfigureMaxMemory(1<<30, 16)
		baseline := executor.store.UsedMemory()
		executor.store.ConfigureMaxMemory(baseline+baseline/2, 16)
		time.Sleep(2 * time.Millisecond)

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("SET", "hot!", payload))
		if err != nil {
			t.Fatalf("evicting SET error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.SimpleString{Value: "OK"})
		assertPropagationFrames(t, result.Propagation,
			requestValue("SET", "hot!", payload),
			requestValue("DEL", "cold"),
		)
		assertPropagationFrames(t, result.Durability,
			requestValue("SET", "hot!", payload),
			requestValue("DEL", "cold"),
		)

		cold, err := executor.Execute(context.Background(), requestValue("GET", "cold"))
		if err != nil {
			t.Fatalf("GET cold error = %v", err)
		}
		assertValueEqual(t, cold, protocol.BulkString{Null: true})

		hot, err := executor.Execute(context.Background(), requestValue("GET", "hot!"))
		if err != nil {
			t.Fatalf("GET hot error = %v", err)
		}
		assertValueEqual(t, hot, protocol.BulkString{Data: []byte(payload)})
	})
}

func TestExecutorInfo(t *testing.T) {
	executor := newTestExecutor()
	if _, err := executor.Execute(context.Background(), requestValue("SET", "name", "Stash")); err != nil {
		t.Fatalf("SET error = %v", err)
	}
	if _, err := executor.Execute(context.Background(), requestValue("LPUSH", "letters", "a")); err != nil {
		t.Fatalf("LPUSH error = %v", err)
	}

	value, err := executor.Execute(context.Background(), requestValue("INFO", "memory"))
	if err != nil {
		t.Fatalf("INFO memory error = %v", err)
	}
	text := mustBulkStringText(t, value)
	for _, want := range []string{"# Memory", "used_memory:", "mem_fragmentation_ratio:", "go_heap_alloc:", "key_count:2", "key_count_string:1", "key_count_list:1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("INFO memory = %q, missing %q", text, want)
		}
	}

	value, err = executor.Execute(context.Background(), requestValue("INFO"))
	if err != nil {
		t.Fatalf("INFO error = %v", err)
	}
	text = mustBulkStringText(t, value)
	for _, want := range []string{"# Memory", "# Replication", "# Clients", "role:master"} {
		if !strings.Contains(text, want) {
			t.Fatalf("INFO = %q, missing %q", text, want)
		}
	}
}

func TestExecutorSlowlog(t *testing.T) {
	t.Run("records commands at zero threshold", func(t *testing.T) {
		executor := newTestExecutor()
		registry := server.NewSlowlogRegistry()
		executor.SetSlowlogConfig(registry, 0)

		if _, err := executor.Execute(context.Background(), requestValue("PING")); err != nil {
			t.Fatalf("PING error = %v", err)
		}
		value, err := executor.Execute(context.Background(), requestValue("SLOWLOG", "LEN"))
		if err != nil {
			t.Fatalf("SLOWLOG LEN error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 1})

		value, err = executor.Execute(context.Background(), requestValue("SLOWLOG", "GET", "1"))
		if err != nil {
			t.Fatalf("SLOWLOG GET error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok || len(array.Elements) != 1 {
			t.Fatalf("SLOWLOG GET response = %#v, want one entry", value)
		}
	})

	t.Run("negative threshold disables recording", func(t *testing.T) {
		executor := newTestExecutor()
		registry := server.NewSlowlogRegistry()
		executor.SetSlowlogConfig(registry, -time.Microsecond)

		if _, err := executor.Execute(context.Background(), requestValue("PING")); err != nil {
			t.Fatalf("PING error = %v", err)
		}
		if got := registry.Len(); got != 0 {
			t.Fatalf("slowlog Len() = %d, want 0", got)
		}
	})

	t.Run("reset clears entries", func(t *testing.T) {
		executor := newTestExecutor()
		registry := server.NewSlowlogRegistry()
		executor.SetSlowlogConfig(registry, 0)

		if _, err := executor.Execute(context.Background(), requestValue("PING")); err != nil {
			t.Fatalf("PING error = %v", err)
		}
		if _, err := executor.Execute(context.Background(), requestValue("SLOWLOG", "RESET")); err != nil {
			t.Fatalf("SLOWLOG RESET error = %v", err)
		}
		// RESET itself is recorded at a zero threshold after clearing older entries.
		if got := registry.Len(); got != 1 {
			t.Fatalf("slowlog Len() = %d after RESET at zero threshold, want 1", got)
		}
	})

	t.Run("does not record commands rejected before dispatch", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		registry := server.NewSlowlogRegistry()
		executor.SetSlowlogConfig(registry, 0)
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 77)

		if _, err := executor.Execute(ctx, requestValue("SET", "name", "Stash")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("SET unauthenticated error = %v, want ErrNoAuth", err)
		}
		if got := registry.Len(); got != 0 {
			t.Fatalf("slowlog Len() = %d after rejected SET, want 0", got)
		}
	})

	t.Run("redacts sensitive command arguments", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		registry := server.NewSlowlogRegistry()
		executor.SetSlowlogConfig(registry, 0)
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 78)

		if _, err := executor.Execute(ctx, requestValue("AUTH", "secret")); err != nil {
			t.Fatalf("AUTH error = %v", err)
		}
		entries := registry.Entries(1)
		if len(entries) != 1 {
			t.Fatalf("len(slowlog entries) = %d, want 1", len(entries))
		}
		want := []string{"AUTH", "[redacted]"}
		if !reflect.DeepEqual(entries[0].Command, want) {
			t.Fatalf("slowlog command = %#v, want %#v", entries[0].Command, want)
		}
	})
}

func TestInfoPersistence(t *testing.T) {
	info := func(t *testing.T, executor *Executor, args ...string) string {
		t.Helper()

		value, err := executor.Execute(context.Background(), requestValue(append([]string{"INFO"}, args...)...))
		if err != nil {
			t.Fatalf("INFO %v error = %v", args, err)
		}
		return mustBulkStringText(t, value)
	}

	t.Run("no server behind the executor reports no AOF and no failure", func(t *testing.T) {
		text := info(t, newTestExecutor(), "persistence")
		if want := "# Persistence\r\naof_enabled:0\r\naof_last_write_status:ok\r\n"; text != want {
			t.Fatalf("INFO persistence = %q, want %q", text, want)
		}
	})

	t.Run("a failed AOF write is reported as err", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetServerStatsProvider(func() server.Stats {
			return server.Stats{Role: "master", AOFEnabled: true, AOFLastWriteOK: false}
		})

		text := info(t, executor, "persistence")
		for _, want := range []string{"aof_enabled:1", "aof_last_write_status:err"} {
			if !strings.Contains(text, want) {
				t.Fatalf("INFO persistence = %q, missing %q", text, want)
			}
		}
	})

	t.Run("the default INFO includes the section after the existing ones", func(t *testing.T) {
		text := info(t, newTestExecutor())
		clients := strings.Index(text, "# Clients")
		persistence := strings.Index(text, "# Persistence")
		if clients < 0 || persistence < clients {
			t.Fatalf("INFO = %q, want # Persistence after # Clients", text)
		}
	})
}

func TestSlowlogTruncatesLargeCommands(t *testing.T) {
	record := func(t *testing.T, args ...string) []string {
		t.Helper()

		executor := newTestExecutor()
		registry := server.NewSlowlogRegistry()
		executor.SetSlowlogConfig(registry, 0)
		if _, err := executor.Execute(context.Background(), requestValue(args...)); err != nil {
			t.Fatalf("%s error = %v", args[0], err)
		}
		entries := registry.Entries(1)
		if len(entries) != 1 {
			t.Fatalf("len(slowlog entries) = %d, want 1", len(entries))
		}
		return entries[0].Command
	}

	t.Run("an argument over 128 bytes keeps its first 128 and the count of the rest", func(t *testing.T) {
		value := strings.Repeat("x", 128+172)
		got := record(t, "SET", "key", value)

		want := []string{"SET", "key", strings.Repeat("x", 128) + "... (172 more bytes)"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("slowlog command = %q, want %q", got, want)
		}
	})

	t.Run("an argument of exactly 128 bytes is kept whole", func(t *testing.T) {
		value := strings.Repeat("y", 128)
		got := record(t, "SET", "key", value)

		if want := []string{"SET", "key", value}; !reflect.DeepEqual(got, want) {
			t.Fatalf("slowlog command = %q, want %q", got, want)
		}
	})

	t.Run("more than 32 arguments keep 31 and summarise the rest", func(t *testing.T) {
		request := []string{"RPUSH", "list"}
		for i := 0; i < 48; i++ {
			request = append(request, "v"+strconv.Itoa(i))
		}
		got := record(t, request...)

		// 50 tokens in all (name, key, 48 values): 31 kept, the last slot says
		// how many were dropped, counting the one it replaces.
		if len(got) != 32 {
			t.Fatalf("len(slowlog command) = %d, want 32", len(got))
		}
		if !reflect.DeepEqual(got[:31], request[:31]) {
			t.Fatalf("first 31 tokens = %q, want %q", got[:31], request[:31])
		}
		if want := "... (19 more arguments)"; got[31] != want {
			t.Fatalf("last token = %q, want %q", got[31], want)
		}
	})

	t.Run("exactly 32 arguments are kept whole", func(t *testing.T) {
		request := []string{"RPUSH", "list"}
		for i := 0; i < 30; i++ {
			request = append(request, "v"+strconv.Itoa(i))
		}
		got := record(t, request...)

		if !reflect.DeepEqual(got, request) {
			t.Fatalf("slowlog command = %q, want %q", got, request)
		}
	})

	t.Run("a stored entry does not retain a huge value", func(t *testing.T) {
		got := record(t, "SET", "key", strings.Repeat("z", 8<<20))

		total := 0
		for _, token := range got {
			total += len(token)
		}
		if total > 1024 {
			t.Fatalf("slowlog entry holds %d bytes, want it bounded by the truncation limits", total)
		}
	})
}

func TestExecutorReplicationAcknowledgements(t *testing.T) {
	t.Run("replica-origin GETACK emits upstream ACK reply", func(t *testing.T) {
		executor := newTestExecutor()
		replication := &server.ReplicationState{}
		replication.AdvanceReplicaOffset(123)
		executor.SetReplicationState(replication)

		result, err := executor.ExecuteDetailed(server.WithReplicationOrigin(context.Background()), requestValue("REPLCONF", "GETACK", "*"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 0 {
			t.Fatalf("len(result.Responses) = %d, want 0", len(result.Responses))
		}
		if len(result.UpstreamReplies) != 1 {
			t.Fatalf("len(result.UpstreamReplies) = %d, want 1", len(result.UpstreamReplies))
		}

		assertValueEqual(t, result.UpstreamReplies[0], requestValue("REPLCONF", "ACK", "123"))
	})

	t.Run("ACK updates tracked replica offset", func(t *testing.T) {
		executor := newTestExecutor()
		registry := server.NewReplicaRegistry()
		serverConn, replicaConn := net.Pipe()
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = replicaConn.Close() }()

		state := newReplicaPeerStateForExecutor(executor, 7, serverConn)
		registry.Add(nil, 7, serverConn, 6380, state)
		executor.SetReplicaRegistry(registry)

		ctx := server.WithClientState(context.Background(), state)

		result, err := executor.ExecuteDetailed(ctx, requestValue("REPLCONF", "ACK", "42"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Responses) != 0 {
			t.Fatalf("len(result.Responses) = %d, want 0", len(result.Responses))
		}
		if got := registry.CountReplicasAtOrAbove(42); got != 1 {
			t.Fatalf("CountReplicasAtOrAbove(42) = %d, want 1", got)
		}
		if got := registry.CountReplicasAtOrAbove(43); got != 0 {
			t.Fatalf("CountReplicasAtOrAbove(43) = %d, want 0", got)
		}
	})
}

func TestExecutorWait(t *testing.T) {
	t.Run("WAIT requests ACKs and returns once enough replicas catch up", func(t *testing.T) {
		executor := newTestExecutor()
		replication := &server.ReplicationState{}
		executor.SetReplicationState(replication)

		registry := server.NewReplicaRegistry()
		serverConn, replicaConn := net.Pipe()
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = replicaConn.Close() }()

		registry.Add(replication, 11, serverConn, 6380, newReplicaPeerStateForExecutor(executor, 11, serverConn))
		executor.SetReplicaRegistry(registry)
		// 50 bytes written after the replica attached, which it acknowledges below.
		replication.AdvanceMasterOffset(50)

		requestSeen := make(chan struct{})
		go func() {
			defer close(requestSeen)

			parser := protocol.NewParser(replicaConn)
			value, err := parser.Parse()
			if err != nil {
				return
			}
			request, err := DecodeRequest(value)
			if err != nil {
				return
			}
			if request.Name != "REPLCONF" || len(request.Args) != 2 || string(request.Args[0]) != "GETACK" || string(request.Args[1]) != "*" {
				return
			}

			time.Sleep(20 * time.Millisecond)
			registry.UpdateAck(11, 50)
		}()

		result, err := executor.ExecuteDetailed(context.Background(), requestValue("WAIT", "1", "200"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		<-requestSeen

		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 1})
	})

	t.Run("WAIT times out when replicas do not acknowledge", func(t *testing.T) {
		executor := newTestExecutor()
		replication := &server.ReplicationState{}
		replication.AdvanceMasterOffset(5)
		executor.SetReplicationState(replication)

		startedAt := time.Now()
		result, err := executor.ExecuteDetailed(context.Background(), requestValue("WAIT", "1", "25"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if time.Since(startedAt) < 20*time.Millisecond {
			t.Fatalf("WAIT returned too quickly: %v", time.Since(startedAt))
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 0})
	})

	t.Run("WAIT with a timeout past what a duration can hold keeps waiting", func(t *testing.T) {
		// 9223372036855 ms overflows time.Duration; it used to wrap negative, so the
		// timer fired at once and WAIT answered without waiting at all.
		executor := newTestExecutor()
		replication := &server.ReplicationState{}
		replication.AdvanceMasterOffset(5)
		executor.SetReplicationState(replication)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := executor.ExecuteDetailed(ctx, requestValue("WAIT", "1", "9223372036855"))
			done <- err
		}()

		select {
		case err := <-done:
			t.Fatalf("WAIT returned at once (error = %v), want it to keep waiting for its huge timeout", err)
		case <-time.After(200 * time.Millisecond):
		}

		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("WAIT error after cancel = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("WAIT did not stop when its context was canceled")
		}
	})

	t.Run("WAIT uses the calling client's last write offset", func(t *testing.T) {
		executor := newTestExecutor()
		replication := &server.ReplicationState{}
		replication.AdvanceMasterOffset(50)
		executor.SetReplicationState(replication)

		registry := server.NewReplicaRegistry()
		serverConn, replicaConn := net.Pipe()
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = replicaConn.Close() }()

		registry.Add(replication, 12, serverConn, 6380, newReplicaPeerStateForExecutor(executor, 12, serverConn))
		executor.SetReplicaRegistry(registry)

		state := &server.ClientState{ID: 99, Authenticated: true}
		state.SetLastWriteReplicationOffset(0)
		ctx := server.WithClientState(context.Background(), state)

		startedAt := time.Now()
		result, err := executor.ExecuteDetailed(ctx, requestValue("WAIT", "1", "50"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if time.Since(startedAt) > 20*time.Millisecond {
			t.Fatalf("WAIT took too long for a client with no pending replicated writes: %v", time.Since(startedAt))
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.Integer{Value: 1})
	})
}

func TestExecutorAuth(t *testing.T) {
	t.Run("unauthenticated clients may only use AUTH and PING", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 1)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		pong, err := executor.Execute(ctx, requestValue("PING"))
		if err != nil {
			t.Fatalf("PING error = %v", err)
		}
		assertValueEqual(t, pong, protocol.SimpleString{Value: "PONG"})

		echoed, err := executor.Execute(ctx, requestValue("PING", "hello"))
		if err != nil {
			t.Fatalf("PING hello error = %v", err)
		}
		assertValueEqual(t, echoed, protocol.BulkString{Data: []byte("hello")})

		if _, err := executor.Execute(ctx, requestValue("SET", "name", "Stash")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("SET error = %v, want ErrNoAuth", err)
		}
		if state.InTransactionActive() {
			t.Fatal("InTransactionActive() = true after rejected MULTI, want false")
		}

		if _, err := executor.Execute(ctx, requestValue("MULTI")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("MULTI error = %v, want ErrNoAuth", err)
		}
		if state.InTransactionActive() {
			t.Fatal("InTransactionActive() = true after rejected MULTI, want false")
		}

		if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("SUBSCRIBE error = %v, want ErrNoAuth", err)
		}
		if state.IsSubscribed() {
			t.Fatal("IsSubscribed() = true after rejected SUBSCRIBE, want false")
		}
	})

	t.Run("AUTH success unlocks subsequent commands on the same connection", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 2)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		value, err := executor.Execute(ctx, requestValue("AUTH", "secret"))
		if err != nil {
			t.Fatalf("AUTH error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})
		if !state.IsAuthenticated() {
			t.Fatal("IsAuthenticated() = false after successful AUTH, want true")
		}

		value, err = executor.Execute(ctx, requestValue("SET", "name", "Stash"))
		if err != nil {
			t.Fatalf("SET after AUTH error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})

		value, err = executor.Execute(ctx, requestValue("GET", "name"))
		if err != nil {
			t.Fatalf("GET after AUTH error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("Stash")})
	})

	t.Run("AUTH failure keeps the client blocked", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 3)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		if _, err := executor.Execute(ctx, requestValue("AUTH", "wrong")); !errors.Is(err, ErrWrongPass) {
			t.Fatalf("AUTH wrong error = %v, want ErrWrongPass", err)
		}
		if state.IsAuthenticated() {
			t.Fatal("IsAuthenticated() = true after failed AUTH, want false")
		}

		if _, err := executor.Execute(ctx, requestValue("GET", "name")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("GET after failed AUTH error = %v, want ErrNoAuth", err)
		}
	})

	t.Run("AUTH failure after successful AUTH keeps the client authenticated", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 33)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		if _, err := executor.Execute(ctx, requestValue("AUTH", "secret")); err != nil {
			t.Fatalf("AUTH secret error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("AUTH", "wrong")); !errors.Is(err, ErrWrongPass) {
			t.Fatalf("AUTH wrong error = %v, want ErrWrongPass", err)
		}
		if !state.IsAuthenticated() {
			t.Fatal("IsAuthenticated() = false after failed re-auth, want true")
		}

		value, err := executor.Execute(ctx, requestValue("SET", "name", "Stash"))
		if err != nil {
			t.Fatalf("SET after failed re-auth error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})
	})

	t.Run("AUTH without configured password returns an error", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 4)

		if _, err := executor.Execute(ctx, requestValue("AUTH", "secret")); !errors.Is(err, ErrAuthNotConfigured) {
			t.Fatalf("AUTH without requirepass error = %v, want ErrAuthNotConfigured", err)
		}
	})

	t.Run("unauthenticated clients cannot start replica handshake when password protection is enabled", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		executor.SetReplicationState(&server.ReplicationState{MasterReplicationID: "test-replid"})
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 5)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		if _, err := executor.ExecuteDetailed(ctx, requestValue("REPLCONF", "listening-port", "6380")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("REPLCONF listening-port error = %v, want ErrNoAuth", err)
		}
		if _, err := executor.ExecuteDetailed(ctx, requestValue("PSYNC", "?", "-1")); !errors.Is(err, ErrNoAuth) {
			t.Fatalf("PSYNC error = %v, want ErrNoAuth", err)
		}
		if state.IsReplica() {
			t.Fatal("IsReplica() = true after rejected handshake, want false")
		}
	})

	t.Run("authenticated clients may perform replica handshake on protected masters", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		executor.SetReplicationState(&server.ReplicationState{MasterReplicationID: "test-replid"})
		registry := server.NewReplicaRegistry()
		executor.SetReplicaRegistry(registry)
		ctx := withUnauthenticatedClientStateForExecutor(context.Background(), executor, 5)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		value, err := executor.Execute(ctx, requestValue("AUTH", "secret"))
		if err != nil {
			t.Fatalf("AUTH error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})

		result, err := executor.ExecuteDetailed(ctx, requestValue("REPLCONF", "listening-port", "6380"))
		if err != nil {
			t.Fatalf("REPLCONF listening-port error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.SimpleString{Value: "OK"})

		result, err = executor.ExecuteDetailed(ctx, requestValue("PSYNC", "?", "-1"))
		if err != nil {
			t.Fatalf("PSYNC error = %v", err)
		}
		if len(result.Responses) != 2 {
			t.Fatalf("len(result.Responses) = %d, want 2", len(result.Responses))
		}
		if !state.IsReplica() {
			t.Fatal("IsReplica() = false after PSYNC, want true")
		}
		if !result.RegisterReplica || registry.Count() != 1 {
			t.Fatalf("RegisterReplica = %v with %d replicas registered, want true and 1", result.RegisterReplica, registry.Count())
		}
		if !state.IsAuthenticated() {
			t.Fatal("IsAuthenticated() = false after authenticated PSYNC, want true")
		}
	})

	t.Run("replication-origin traffic bypasses the auth gate", func(t *testing.T) {
		executor := newTestExecutor()
		executor.SetRequirePass("secret")
		ctx := server.WithReplicationOrigin(withUnauthenticatedClientStateForExecutor(context.Background(), executor, 6))

		value, err := executor.Execute(ctx, requestValue("SET", "replicated", "1"))
		if err != nil {
			t.Fatalf("replication-origin SET error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})
	})
}

func TestExecutorXAddAutoGeneratesIDs(t *testing.T) {
	executor := newTestExecutor()

	first, err := executor.Execute(context.Background(), requestValue("XADD", "events", "*", "field", "one"))
	if err != nil {
		t.Fatalf("first XADD error = %v", err)
	}
	second, err := executor.Execute(context.Background(), requestValue("XADD", "events", "*", "field", "two"))
	if err != nil {
		t.Fatalf("second XADD error = %v", err)
	}

	firstIDText := mustBulkStringText(t, first)
	secondIDText := mustBulkStringText(t, second)

	firstID, err := storageParseStreamIDForTest(firstIDText)
	if err != nil {
		t.Fatalf("parse first ID error = %v", err)
	}
	secondID, err := storageParseStreamIDForTest(secondIDText)
	if err != nil {
		t.Fatalf("parse second ID error = %v", err)
	}
	if secondID.milliseconds < firstID.milliseconds || (secondID.milliseconds == firstID.milliseconds && secondID.sequence <= firstID.sequence) {
		t.Fatalf("second auto-generated ID %q is not greater than first ID %q", secondIDText, firstIDText)
	}
}

func storageParseStreamIDForTest(raw string) (struct{ milliseconds, sequence int64 }, error) {
	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return struct{ milliseconds, sequence int64 }{}, errors.New("invalid stream ID")
	}
	milliseconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return struct{ milliseconds, sequence int64 }{}, err
	}
	sequence, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return struct{ milliseconds, sequence int64 }{}, err
	}
	return struct{ milliseconds, sequence int64 }{milliseconds: milliseconds, sequence: sequence}, nil
}

func TestExecutorBLPop(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Executor)
	}{
		{name: "unblocks on regular push"},
		{
			name: "unblocks when maxmemory is enabled",
			configure: func(executor *Executor) {
				executor.store.ConfigureMaxMemory(1<<20, 16)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := newTestExecutor()
			if tt.configure != nil {
				tt.configure(executor)
			}

			pushErrCh := make(chan error, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				_, err := executor.Execute(context.Background(), requestValue("RPUSH", "jobs", "build"))
				pushErrCh <- err
			}()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			value, err := executor.Execute(ctx, requestValue("BLPOP", "jobs"))
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if pushErr := <-pushErrCh; pushErr != nil {
				t.Fatalf("RPUSH error = %v", pushErr)
			}

			assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: []byte("jobs")},
				protocol.BulkString{Data: []byte("build")},
			}})
		})
	}
}

func TestExecutorTransactions(t *testing.T) {
	t.Run("MULTI queues commands until EXEC", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		value, err := executor.Execute(ctx, requestValue("MULTI"))
		if err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})

		value, err = executor.Execute(ctx, requestValue("SET", "name", "Stash"))
		if err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		value, err = executor.Execute(ctx, requestValue("GET", "name"))
		if err != nil {
			t.Fatalf("queued GET error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		value, err = executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.SimpleString{Value: "OK"},
			protocol.BulkString{Data: []byte("Stash")},
		}})
	})

	t.Run("EXEC includes per-command errors", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "bad", "hello")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("INCR", "bad")); err != nil {
			t.Fatalf("queued INCR error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.SimpleString{Value: "OK"},
			protocol.ErrorValue{Message: "ERR value is not an integer or out of range"},
		}})
	})

	t.Run("empty EXEC returns an empty array", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{}})
	})

	t.Run("transaction state errors use Redis-compatible sentinels", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecWithoutMulti) {
			t.Fatalf("EXEC error = %v, want ErrExecWithoutMulti", err)
		}
		if _, err := executor.Execute(ctx, requestValue("DISCARD")); !errors.Is(err, ErrDiscardWithoutMulti) {
			t.Fatalf("DISCARD error = %v, want ErrDiscardWithoutMulti", err)
		}
		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("first MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("MULTI")); !errors.Is(err, ErrMultiNested) {
			t.Fatalf("nested MULTI error = %v, want ErrMultiNested", err)
		}
	})

	t.Run("WATCH aborts EXEC after another client modifies a watched key", func(t *testing.T) {
		executor := newTestExecutor()
		watcherCtx := withClientStateForExecutor(context.Background(), executor, 1)
		writerCtx := withClientStateForExecutor(context.Background(), executor, 2)

		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "counter")); err != nil {
			t.Fatalf("WATCH error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("SET", "counter", "2")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("SET", "counter", "1")); err != nil {
			t.Fatalf("writer SET error = %v", err)
		}

		value, err := executor.Execute(watcherCtx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Null: true})

		value, err = executor.Execute(watcherCtx, requestValue("GET", "counter"))
		if err != nil {
			t.Fatalf("GET after aborted EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("1")})
	})

	t.Run("repeated WATCH calls accumulate watched keys", func(t *testing.T) {
		executor := newTestExecutor()
		watcherCtx := withClientStateForExecutor(context.Background(), executor, 1)
		writerCtx := withClientStateForExecutor(context.Background(), executor, 2)

		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "alpha")); err != nil {
			t.Fatalf("WATCH alpha error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "beta")); err != nil {
			t.Fatalf("WATCH beta error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("SET", "beta", "1")); err != nil {
			t.Fatalf("writer SET beta error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("SET", "alpha", "2")); err != nil {
			t.Fatalf("queued SET alpha error = %v", err)
		}

		value, err := executor.Execute(watcherCtx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Null: true})
	})

	t.Run("WATCH on an untouched missing key still allows EXEC", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("WATCH", "missing")); err != nil {
			t.Fatalf("WATCH error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "missing", "1")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{protocol.SimpleString{Value: "OK"}}})

		value, err = executor.Execute(ctx, requestValue("GET", "missing"))
		if err != nil {
			t.Fatalf("GET after EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("1")})
	})

	t.Run("DISCARD clears queued state so later commands execute immediately", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "temp", "discarded")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		value, err := executor.Execute(ctx, requestValue("DISCARD"))
		if err != nil {
			t.Fatalf("DISCARD error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})

		if state.InTransactionActive() {
			t.Fatal("InTransactionActive() = true after DISCARD, want false")
		}
		if state.TxQueue != nil {
			t.Fatalf("TxQueue = %#v after DISCARD, want nil", state.TxQueue)
		}

		value, err = executor.Execute(ctx, requestValue("SET", "after-discard", "1"))
		if err != nil {
			t.Fatalf("SET after DISCARD error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})

		value, err = executor.Execute(ctx, requestValue("GET", "temp"))
		if err != nil {
			t.Fatalf("GET temp error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})

		value, err = executor.Execute(ctx, requestValue("GET", "after-discard"))
		if err != nil {
			t.Fatalf("GET after-discard error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("1")})
	})

	t.Run("DISCARD clears dirty transaction state for the next MULTI", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)
		state, ok := server.ClientStateFromContext(ctx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no state")
		}

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("first MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("NOPE")); err == nil {
			t.Fatal("NOPE error = nil, want unknown command error")
		}
		if !state.TransactionDirty() {
			t.Fatal("TransactionDirty() = false after queue-time error, want true")
		}

		value, err := executor.Execute(ctx, requestValue("DISCARD"))
		if err != nil {
			t.Fatalf("DISCARD error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})
		if state.TransactionDirty() {
			t.Fatal("TransactionDirty() = true after DISCARD, want false")
		}

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("second MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "clean", "1")); err != nil {
			t.Fatalf("queued clean SET error = %v", err)
		}

		value, err = executor.Execute(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{protocol.SimpleString{Value: "OK"}}})
	})

	t.Run("DISCARD clears failed watch state for the next transaction", func(t *testing.T) {
		executor := newTestExecutor()
		watcherCtx := withClientStateForExecutor(context.Background(), executor, 1)
		writerCtx := withClientStateForExecutor(context.Background(), executor, 2)
		state, ok := server.ClientStateFromContext(watcherCtx)
		if !ok || state == nil {
			t.Fatal("ClientStateFromContext() returned no watcher state")
		}

		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "counter")); err != nil {
			t.Fatalf("WATCH error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("SET", "counter", "1")); err != nil {
			t.Fatalf("writer SET error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("SET", "counter", "2")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if !state.TransactionFailed() {
			t.Fatal("TransactionFailed() = false before DISCARD, want true")
		}

		value, err := executor.Execute(watcherCtx, requestValue("DISCARD"))
		if err != nil {
			t.Fatalf("DISCARD error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "OK"})
		if state.TransactionFailed() {
			t.Fatal("TransactionFailed() = true after DISCARD, want false")
		}

		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("second MULTI error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("SET", "counter", "3")); err != nil {
			t.Fatalf("queued SET after DISCARD error = %v", err)
		}

		value, err = executor.Execute(watcherCtx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC after DISCARD error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{protocol.SimpleString{Value: "OK"}}})

		value, err = executor.Execute(watcherCtx, requestValue("GET", "counter"))
		if err != nil {
			t.Fatalf("GET counter error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("3")})
	})

	t.Run("WATCH invalidation before MULTI still aborts EXEC", func(t *testing.T) {
		executor := newTestExecutor()
		watcherCtx := withClientStateForExecutor(context.Background(), executor, 1)
		writerCtx := withClientStateForExecutor(context.Background(), executor, 2)

		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "counter")); err != nil {
			t.Fatalf("WATCH error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("SET", "counter", "1")); err != nil {
			t.Fatalf("writer SET error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}

		value, err := executor.Execute(watcherCtx, requestValue("SET", "counter", "2"))
		if err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		value, err = executor.Execute(watcherCtx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Null: true})

		value, err = executor.Execute(watcherCtx, requestValue("GET", "counter"))
		if err != nil {
			t.Fatalf("GET after aborted EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("1")})
	})

	t.Run("multi-key DEL only touches watchers for removed keys", func(t *testing.T) {
		executor := newTestExecutor()
		watcherCtx := withClientStateForExecutor(context.Background(), executor, 1)
		writerCtx := withClientStateForExecutor(context.Background(), executor, 2)

		if _, err := executor.Execute(watcherCtx, requestValue("WATCH", "watched")); err != nil {
			t.Fatalf("WATCH error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(watcherCtx, requestValue("SET", "watched", "2")); err != nil {
			t.Fatalf("queued SET error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("SET", "other", "1")); err != nil {
			t.Fatalf("writer SET error = %v", err)
		}
		if _, err := executor.Execute(writerCtx, requestValue("DEL", "other", "watched")); err != nil {
			t.Fatalf("writer DEL error = %v", err)
		}

		value, err := executor.Execute(watcherCtx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{protocol.SimpleString{Value: "OK"}}})

		value, err = executor.Execute(watcherCtx, requestValue("GET", "watched"))
		if err != nil {
			t.Fatalf("GET after EXEC error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("2")})
	})

	t.Run("WATCH is rejected inside MULTI", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("WATCH", "counter")); !errors.Is(err, ErrWatchInsideMulti) {
			t.Fatalf("WATCH error = %v, want ErrWatchInsideMulti", err)
		}
	})

	t.Run("queue-time validation errors abort EXEC", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("NOPE")); err == nil || err.Error() != "unknown command \"NOPE\"" {
			t.Fatalf("unknown command error = %v, want unknown command \"NOPE\"", err)
		}
		value, err := executor.Execute(ctx, requestValue("SET", "name", "Stash"))
		if err != nil {
			t.Fatalf("queued SET after invalid command error = %v", err)
		}
		assertValueEqual(t, value, protocol.SimpleString{Value: "QUEUED"})

		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
			t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
		}

		value, err = executor.Execute(ctx, requestValue("GET", "name"))
		if err != nil {
			t.Fatalf("GET after EXECABORT error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})
	})

	t.Run("queue-time syntax errors are returned immediately", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("SET", "temp", "1", "NX", "10")); !errors.Is(err, ErrSyntax) {
			t.Fatalf("queued SET syntax error = %v, want ErrSyntax", err)
		}
		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
			t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
		}
	})

	t.Run("PSYNC is refused inside a transaction, which then aborts", func(t *testing.T) {
		tests := []struct {
			name string
			args []string
		}{
			{name: "the arguments a replica sends", args: []string{"?", "-1"}},
			// Redis checks only the arity of a NO_MULTI command inside MULTI.
			{name: "arguments PSYNC would reject outside MULTI", args: []string{"a", "b"}},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				executor := newTestExecutor()
				executor.SetReplicationState(&server.ReplicationState{MasterReplicationID: "test-replid"})
				ctx := withClientStateForExecutor(context.Background(), executor, 1)
				state, _ := server.ClientStateFromContext(ctx)

				if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
					t.Fatalf("MULTI error = %v", err)
				}
				_, err := executor.ExecuteDetailed(ctx, requestValue(append([]string{"PSYNC"}, tt.args...)...))
				if !errors.Is(err, ErrNotAllowedInTransaction) || err.Error() != "Command not allowed inside a transaction" {
					t.Fatalf("queued PSYNC %v error = %v, want ErrNotAllowedInTransaction", tt.args, err)
				}
				if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
					t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
				}
				if state.IsReplica() {
					t.Fatal("IsReplica() = true after a refused PSYNC, want false")
				}
			})
		}
	})

	t.Run("queue-time malformed stream IDs abort EXEC", func(t *testing.T) {
		tests := []struct {
			name    string
			request protocol.Value
		}{
			{
				name:    "XADD malformed ID",
				request: requestValue("XADD", "events", "bad-id", "field", "value"),
			},
			{
				name:    "XREAD malformed ID",
				request: requestValue("XREAD", "STREAMS", "events", "bad-id"),
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				executor := newTestExecutor()
				ctx := withClientStateForExecutor(context.Background(), executor, 1)

				if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
					t.Fatalf("MULTI error = %v", err)
				}
				if _, err := executor.Execute(ctx, tt.request); !errors.Is(err, ErrInvalidStreamID) {
					t.Fatalf("queued stream validation error = %v, want ErrInvalidStreamID", err)
				}
				if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrExecAbort) {
					t.Fatalf("EXEC error = %v, want ErrExecAbort", err)
				}
			})
		}
	})
}

func TestExecutorPubSub(t *testing.T) {
	t.Run("SUBSCRIBE then PUBLISH delivers message and returns subscriber count", func(t *testing.T) {
		executor := newTestExecutor()
		state := newTestClientState(executor, 1)
		var outbound bytes.Buffer
		state.BindResponseWriter(bufio.NewWriter(&outbound))
		ctx := server.WithClientState(context.Background(), state)

		result, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news"))
		if err != nil {
			t.Fatalf("SUBSCRIBE error = %v", err)
		}
		if len(result.Responses) != 1 {
			t.Fatalf("len(result.Responses) = %d, want 1", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "subscribe"},
			protocol.BulkString{Data: []byte("news")},
			protocol.Integer{Value: 1},
		}})

		published, err := executor.Execute(context.Background(), requestValue("PUBLISH", "news", "hello"))
		if err != nil {
			t.Fatalf("PUBLISH error = %v", err)
		}
		assertValueEqual(t, published, protocol.Integer{Value: 1})

		parser := protocol.NewParser(&outbound)
		message, err := parser.Parse()
		if err != nil {
			t.Fatalf("Parse() pushed message error = %v", err)
		}
		assertValueEqual(t, message, protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "message"},
			protocol.BulkString{Data: []byte("news")},
			protocol.BulkString{Data: []byte("hello")},
		}})
	})

	t.Run("duplicate SUBSCRIBE does not increase subscription count", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		first, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news"))
		if err != nil {
			t.Fatalf("first SUBSCRIBE error = %v", err)
		}
		second, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news"))
		if err != nil {
			t.Fatalf("second SUBSCRIBE error = %v", err)
		}

		assertValueEqual(t, first.Responses[0], protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "subscribe"},
			protocol.BulkString{Data: []byte("news")},
			protocol.Integer{Value: 1},
		}})
		assertValueEqual(t, second.Responses[0], protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "subscribe"},
			protocol.BulkString{Data: []byte("news")},
			protocol.Integer{Value: 1},
		}})
	})

	t.Run("UNSUBSCRIBE with no arguments removes all channels in sorted order", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "zulu", "alpha")); err != nil {
			t.Fatalf("SUBSCRIBE error = %v", err)
		}

		result, err := executor.ExecuteDetailed(ctx, requestValue("UNSUBSCRIBE"))
		if err != nil {
			t.Fatalf("UNSUBSCRIBE error = %v", err)
		}
		if len(result.Responses) != 2 {
			t.Fatalf("len(result.Responses) = %d, want 2", len(result.Responses))
		}
		assertValueEqual(t, result.Responses[0], protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "unsubscribe"},
			protocol.BulkString{Data: []byte("alpha")},
			protocol.Integer{Value: 1},
		}})
		assertValueEqual(t, result.Responses[1], protocol.Array{Elements: []protocol.Value{
			protocol.TextBulkString{Value: "unsubscribe"},
			protocol.BulkString{Data: []byte("zulu")},
			protocol.Integer{Value: 0},
		}})
	})

	t.Run("subscribed clients only allow PING SUBSCRIBE and UNSUBSCRIBE", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news")); err != nil {
			t.Fatalf("SUBSCRIBE error = %v", err)
		}

		pong, err := executor.Execute(ctx, requestValue("PING", "hello"))
		if err != nil {
			t.Fatalf("PING error = %v", err)
		}
		assertValueEqual(t, pong, protocol.BulkString{Data: []byte("hello")})

		if _, err := executor.Execute(ctx, requestValue("GET", "news")); !errors.Is(err, ErrSubscribedModeOnly) {
			t.Fatalf("GET error = %v, want ErrSubscribedModeOnly", err)
		}
		if _, err := executor.Execute(ctx, requestValue("MULTI")); !errors.Is(err, ErrSubscribedModeOnly) {
			t.Fatalf("MULTI error = %v, want ErrSubscribedModeOnly", err)
		}
		if _, err := executor.Execute(ctx, requestValue("WATCH", "news")); !errors.Is(err, ErrSubscribedModeOnly) {
			t.Fatalf("WATCH error = %v, want ErrSubscribedModeOnly", err)
		}
		if _, err := executor.Execute(ctx, requestValue("EXEC")); !errors.Is(err, ErrSubscribedModeOnly) {
			t.Fatalf("EXEC error = %v, want ErrSubscribedModeOnly", err)
		}
		if _, err := executor.Execute(ctx, requestValue("DISCARD")); !errors.Is(err, ErrSubscribedModeOnly) {
			t.Fatalf("DISCARD error = %v, want ErrSubscribedModeOnly", err)
		}
	})

	t.Run("PUBLISH skips disconnected subscribers and prunes the registry", func(t *testing.T) {
		executor := newTestExecutor()
		state := newTestClientState(executor, 1)
		state.BindResponseWriter(bufio.NewWriter(io.Discard))
		ctx := server.WithClientState(context.Background(), state)

		if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news")); err != nil {
			t.Fatalf("SUBSCRIBE error = %v", err)
		}

		state.Disconnect()

		published, err := executor.Execute(context.Background(), requestValue("PUBLISH", "news", "hello"))
		if err != nil {
			t.Fatalf("PUBLISH error = %v", err)
		}
		assertValueEqual(t, published, protocol.Integer{Value: 0})
		if got := len(executor.PubSubRegistry().Subscribers("news")); got != 0 {
			t.Fatalf("len(Subscribers(news)) = %d after disconnected publish, want 0", got)
		}
		if state.IsSubscribed() {
			t.Fatal("IsSubscribed() = true after disconnected publish cleanup, want false")
		}
	})

	t.Run("SUBSCRIBE is rejected inside MULTI", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 1)

		if _, err := executor.Execute(ctx, requestValue("MULTI")); err != nil {
			t.Fatalf("MULTI error = %v", err)
		}
		if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news")); !errors.Is(err, ErrSubscribeInsideMulti) {
			t.Fatalf("SUBSCRIBE error = %v, want ErrSubscribeInsideMulti", err)
		}
	})

	t.Run("SUBSCRIBE rejects empty channel names", func(t *testing.T) {
		executor := newTestExecutor()
		state := newTestClientState(executor, 1)
		ctx := server.WithClientState(context.Background(), state)

		_, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", ""))
		if !errors.Is(err, ErrSyntax) {
			t.Fatalf("SUBSCRIBE empty channel error = %v, want ErrSyntax", err)
		}
		if state.IsSubscribed() {
			t.Fatal("IsSubscribed() = true after rejected empty-channel SUBSCRIBE, want false")
		}
	})

	t.Run("PUBLISH rejects empty channel names", func(t *testing.T) {
		executor := newTestExecutor()

		_, err := executor.Execute(context.Background(), requestValue("PUBLISH", "", "hello"))
		if !errors.Is(err, ErrSyntax) {
			t.Fatalf("PUBLISH empty channel error = %v, want ErrSyntax", err)
		}
	})

	t.Run("UNSUBSCRIBE rejects empty channel names without altering state", func(t *testing.T) {
		executor := newTestExecutor()
		state := newTestClientState(executor, 1)
		ctx := server.WithClientState(context.Background(), state)

		if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news")); err != nil {
			t.Fatalf("SUBSCRIBE news error = %v", err)
		}

		_, err := executor.ExecuteDetailed(ctx, requestValue("UNSUBSCRIBE", ""))
		if !errors.Is(err, ErrSyntax) {
			t.Fatalf("UNSUBSCRIBE empty channel error = %v, want ErrSyntax", err)
		}
		if got := state.SubscriptionCount(); got != 1 {
			t.Fatalf("SubscriptionCount() = %d after rejected empty-channel UNSUBSCRIBE, want 1", got)
		}
	})
}

func BenchmarkExecutorPublish(b *testing.B) {
	for _, subscriberCount := range []int{1, 32, 256} {
		b.Run(fmt.Sprintf("subscribers=%d", subscriberCount), func(b *testing.B) {
			executor := newTestExecutor()
			for i := 0; i < subscriberCount; i++ {
				state := newTestClientState(executor, uint64(i+1))
				state.BindResponseWriter(bufio.NewWriter(io.Discard))
				ctx := server.WithClientState(context.Background(), state)
				if _, err := executor.ExecuteDetailed(ctx, requestValue("SUBSCRIBE", "news")); err != nil {
					b.Fatalf("SUBSCRIBE error = %v", err)
				}
			}

			publishRequest := requestValue("PUBLISH", "news", "hello")
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				value, err := executor.Execute(context.Background(), publishRequest)
				if err != nil {
					b.Fatalf("PUBLISH error = %v", err)
				}
				if got, ok := value.(protocol.Integer); !ok || got.Value != int64(subscriberCount) {
					b.Fatalf("PUBLISH result = %#v, want %d subscribers", value, subscriberCount)
				}
			}
		})
	}
}

func BenchmarkExecutorDetailedPropagation(b *testing.B) {
	executor := newTestExecutor()
	request := requestValue("SET", "name", "Stash")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := executor.ExecuteDetailed(context.Background(), request)
		if err != nil {
			b.Fatalf("ExecuteDetailed() error = %v", err)
		}
		if len(result.Propagation) != 1 {
			b.Fatalf("len(result.Propagation) = %d, want 1", len(result.Propagation))
		}
	}
}

func TestDecodeRequest(t *testing.T) {
	tests := []struct {
		name   string
		value  protocol.Value
		want   *Request
		assert func(*testing.T, error)
	}{
		{
			name:  "bulk string command",
			value: requestValue("SET", "name", "Stash"),
			want:  &Request{Name: "SET", Args: [][]byte{[]byte("name"), []byte("Stash")}},
			assert: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("DecodeRequest() error = %v", err)
				}
			},
		},
		{
			name: "mixed simple string and integer tokens",
			value: protocol.Array{Elements: []protocol.Value{
				protocol.SimpleString{Value: "PING"},
				protocol.Integer{Value: 42},
			}},
			want: &Request{Name: "PING", Args: [][]byte{[]byte("42")}},
			assert: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("DecodeRequest() error = %v", err)
				}
			},
		},
		{
			name:  "null array rejected",
			value: protocol.Array{Null: true},
			assert: func(t *testing.T, err error) {
				t.Helper()
				var protocolErr ProtocolError
				if !errors.As(err, &protocolErr) {
					t.Fatalf("DecodeRequest() error = %v, want ProtocolError", err)
				}
			},
		},
		{
			name: "null bulk string token rejected",
			value: protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: []byte("GET")},
				protocol.BulkString{Null: true},
			}},
			assert: func(t *testing.T, err error) {
				t.Helper()
				var protocolErr ProtocolError
				if !errors.As(err, &protocolErr) {
					t.Fatalf("DecodeRequest() error = %v, want ProtocolError", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			request, err := DecodeRequest(tt.value)
			tt.assert(t, err)
			if tt.want == nil || err != nil {
				return
			}

			if request.Name != tt.want.Name {
				t.Fatalf("request.Name = %q, want %q", request.Name, tt.want.Name)
			}
			if len(request.Args) != len(tt.want.Args) {
				t.Fatalf("len(request.Args) = %d, want %d", len(request.Args), len(tt.want.Args))
			}
			for i := range request.Args {
				if string(request.Args[i]) != string(tt.want.Args[i]) {
					t.Fatalf("request.Args[%d] = %q, want %q", i, string(request.Args[i]), string(tt.want.Args[i]))
				}
			}
		})
	}
}

func TestSetRelativeExpiryPropagatesAsPXAT(t *testing.T) {
	extract := func(v protocol.Value) string {
		t.Helper()
		switch typed := v.(type) {
		case protocol.TextBulkString:
			return typed.Value
		case protocol.BulkString:
			return string(typed.Data)
		default:
			t.Fatalf("frame element type = %T, want bulk string", v)
			return ""
		}
	}

	t.Run("EX is rewritten to absolute PXAT for propagation and durability", func(t *testing.T) {
		executor := newTestExecutor()

		before := time.Now().UnixMilli()
		result, err := executor.ExecuteDetailed(context.Background(), requestValue("SET", "name", "Stash", "EX", "100"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		after := time.Now().UnixMilli()

		// The rewritten PXAT must equal the deadline the handler actually stored,
		// not a value re-derived from a later clock read.
		storedExpiry := storedExpiryMillis(t, executor, "name")

		for name, frames := range map[string][]protocol.Value{"propagation": result.Propagation, "durability": result.Durability} {
			if len(frames) != 1 {
				t.Fatalf("%s frames = %d, want 1", name, len(frames))
			}
			array, ok := frames[0].(protocol.Array)
			if !ok {
				t.Fatalf("%s frame type = %T, want Array", name, frames[0])
			}
			if len(array.Elements) != 5 {
				t.Fatalf("%s frame arity = %d, want 5 (SET key value PXAT ms)", name, len(array.Elements))
			}
			if got := extract(array.Elements[0]); got != "SET" {
				t.Fatalf("%s frame[0] = %q, want SET", name, got)
			}
			if got := extract(array.Elements[3]); got != "PXAT" {
				t.Fatalf("%s frame[3] = %q, want PXAT (relative EX must not be propagated verbatim)", name, got)
			}
			pxat, err := strconv.ParseInt(extract(array.Elements[4]), 10, 64)
			if err != nil {
				t.Fatalf("%s PXAT value not an integer: %v", name, err)
			}
			if pxat < before+100_000 || pxat > after+100_000 {
				t.Fatalf("%s PXAT = %d, want within [%d, %d]", name, pxat, before+100_000, after+100_000)
			}
			if pxat != storedExpiry {
				t.Fatalf("%s PXAT = %d, want exactly the stored deadline %d (no clock drift)", name, pxat, storedExpiry)
			}
		}
	})

	t.Run("SET without expiry keeps its verbatim frame", func(t *testing.T) {
		executor := newTestExecutor()
		result, err := executor.ExecuteDetailed(context.Background(), requestValue("SET", "name", "Stash"))
		if err != nil {
			t.Fatalf("ExecuteDetailed() error = %v", err)
		}
		assertPropagationFrames(t, result.Propagation, requestValue("SET", "name", "Stash"))
		assertPropagationFrames(t, result.Durability, requestValue("SET", "name", "Stash"))
	})

	t.Run("rewritten PXAT frame is accepted on replay and preserves the value", func(t *testing.T) {
		executor := newTestExecutor()
		future := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
		result, err := executor.ExecuteDetailed(context.Background(), requestValue("SET", "k", "v", "PXAT", future))
		if err != nil {
			t.Fatalf("ExecuteDetailed(SET PXAT) error = %v", err)
		}
		assertValueEqual(t, result.Responses[0], protocol.SimpleString{Value: "OK"})

		got, err := executor.ExecuteDetailed(context.Background(), requestValue("GET", "k"))
		if err != nil {
			t.Fatalf("ExecuteDetailed(GET) error = %v", err)
		}
		assertValueEqual(t, got.Responses[0], protocol.BulkString{Data: []byte("v")})
	})
}

func TestXAddAutoIDIsLoggedAsTheGeneratedID(t *testing.T) {
	// The durability frame is replayed at startup. Logging XADD's "*" verbatim
	// makes replay mint a fresh ID from the clock at that moment, so every
	// auto-ID entry came back under a different ID after a restart.
	t.Run("auto ID is replaced by the ID that was generated", func(t *testing.T) {
		executor := newTestExecutor()
		result, err := executor.ExecuteDetailed(context.Background(), requestValue("XADD", "events", "*", "type", "start"))
		if err != nil {
			t.Fatalf("ExecuteDetailed(XADD *) error = %v", err)
		}
		id, ok := result.Responses[0].(protocol.TextBulkString)
		if !ok || id.Value == "" || id.Value == "*" {
			t.Fatalf("XADD * reply = %#v, want the generated ID", result.Responses[0])
		}
		assertPropagationFrames(t, result.Durability, requestValue("XADD", "events", id.Value, "type", "start"))

		// Replaying the logged frame recreates the entry under the same ID.
		replayed := newTestExecutor()
		frame, ok := result.Durability[0].(protocol.Array)
		if !ok {
			t.Fatalf("durability frame = %#v, want an array", result.Durability[0])
		}
		if _, err := replayed.ExecuteDetailed(context.Background(), frame); err != nil {
			t.Fatalf("replaying the logged frame: %v", err)
		}
		entries, err := replayed.store.XRead("events", "0-0")
		if err != nil || len(entries) != 1 || entries[0].ID != id.Value {
			t.Fatalf("XRead after replay = (%#v, %v), want one entry with ID %q", entries, err, id.Value)
		}
	})

	t.Run("an XADD queued in a transaction is logged with its generated ID", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := withClientStateForExecutor(context.Background(), executor, 3)
		for _, step := range []protocol.Value{
			requestValue("MULTI"),
			requestValue("XADD", "events", "*", "type", "start"),
		} {
			if _, err := executor.ExecuteDetailed(ctx, step); err != nil {
				t.Fatalf("ExecuteDetailed(%v) error = %v", step, err)
			}
		}
		result, err := executor.ExecuteDetailed(ctx, requestValue("EXEC"))
		if err != nil {
			t.Fatalf("ExecuteDetailed(EXEC) error = %v", err)
		}
		replies, ok := result.Responses[0].(protocol.Array)
		if !ok || len(replies.Elements) != 1 {
			t.Fatalf("EXEC reply = %#v, want one queued reply", result.Responses[0])
		}
		id, ok := replies.Elements[0].(protocol.TextBulkString)
		if !ok || id.Value == "" || id.Value == "*" {
			t.Fatalf("queued XADD reply = %#v, want the generated ID", replies.Elements[0])
		}
		assertPropagationFrames(t, result.Durability, requestValue("XADD", "events", id.Value, "type", "start"))
	})

	t.Run("an explicit ID keeps its verbatim frame", func(t *testing.T) {
		executor := newTestExecutor()
		result, err := executor.ExecuteDetailed(context.Background(), requestValue("XADD", "events", "5-1", "type", "start"))
		if err != nil {
			t.Fatalf("ExecuteDetailed(XADD 5-1) error = %v", err)
		}
		assertPropagationFrames(t, result.Durability, requestValue("XADD", "events", "5-1", "type", "start"))
	})
}

func newTestExecutor() *Executor {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewExecutor(storage.NewStore(), logger)
}

func withClientStateForExecutor(ctx context.Context, executor *Executor, id uint64) context.Context {
	return server.WithClientState(ctx, newTestClientState(executor, id))
}

func withUnauthenticatedClientStateForExecutor(ctx context.Context, executor *Executor, id uint64) context.Context {
	state := newTestClientState(executor, id)
	state.SetAuthenticated(false)
	return server.WithClientState(ctx, state)
}

func newTestClientState(executor *Executor, id uint64) *server.ClientState {
	state := &server.ClientState{ID: id, Authenticated: true}
	state.SetWatchRegistry(executor.WatchRegistry())
	state.SetPubSubRegistry(executor.PubSubRegistry())
	return state
}

func newReplicaPeerStateForExecutor(executor *Executor, id uint64, conn net.Conn) *server.ClientState {
	state := newTestClientState(executor, id)
	state.PromoteToReplica()
	state.BindResponseWriter(bufio.NewWriter(conn))
	return state
}

func storedExpiryMillis(t *testing.T, executor *Executor, key string) int64 {
	t.Helper()
	entries, _ := executor.store.SnapshotAll()
	for _, entry := range entries {
		if entry.Key == key {
			return entry.ExpiresAt
		}
	}
	t.Fatalf("key %q not found in store snapshot", key)
	return 0
}

func requestValue(parts ...string) protocol.Value {
	elements := make([]protocol.Value, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, protocol.BulkString{Data: []byte(part)})
	}
	return protocol.Array{Elements: elements}
}

func assertValueEqual(t *testing.T, got protocol.Value, want protocol.Value) {
	t.Helper()

	switch typedWant := want.(type) {
	case protocol.SimpleString:
		typedGot, ok := got.(protocol.SimpleString)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("simple string = %q, want %q", typedGot.Value, typedWant.Value)
		}
	case protocol.BulkString:
		gotText, gotNull, ok := bulkStringContent(got)
		if !ok {
			t.Fatalf("value type = %T, want bulk-string-compatible type", got)
		}
		if gotNull != typedWant.Null {
			t.Fatalf("bulk string null = %v, want %v", gotNull, typedWant.Null)
		}
		if gotText != string(typedWant.Data) {
			t.Fatalf("bulk string = %q, want %q", gotText, string(typedWant.Data))
		}
	case protocol.TextBulkString:
		gotText, gotNull, ok := bulkStringContent(got)
		if !ok {
			t.Fatalf("value type = %T, want bulk-string-compatible type", got)
		}
		if gotNull != typedWant.Null {
			t.Fatalf("bulk string null = %v, want %v", gotNull, typedWant.Null)
		}
		if gotText != typedWant.Value {
			t.Fatalf("bulk string = %q, want %q", gotText, typedWant.Value)
		}
	case protocol.Integer:
		typedGot, ok := got.(protocol.Integer)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Value != typedWant.Value {
			t.Fatalf("integer = %d, want %d", typedGot.Value, typedWant.Value)
		}
	case protocol.Array:
		typedGot, ok := got.(protocol.Array)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Null != typedWant.Null {
			t.Fatalf("array null = %v, want %v", typedGot.Null, typedWant.Null)
		}
		if len(typedGot.Elements) != len(typedWant.Elements) {
			t.Fatalf("len(array) = %d, want %d", len(typedGot.Elements), len(typedWant.Elements))
		}
		for i := range typedWant.Elements {
			assertValueEqual(t, typedGot.Elements[i], typedWant.Elements[i])
		}
	case protocol.ErrorValue:
		typedGot, ok := got.(protocol.ErrorValue)
		if !ok {
			t.Fatalf("value type = %T, want %T", got, want)
		}
		if typedGot.Message != typedWant.Message {
			t.Fatalf("error message = %q, want %q", typedGot.Message, typedWant.Message)
		}
	default:
		t.Fatalf("unsupported wanted type %T", want)
	}
}

func mustBulkStringText(t *testing.T, value protocol.Value) string {
	t.Helper()

	text, isNull, ok := bulkStringContent(value)
	if !ok {
		t.Fatalf("value type = %T, want bulk-string-compatible type", value)
	}
	if isNull {
		t.Fatal("bulk string unexpectedly null")
	}

	return text
}

func bulkStringContent(value protocol.Value) (string, bool, bool) {
	switch typed := value.(type) {
	case protocol.BulkString:
		return string(typed.Data), typed.Null, true
	case protocol.TextBulkString:
		return typed.Value, typed.Null, true
	default:
		return "", false, false
	}
}

func assertPropagationFrames(t *testing.T, got []protocol.Value, want ...protocol.Value) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(propagation) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		assertValueEqual(t, got[i], want[i])
	}
}

// TestCommandSpecsAlwaysValidate locks the invariant the handlers rely on:
// ExecuteDetailed runs spec.validate before spec.handler/spec.detailed, so an
// executable command must register a validator. Handlers may then read their
// arguments without repeating the arity check. A new command registered with a
// handler but no validate would reach that handler unvalidated.
func TestCommandSpecsAlwaysValidate(t *testing.T) {
	executor := NewExecutor(storage.NewStore(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	for name, spec := range executor.commands {
		t.Run(name, func(t *testing.T) {
			if spec.handler == nil && spec.detailed == nil {
				t.Skip("command has no executable handler")
			}
			if spec.validate == nil {
				t.Errorf("command %q registers a handler but no validate function", name)
			}
		})
	}
}
