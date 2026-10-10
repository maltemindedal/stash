package command

import (
	"errors"
	"sort"
	"testing"

	"github.com/maltemindedal/stash/internal/protocol"
)

func TestExecutorLPopRPop(t *testing.T) {
	t.Run("LPOP without count returns bulk string", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("RPUSH", "k", "a", "b")); err != nil {
			t.Fatalf("RPUSH error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("LPOP", "k"))
		if err != nil {
			t.Fatalf("LPOP error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("a")})

		value, err = executor.Execute(ctx, requestValue("LPOP", "missing"))
		if err != nil {
			t.Fatalf("LPOP missing error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})
	})

	t.Run("LPOP with count returns array", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("RPUSH", "k", "a", "b", "c")); err != nil {
			t.Fatalf("RPUSH error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("LPOP", "k", "2"))
		if err != nil {
			t.Fatalf("LPOP k 2 error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok || len(array.Elements) != 2 {
			t.Fatalf("LPOP k 2 = %+v, want 2-element array", value)
		}
	})

	t.Run("LPOP missing key with count returns null array", func(t *testing.T) {
		executor := newTestExecutor()
		value, err := executor.Execute(clientContext(executor), requestValue("LPOP", "missing", "3"))
		if err != nil {
			t.Fatalf("LPOP missing 3 error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok || !array.Null {
			t.Fatalf("LPOP missing 3 = %+v, want null array", value)
		}
	})

	t.Run("RPOP without count returns tail", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("RPUSH", "k", "a", "b")); err != nil {
			t.Fatalf("RPUSH error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("RPOP", "k"))
		if err != nil {
			t.Fatalf("RPOP error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("b")})
	})

	t.Run("RPOP with count returns array in tail-first order", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("RPUSH", "k", "a", "b", "c")); err != nil {
			t.Fatalf("RPUSH error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("RPOP", "k", "2"))
		if err != nil {
			t.Fatalf("RPOP k 2 error = %v", err)
		}
		assertValueEqual(t, value, protocol.Array{Elements: []protocol.Value{
			protocol.BulkString{Data: []byte("c")},
			protocol.BulkString{Data: []byte("b")},
		}})
	})

	t.Run("RPOP missing key with count returns null array", func(t *testing.T) {
		executor := newTestExecutor()
		value, err := executor.Execute(clientContext(executor), requestValue("RPOP", "missing", "3"))
		if err != nil {
			t.Fatalf("RPOP missing 3 error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok || !array.Null {
			t.Fatalf("RPOP missing 3 = %+v, want null array", value)
		}
	})

	t.Run("LPOP rejects negative count", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("RPUSH", "k", "a")); err != nil {
			t.Fatalf("RPUSH error = %v", err)
		}
		if _, err := executor.Execute(ctx, requestValue("LPOP", "k", "-1")); err == nil {
			t.Fatalf("LPOP k -1 expected error, got nil")
		}
	})

	t.Run("LPOP on wrong type returns WRONGTYPE", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("SET", "k", "v")); err != nil {
			t.Fatalf("SET error = %v", err)
		}
		_, err := executor.Execute(ctx, requestValue("LPOP", "k"))
		if err == nil {
			t.Fatalf("LPOP expected WRONGTYPE error")
		}
		assertRESPPrefix(t, err, "WRONGTYPE")
	})
}

func TestExecutorHashCommands(t *testing.T) {
	t.Run("HSET returns newly added count", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)

		value, err := executor.Execute(ctx, requestValue("HSET", "h", "f1", "v1", "f2", "v2"))
		if err != nil {
			t.Fatalf("HSET error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 2})

		value, err = executor.Execute(ctx, requestValue("HSET", "h", "f1", "new", "f3", "v3"))
		if err != nil {
			t.Fatalf("HSET error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 1})
	})

	t.Run("HGET returns value or null", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("HSET", "h", "f", "v")); err != nil {
			t.Fatalf("HSET error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("HGET", "h", "f"))
		if err != nil {
			t.Fatalf("HGET error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Data: []byte("v")})

		value, err = executor.Execute(ctx, requestValue("HGET", "h", "missing"))
		if err != nil {
			t.Fatalf("HGET missing error = %v", err)
		}
		assertValueEqual(t, value, protocol.BulkString{Null: true})
	})

	t.Run("HDEL returns removed count", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("HSET", "h", "a", "1", "b", "2")); err != nil {
			t.Fatalf("HSET error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("HDEL", "h", "a", "missing"))
		if err != nil {
			t.Fatalf("HDEL error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 1})
	})

	t.Run("HGETALL returns all field/value pairs", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("HSET", "h", "a", "1", "b", "2")); err != nil {
			t.Fatalf("HSET error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("HGETALL", "h"))
		if err != nil {
			t.Fatalf("HGETALL error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok {
			t.Fatalf("HGETALL type = %T, want Array", value)
		}
		if len(array.Elements) != 4 {
			t.Fatalf("HGETALL len = %d, want 4", len(array.Elements))
		}
		pairs := collectHashPairs(t, array)
		sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
		if pairs[0][0] != "a" || pairs[0][1] != "1" || pairs[1][0] != "b" || pairs[1][1] != "2" {
			t.Fatalf("HGETALL pairs = %v", pairs)
		}
	})

	t.Run("HSET rejects odd arg count", func(t *testing.T) {
		executor := newTestExecutor()
		if _, err := executor.Execute(clientContext(executor), requestValue("HSET", "h", "f")); err == nil {
			t.Fatalf("HSET h f expected error")
		}
	})

	t.Run("Hash commands on wrong type return WRONGTYPE", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("SET", "k", "v")); err != nil {
			t.Fatalf("SET error = %v", err)
		}

		for _, req := range [][]string{
			{"HSET", "k", "f", "v"},
			{"HGET", "k", "f"},
			{"HDEL", "k", "f"},
			{"HGETALL", "k"},
		} {
			_, err := executor.Execute(ctx, requestValue(req...))
			if err == nil {
				t.Fatalf("%v expected WRONGTYPE error", req)
			}
			assertRESPPrefix(t, err, "WRONGTYPE")
		}
	})
}

func TestExecutorSetCommands(t *testing.T) {
	t.Run("SADD returns added count", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		value, err := executor.Execute(ctx, requestValue("SADD", "s", "a", "b", "a"))
		if err != nil {
			t.Fatalf("SADD error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 2})

		value, err = executor.Execute(ctx, requestValue("SADD", "s", "b", "c"))
		if err != nil {
			t.Fatalf("SADD error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 1})
	})

	t.Run("SISMEMBER returns 0/1", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("SADD", "s", "x")); err != nil {
			t.Fatalf("SADD error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("SISMEMBER", "s", "x"))
		if err != nil {
			t.Fatalf("SISMEMBER error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 1})

		value, err = executor.Execute(ctx, requestValue("SISMEMBER", "s", "y"))
		if err != nil {
			t.Fatalf("SISMEMBER y error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 0})
	})

	t.Run("SREM returns removed count", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("SADD", "s", "a", "b", "c")); err != nil {
			t.Fatalf("SADD error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("SREM", "s", "a", "missing"))
		if err != nil {
			t.Fatalf("SREM error = %v", err)
		}
		assertValueEqual(t, value, protocol.Integer{Value: 1})
	})

	t.Run("SMEMBERS returns all members", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("SADD", "s", "a", "b", "c")); err != nil {
			t.Fatalf("SADD error = %v", err)
		}

		value, err := executor.Execute(ctx, requestValue("SMEMBERS", "s"))
		if err != nil {
			t.Fatalf("SMEMBERS error = %v", err)
		}
		array, ok := value.(protocol.Array)
		if !ok || len(array.Elements) != 3 {
			t.Fatalf("SMEMBERS = %+v, want 3-element array", value)
		}
	})

	t.Run("Set commands on wrong type return WRONGTYPE", func(t *testing.T) {
		executor := newTestExecutor()
		ctx := clientContext(executor)
		if _, err := executor.Execute(ctx, requestValue("SET", "k", "v")); err != nil {
			t.Fatalf("SET error = %v", err)
		}

		for _, req := range [][]string{
			{"SADD", "k", "m"},
			{"SISMEMBER", "k", "m"},
			{"SREM", "k", "m"},
			{"SMEMBERS", "k"},
		} {
			_, err := executor.Execute(ctx, requestValue(req...))
			if err == nil {
				t.Fatalf("%v expected WRONGTYPE error", req)
			}
			assertRESPPrefix(t, err, "WRONGTYPE")
		}
	})
}

func assertRESPPrefix(t *testing.T, err error, wantPrefix string) {
	t.Helper()
	var resp RESPError
	if !errors.As(err, &resp) {
		t.Fatalf("error %v does not implement RESPError", err)
	}
	if resp.RESPErrorPrefix() != wantPrefix {
		t.Fatalf("prefix = %q, want %q", resp.RESPErrorPrefix(), wantPrefix)
	}
}

func collectHashPairs(t *testing.T, array protocol.Array) [][2]string {
	t.Helper()
	pairs := make([][2]string, 0, len(array.Elements)/2)
	for i := 0; i < len(array.Elements); i += 2 {
		field, _, ok := bulkStringContent(array.Elements[i])
		if !ok {
			t.Fatalf("element %d type = %T, want bulk string", i, array.Elements[i])
		}
		val, _, ok := bulkStringContent(array.Elements[i+1])
		if !ok {
			t.Fatalf("element %d type = %T, want bulk string", i+1, array.Elements[i+1])
		}
		pairs = append(pairs, [2]string{field, val})
	}
	return pairs
}
