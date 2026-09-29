package command

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func (e *Executor) handleLPush(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("LPUSH")
	}

	key := string(request.Args[0])
	length, evicted, err := e.store.LeftPush(key, request.Args[1:])
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: length}, nil
}

func (e *Executor) handleRPush(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("RPUSH")
	}

	key := string(request.Args[0])
	length, evicted, err := e.store.RightPush(key, request.Args[1:])
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(ctx, key, evicted)
	return protocol.Integer{Value: length}, nil
}

func (e *Executor) handleLRange(_ context.Context, request *Request) (protocol.Value, error) {
	start, stop, err := parseRangeBounds(request.Args[1:3])
	if err != nil {
		return nil, err
	}

	key := string(request.Args[0])
	values, err := e.store.ListRange(key, start, stop)
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	return listResponse(values), nil
}

func (e *Executor) handleBLPop(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("BLPOP")
	}

	key := string(request.Args[0])
	// While it waits, the command checks now and then that its client is still
	// there. Otherwise the next push would wake it, it would pop the element, and
	// the reply would go to a connection nobody is reading.
	clientCheck := time.NewTicker(blockedClientCheckInterval)
	defer clientCheck.Stop()
	for {
		waiter := e.store.SubscribeListPush(key)

		value, ok, err := e.store.LeftPop(key)
		if err != nil {
			e.store.UnsubscribeListPush(key, waiter)
			if errors.Is(err, storage.ErrWrongType) {
				return nil, ErrWrongTypeError()
			}
			return nil, err
		}
		if ok {
			e.store.UnsubscribeListPush(key, waiter)
			e.touchWatchKeys(key)
			return protocol.Array{Elements: []protocol.Value{
				protocol.BulkString{Data: clone(request.Args[0])},
				protocol.BulkString{Data: value},
			}}, nil
		}

		// On the event loop, blocking would deadlock the whole server: the
		// LPUSH that fires the waiter can only execute on the loop goroutine
		// this command is blocking.
		if server.IsInlineExecution(ctx) {
			e.store.UnsubscribeListPush(key, waiter)
			return nil, blockingNotSupportedError("BLPOP")
		}

		// Replies to the requests pipelined ahead of this one must not wait behind it.
		if err := server.FlushClientResponses(ctx); err != nil {
			e.store.UnsubscribeListPush(key, waiter)
			return nil, err
		}

		for waiting := true; waiting; {
			select {
			case <-waiter:
				waiting = false
			case <-clientCheck.C:
				if server.ClientDisconnected(ctx) {
					e.store.UnsubscribeListPush(key, waiter)
					return nil, server.ErrClientDisconnected
				}
			case <-ctx.Done():
				e.store.UnsubscribeListPush(key, waiter)
				return nil, ctx.Err()
			}
		}
		e.store.UnsubscribeListPush(key, waiter)
	}
}

// blockedClientCheckInterval is how often a blocked BLPOP checks that its client
// is still connected.
const blockedClientCheckInterval = 100 * time.Millisecond

func (e *Executor) handleLPop(_ context.Context, request *Request) (protocol.Value, error) {
	return e.popList(request, "LPOP", true)
}

func (e *Executor) handleRPop(_ context.Context, request *Request) (protocol.Value, error) {
	return e.popList(request, "RPOP", false)
}

func (e *Executor) popList(request *Request, name string, left bool) (protocol.Value, error) {
	if len(request.Args) != 1 && len(request.Args) != 2 {
		return nil, wrongNumberOfArgumentsError(name)
	}

	key := string(request.Args[0])

	if len(request.Args) == 1 {
		var (
			value []byte
			ok    bool
			err   error
		)
		if left {
			value, ok, err = e.store.LeftPop(key)
		} else {
			value, ok, err = e.store.RightPop(key)
		}
		if err != nil {
			if errors.Is(err, storage.ErrWrongType) {
				return nil, ErrWrongTypeError()
			}
			return nil, err
		}
		if !ok {
			return protocol.BulkString{Null: true}, nil
		}

		e.touchWatchKeys(key)
		return protocol.BulkString{Data: value}, nil
	}

	count, err := parseIntegerArgument(request.Args[1])
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, newRESPMessageError("ERR", "value is out of range, must be positive")
	}

	var (
		values [][]byte
		ok     bool
	)
	if left {
		values, ok, err = e.store.LeftPopN(key, count)
	} else {
		values, ok, err = e.store.RightPopN(key, count)
	}
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		if errors.Is(err, storage.ErrSyntax) {
			return nil, ErrSyntaxError()
		}
		return nil, err
	}
	if !ok {
		return protocol.Array{Null: true}, nil
	}

	if count > 0 && len(values) > 0 {
		e.touchWatchKeys(key)
	}
	return listResponse(values), nil
}

func validateLPopRequest(request *Request) error {
	return validatePopRequest(request, "LPOP")
}

func validateRPopRequest(request *Request) error {
	return validatePopRequest(request, "RPOP")
}

func validatePopRequest(request *Request, name string) error {
	if len(request.Args) != 1 && len(request.Args) != 2 {
		return wrongNumberOfArgumentsError(name)
	}
	if len(request.Args) == 2 {
		count, err := strconv.ParseInt(string(request.Args[1]), 10, 64)
		if err != nil {
			return ErrValueNotIntegerError()
		}
		if count < 0 {
			return newRESPMessageError("ERR", "value is out of range, must be positive")
		}
	}
	return nil
}
