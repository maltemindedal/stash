package command

import (
	"context"
	"errors"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

func (e *Executor) handleHSet(ctx context.Context, request *Request) (protocol.Value, error) {
	key := string(request.Args[0])
	pairs := make([]storage.HashFieldValue, 0, (len(request.Args)-1)/2)
	for i := 1; i < len(request.Args); i += 2 {
		pairs = append(pairs, storage.HashFieldValue{
			Field: string(request.Args[i]),
			Value: request.Args[i+1],
		})
	}

	added, evicted, err := e.store.HSet(key, pairs)
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(request, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleHGet(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 2 {
		return nil, wrongNumberOfArgumentsError("HGET")
	}

	value, ok, err := e.store.HGet(string(request.Args[0]), string(request.Args[1]))
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}
	if !ok {
		return protocol.BulkString{Null: true}, nil
	}

	return protocol.BulkString{Data: value}, nil
}

func (e *Executor) handleHDel(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("HDEL")
	}

	key := string(request.Args[0])
	fields := make([]string, 0, len(request.Args)-1)
	for _, arg := range request.Args[1:] {
		fields = append(fields, string(arg))
	}

	removed, err := e.store.HDel(key, fields)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrWrongType):
			return nil, ErrWrongTypeError()
		case errors.Is(err, storage.ErrSyntax):
			return nil, ErrSyntaxError()
		default:
			return nil, err
		}
	}

	if removed > 0 {
		e.touchWatchKeys(key)
	}
	return protocol.Integer{Value: removed}, nil
}

func (e *Executor) handleHGetAll(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("HGETALL")
	}

	entries, err := e.store.HGetAll(string(request.Args[0]))
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	elements := make([]protocol.Value, 0, len(entries)*2)
	for _, entry := range entries {
		elements = append(elements, protocol.BulkString{Data: []byte(entry.Field)})
		elements = append(elements, protocol.BulkString{Data: entry.Value})
	}

	return protocol.Array{Elements: elements}, nil
}

func (e *Executor) handleSAdd(ctx context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("SADD")
	}

	key := string(request.Args[0])
	added, evicted, err := e.store.SAdd(key, request.Args[1:])
	if err != nil {
		return nil, storageCommandError(err)
	}

	e.recordWriteEffects(request, key, evicted)
	return protocol.Integer{Value: added}, nil
}

func (e *Executor) handleSIsMember(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 2 {
		return nil, wrongNumberOfArgumentsError("SISMEMBER")
	}

	exists, err := e.store.SIsMember(string(request.Args[0]), request.Args[1])
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	if exists {
		return protocol.Integer{Value: 1}, nil
	}
	return protocol.Integer{Value: 0}, nil
}

func (e *Executor) handleSRem(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) < 2 {
		return nil, wrongNumberOfArgumentsError("SREM")
	}

	key := string(request.Args[0])
	removed, err := e.store.SRem(key, request.Args[1:])
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrWrongType):
			return nil, ErrWrongTypeError()
		case errors.Is(err, storage.ErrSyntax):
			return nil, ErrSyntaxError()
		default:
			return nil, err
		}
	}

	if removed > 0 {
		e.touchWatchKeys(key)
	}
	return protocol.Integer{Value: removed}, nil
}

func (e *Executor) handleSMembers(_ context.Context, request *Request) (protocol.Value, error) {
	if len(request.Args) != 1 {
		return nil, wrongNumberOfArgumentsError("SMEMBERS")
	}

	members, err := e.store.SMembers(string(request.Args[0]))
	if err != nil {
		if errors.Is(err, storage.ErrWrongType) {
			return nil, ErrWrongTypeError()
		}
		return nil, err
	}

	elements := make([]protocol.Value, 0, len(members))
	for _, member := range members {
		elements = append(elements, protocol.BulkString{Data: member})
	}
	return protocol.Array{Elements: elements}, nil
}

func validateHSetRequest(request *Request) error {
	if len(request.Args) < 3 || len(request.Args)%2 == 0 {
		return wrongNumberOfArgumentsError("HSET")
	}
	return nil
}
