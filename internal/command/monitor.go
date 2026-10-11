package command

import (
	"context"
	"fmt"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
)

func (e *Executor) handleMonitor(ctx context.Context, request *Request) (server.ExecuteResult, error) {
	if len(request.Args) != 0 {
		return server.ExecuteResult{}, wrongNumberOfArgumentsError("MONITOR")
	}

	state, err := clientStateOf(request)
	if err != nil {
		return server.ExecuteResult{}, err
	}
	// A replica's connection carries the replication stream alone, and a
	// MONITOR event pushed to it would land in the stream. As in Redis, whose
	// monitorCommand returns at once for a client that is already a replica,
	// it gets no reply and nothing changes.
	if state.IsReplica() {
		return server.ExecuteResult{}, nil
	}
	if state.InTransactionActive() {
		return server.ExecuteResult{}, fmt.Errorf("MONITOR inside MULTI is not allowed")
	}
	if state.IsSubscribed() {
		return server.ExecuteResult{}, ErrSubscribedModeOnlyError()
	}

	state.StartMonitoring()
	return server.SingleResponse(protocol.SimpleString{Value: "OK"}), nil
}
