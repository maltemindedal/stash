package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/storage"
)

// CommandExecutor runs the requests the server receives. The server cannot
// import the command package that implements it, so New builds it once, from
// Services, with the NewExecutorFunc it is given. A method renamed on either
// side is a compile error rather than a feature that silently switches off.
type CommandExecutor interface {
	// ExecuteSequenced runs a request ordered against the other requests (see
	// the command package's sequencer). When the result has a Release
	// function, the caller calls it once it has applied the result's
	// durability and propagation frames, and not before.
	ExecuteSequenced(context.Context, protocol.Value) (ExecuteResult, error)
	// ExecuteDetailed runs a request without ordering it. AOF replay uses it,
	// before anything else runs.
	ExecuteDetailed(context.Context, protocol.Value) (ExecuteResult, error)
}

// backgroundWriteSequencer orders work the server does on its own (the expiry
// sweep, the snapshot of an AOF rewrite) against client requests. The returned
// function is called when that work's frames have been applied. It stays
// optional until the background runners' wiring is made required (#44).
type backgroundWriteSequencer interface {
	BeginBackgroundWrite() (release func())
}

// NewExecutorFunc builds the command executor from the server's Services. New
// calls it once.
type NewExecutorFunc func(Services) (CommandExecutor, error)

// Services is everything the command executor needs from the server. New fills
// every field, and the executor refuses a Services whose pointer or function
// fields are nil; RequirePass and SlowlogThreshold have meaningful zero values.
type Services struct {
	Store  *storage.Store
	Logger *slog.Logger
	// RequirePass is the password AUTH verifies. It never decides who may run
	// commands: each connection's Client state does.
	RequirePass string
	Watches     *WatchRegistry
	PubSub      *PubSubRegistry
	Slowlog     *SlowlogRegistry
	// SlowlogThreshold is how long a command runs before the Slowlog records
	// it; a negative threshold records nothing.
	SlowlogThreshold time.Duration
	Replication      *ReplicationState
	Replicas         *ReplicaRegistry
	// Stats is the snapshot of the server that INFO reports.
	Stats func() Stats
	// RewriteAOF starts the background AOF rewrite BGREWRITEAOF asks for, or
	// reports that there is no AOF.
	RewriteAOF func(context.Context) error
	// RecordsWrites reports whether anything records writes at the moment (an
	// AOF, a Replica attached or attaching), so that writes must be ordered.
	RecordsWrites func() bool
}
