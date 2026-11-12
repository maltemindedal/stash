# Using an Output

An `Output` is everything one connection is sent.
Callers queue output, ask a named question, or close the connection.
No caller sees a byte count it could compare with a threshold, and no caller reads a clock.

The package is `internal/server/output`.
`ClientState.Output()` returns `*output.Output`.
The replication Master's port (`replication.Output`) and the registries call the same five methods as today, so their code does not change.

## The connection's own goroutine (`handler.go`)

```go
out := output.NewBlocking(s.outputs, conn, logger)   // conn is the net.Conn
state.BindOutput(out)
parser := protocol.NewParser(flushBeforeRead{conn: conn, out: out})

defer s.teardownClient(clientID, logger)
defer out.Close(nil)
defer out.Drain()        // returns once every reply queued so far is written, or the Output is closed

for {
	value, err := parser.Parse()     // flushBeforeRead.Read calls out.Flush() before each socket read
	if err != nil {
		...
		_ = out.Reply(protocol.ErrorValue{Message: "ERR " + err.Error()})
		return
	}
	responses, execErr := s.executeClientRequest(ctx, call, clientID, conn, logger, value)
	if execErr != nil {
		return
	}
	if err := out.Reply(responses...); err != nil {   // also when there are none: it releases held pushes
		return
	}
}
```

What left the handler:

- `if out.Backlog() >= outputWriteThrough { out.Flush() }`. `Reply` writes through when `Ledger.Full` says so. `Backlog` no longer exists.
- Nothing else. `Flush` and `Drain` keep their names and their callers (`flushBeforeRead`, `command/list.go`, `command/commands.go`).

`Flush` returns when `Ledger.Gate` is open.
`Drain` returns when `Ledger.RepliesOut` is true.
Neither holds a rule of its own.

## The event loop (`eventloop.go`, `conn_machine.go`)

```go
// registerConn
conn.out = output.NewPolled(l.srv.outputs, func() { l.markDirty(conn) }, conn.logger)

// connReadable: read only while the gate is open
if conn.out.Status().MayRead {
	... syscall.Read, conn.machine.Feed ...
}

// processConn: run requests while the gate is open
for conn.machine.State() != ConnStateClosed {
	if !conn.out.Status().MayRead {
		if st := l.service(conn); st.Closed != nil || !st.MayRead {
			break
		}
	}
	more, err := conn.machine.ProcessNext(conn.ctx, conn.run)   // ends in out.Reply(responses...)
	...
}

// finishConnEvent: one call, then act on what it says
st := conn.out.Service(fdWriter{fd: conn.fd})
switch {
case st.Closed != nil:
	l.closeConn(conn, st.Closed)          // a limit, the budget, the stall rule, the auth timeout, shutdown
	return
case conn.machine.State() == ConnStateClosing && st.RepliesOut && conn.machine.PendingRequests() == 0,
	conn.peerClosed && st.RepliesOut && conn.machine.PendingRequests() == 0:
	l.closeConn(conn, conn.machine.Err())
	return
case st.EndOfStream && !conn.writeShut:
	syscall.Shutdown(conn.fd, syscall.SHUT_WR)
	conn.writeShut = true
}
l.setInterest(conn, !conn.peerClosed && st.MayRead, st.WantWrite)
l.wakeAt(st.TryAgainAt)                   // one timer for the loop; zero means none
```

What left the event loop:

- `eventLoopOutputHighWater` and its four comparisons with `PendingOutputBytes()`. The loop asks `MayRead`, which is the same `Ledger.Gate` that `Flush` waits for.
- `loopOutputDriver` with its `flush`, `shut`, `closeWrite`, `peerClosed` and `retry`. The loop passes one function, `attention`.
- `closeReqs` and `requestClose`. A closed Output calls `attention`, and `Service` reports `Closed`. `eventConnHandle.Close` calls `conn.out.Close(cause)`.
- The stall rule's timer and `retry`. The loop wakes at `TryAgainAt` and calls `Service`. That try is the verdict.

`ConnMachine.Flush(w)` becomes `m.out.Service(w)`.
`ConnMachine` keeps its 512 MiB cap as `m.out.Waiting() > m.maxWriteBuffer`, one of the listed callers of `Waiting`.

## Pushing from another connection (`pubsub_registry.go`, `monitor_registry.go`, `replication/master.go`)

Unchanged.

```go
// replication.Master.Propagate, under m.mu
for id, replica := range m.replicas {
	if replica.out.Push(payload) {
		queued++
		continue
	}
	m.detachLocked(id)
	refusals = append(refusals, refusal{replica: replica, cause: replica.out.Err()})
}
```

`Push` never waits.
It returns false when the connection is closed, or when this `Push` closed it for passing its limit or the budget.

## A request that changes what is pushed to its own connection

Unchanged.

```go
// replication.Master.reserve, inside the attach cut of a PSYNC
c.Out.MarkReplica()
c.Out.HoldPushes()
m.register(c, true)

// PubSubRegistry.Subscribe, MonitorRegistry.Add
state.Output().HoldPushes()
```

The connection's next `Reply` releases what was held, behind the reply, also when it carries no values.

## Shutdown and teardown

```go
replica.out.EndStream()     // Master.EndStreams: write what is queued, then shut the write side
out.Close(cause)            // from any goroutine, never waits
out.Err()                   // why it was closed, or nil
```

## Tests that drive a Client state without a server

```go
out := output.NewBlocking(output.NewShared(), wire, logger)   // net.Pipe() has write deadlines
```

A writer without `SetWriteDeadline` needs a three-line adapter.
The stall rule cannot interrupt a write to it, so a stall test uses a wire that has deadlines.

## Changing a rule

Open `internal/server/output/internal/ledger/ledger.go`.
The table at the top says which bytes each decision counts.
Change the method, then run `go test ./internal/server/output/...`.
`model_test.go` fails with the shortest sequence of events that breaks a rule, in the networking mode that breaks it.
