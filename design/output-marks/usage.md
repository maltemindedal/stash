# Output: usage

This is the caller's view of the proposed `internal/server/output` package, written before the types in `sketch.go`. Its public surface is close to today's `Output`. The change is behind it, so most call sites change by a rename or not at all.

A connection's own side holds an `*output.Output`. In the goroutine mode that is the handler. In the event loop it is the `eventConn`. Everyone else, which means the registries and the replication Master, holds an `output.Port`. `ClientState.Output()` returns the `Port`, so a registry cannot call `Flush`, `Finish` or `Reply`, because they are not on that type.

No caller compares byte counts. A caller asks a rule (`MayReadNext`, `Drained`) or calls a method that asks one. `Backlog()` exists for INFO only.

## Handler, goroutine mode (`internal/server/handler.go`)

```go
out := s.outputs.NewBlocking(conn, logger)
state.BindOutput(out.Port())

defer s.teardownClient(clientID, logger)
// The only way the handler closes its connection. Finish waits until every
// Reply queued so far is written, by this goroutine or the pump, and then
// closes. There is no separate Drain and Close to put in the wrong order,
// and no mark to pass.
defer out.Finish()

stopClose := context.AfterFunc(ctx, func() {
	if !state.IsReplica() {
		out.Port().Abort(nil)
	}
})
defer stopClose()

parser := protocol.NewParser(flushBeforeRead{conn: conn, out: out}) // Read calls out.Flush first

for {
	value, err := parser.Parse()
	if err != nil {
		// ... unchanged: EOF returns; a bad frame gets one error Reply, then return.
		_ = out.Reply(protocol.ErrorValue{Message: "ERR " + err.Error()})
		return
	}
	responses, execErr := s.executeClientRequest(ctx, call, clientID, conn, logger, value)
	if execErr != nil {
		return
	}
	if err := out.Reply(responses...); err != nil { // none is allowed, and releases what HoldPushes held
		return
	}
	if err := out.FlushIfFull(); err != nil { // rules.WriteThroughDue, not Backlog() >= 4 KiB here
		return
	}
}
```

`Flush` waits only while `MayReadNext` is false. Today the same rule is written twice, once in `goroutineOutputDriver.flush` and once in the signal in `Output.write`. Here it is one function, and the event loop asks the same one.

## Event loop (`internal/server/eventloop.go`, `conn_machine.go`)

```go
conn.out = l.srv.outputs.NewPolled(loopHooks{loop: l, conn: conn}, conn.logger)
state.BindOutput(conn.out.Port())

// connReadable: today `conn.machine.PendingOutputBytes() <= eventLoopOutputHighWater`.
if conn.out.MayReadNext() {
	// read up to eventLoopReadBudget and Feed the machine
}

// processConn
for conn.machine.State() != ConnStateClosed {
	if !conn.out.MayReadNext() {
		if !l.flushOnce(conn) {
			return
		}
		if !conn.out.MayReadNext() {
			break
		}
	}
	// ProcessNext ...
}

// flushOnce: one attempt at the socket. A stalled Replica comes back as
// errReplicaStalled from the attempt that took nothing.
if err := conn.out.WriteSome(fdWriter{fd: conn.fd}); err != nil && !errors.Is(err, errEventLoopWouldBlock) {
	l.closeConn(conn, err)
}

// finishConnEvent
wantRead := !conn.peerClosed && conn.out.MayReadNext()
l.setInterest(conn, wantRead, conn.out.WantsWrite())
```

`loopHooks` implements `output.LoopHooks`. `MarkDirty` lists the connection, `RequestClose` queues it on `closeReqs`, and `ShutWrite` sets `endStream`. These hooks only notify the loop. The stall rule's request for one more attempt arrives through `MarkDirty`, and the attempt's verdict comes back from `WriteSome`.

`ConnMachine.bufferResponses` asks `output.OverWriteBuffer` instead of comparing `PendingOutputBytes()` with `maxWriteBuffer`.

## Replication push (`internal/replication/master.go`)

There is no change. `replication.Output` stays a subset of `output.Port`.

```go
for id, replica := range m.replicas {
	if replica.out.Push(payload) { // never waits; may close the replica at its limit
		queued++
		continue
	}
	m.detachLocked(id)
	refusals = append(refusals, refusal{replica: replica, cause: replica.out.Err()})
}
```

## PSYNC's attach cut and HoldPushes

There is no change at the call site:

```go
// Master.reserve, inside the attach cut, before the Replica is registered.
c.Out.MarkReplica() // the shell reads the clock; a batch on offer starts the stall count
c.Out.HoldPushes()  // the stream propagated from here on waits behind the FULLRESYNC reply
m.register(c, true)
```

The connection's own side then replies with `+FULLRESYNC` and the snapshot (`out.Reply(...)` in the handler, `bufferResponses` in the event loop). That Reply sets `ReplyEnd` before it gives the held stream positions, so the stream is `AfterReply` and never counts toward `MayReadNext`. The checker (`design/output-marks/proto`) holds every rule to that ordering.

## SUBSCRIBE and PUBLISH (`internal/server/pubsub_registry.go`)

There is no change:

```go
state.Output().HoldPushes() // under r.mu, so every later message follows the acknowledgement
...
if state.Output().Push(frame) { delivered++ }
```

`state.Output()` is a `Port`. A future edit that calls `state.Output().Flush()` under `r.mu` does not compile.
