# Output: rationale

## Problem

`internal/server/output.go` is 1001 lines in which one mutex guards the bytes, five counters derived from them, a writer handover, a timer and two networking modes.
Every one of the 15 follow-up fixes changed which bytes a decision counts, or which actor may make it.
The stall rule counted what waited (22d6ba9), then what could be written (1bc2076), then what a writer had tried (c8a0832).
The request gate counted everything (before ca505a9), then reply bytes (ca505a9), then the bytes ahead of the latest reply (5a28666).
The handler's exit reused the gate's threshold and dropped up to 1 MiB of replies (721a457).
An agent that opens `output.go` sees `backlog`, `pendingLocked`, `replyBacklogLocked`, `offeredLocked` and `queueEndLocked` side by side, and nothing says which one a decision must use.

The rules also exist twice.
The goroutine mode decides in `goroutineOutputDriver.flush`.
The event loop decides in `eventloop.go`, by comparing `PendingOutputBytes()` with `eventLoopOutputHighWater` in four places.
ca505a9 fixed the first and says of the second "unchanged".
That is the PR #110 Bugbot finding, and it is still live there.
Measured on 93c4c76 with a copy of `TestTheMasterReadsAReplicasAcknowledgementsWhileItsStreamWaits` that reads INFO in place of WAIT: 5 s after the replica acknowledged, the default mode reports `slave0:...,offset=33554506` and `--event-loop` reports `offset=0`.
The cause is inferred from `finishConnEvent`, which drops read interest while the whole backlog is above 1 MiB.

Constraints the design keeps: the five-method port of `replication.Master`, the lock order (registry or Master, `Output.mu`, budget), no `Output.mu` across a socket write, `Push` and `Reply` usable under the gate and the stripes, Go 1.21, standard library only, and the no-copy path for a large reply (75f9704).

## Usage (caller's view)

See `usage.md`.
Callers keep `Reply`, `Push`, `HoldPushes`, `Flush`, `Drain`, `MarkReplica`, `EndStream`, `Close` and `Err`.
The handler loses its write-through check.
The event loop loses its threshold, its driver, its close-request list and the stall retry, and gets `Service(socket) Status`.

## Shape

Three parts (`sketch.go`, `ledger/ledger.go`).

**The Ledger decides.**
The connection's output is one stream, and the Ledger keeps positions in it: `taken`, `replyEnd`, `end`, plus `held`, the writer's `seal` and the socket's `offerEnd`.
Every decision is a named method that measures one distance, and the table at the top of the file lists them: `Gate`, `RepliesOut`, `Full`, `Push`, `Take`, `Took`.
It holds no bytes, lock, clock, socket or timer, so it is a comparable value that a test can copy.
It lives in `internal/server/output/internal/ledger`, so only the Output can import it, and it exports no position.
A caller cannot compute a gate of its own because the high-water mark is not reachable (per type-system-discipline, and the "importable internals" red flag).

**The Ledger answers with Effects.**
`Effects{StartPusher, Signal, Close, Deadline}` is everything the caller must do after an event.
The Output carries it out in one function, so no path can queue bytes and forget the wakeup, the pump or the budget (per boundary-discipline: pure core, thin shell).

**Only a try of the socket can stall a Replica.**
The stall verdict is the return value of `Took(n, now)`: nothing was taken, the stall timeout after the socket last took a byte or was first offered the bytes.
There is no timer and no `retry`.
`Take` returns a `Deadline`, and the writer makes sure a try ends by then: the goroutine mode sets it as the write deadline, the event loop wakes and calls `Service`.
The four stall fixes each moved the rule closer to the writer. Here it cannot be anywhere else.
With no timer there is nothing to keep a closed Output reachable (e3fce43, 145fa8b).

**One writer loop, three callers.**
`Take(role, now)` says how many bytes this writer may offer.
A `Requester` may write up to its `seal`, the end of the stream when it took the socket, which is exact to the byte.
878dcef counted buffers for this, and f6190ed had to document that a push can still join.
What is queued behind the seal gets a `Pusher`.

**One Output for both modes.**
`NewBlocking` and `NewPolled` differ in how an Effects is carried out, in one switch.
The event loop gets `Status`, whose fields are Ledger decisions.
A closed Output asks for the loop's attention like any other change, so `closeReqs` goes (ee2c8d2).

**Class is one value.**
`Plain`, `Subscriber` or `Replica` replaces `replica` plus `counted`.
The budget's total is the sum of `Waiting()` over Subscribers, reported from the one function that carries out Effects (derive, do not sync).

Interface depth: the callers' surface shrinks by `Backlog`, `writeTo`, `isReplica` and the six-method `outputDriver`, and grows by `Service` and `Status`.
Behind it are the queue, the two limits and the budget, the gate, the drain, the handover between writers and the stall rule.
The Ledger's surface is 14 methods for one caller. That is large, and it is the price of making each decision a name.

Not done: the bytes (`chain`) keep today's layout (`ahead`, `queued`, `held`, `spare`), including `isLargeReply`.
`HoldPushes` still depends on the next `Reply` being called.

## Synthesis decision

To be filled in by the orchestrator.

## Tradeoffs accepted

- We accept one more package level and a second representation of the queue (positions beside bytes) in exchange for decisions that a test can enumerate. A property test must keep `chain` length equal to `Ledger.Waiting()`.
- We accept that `Reply` may write to the socket in the goroutine mode in exchange for removing the handler's own threshold. Only the connection's goroutine calls `Reply` there.
- We accept that the stall rule needs a wire with write deadlines in exchange for no timer. `net.Conn` has them. A test that hands the Output a plain `io.Writer` gets no stall verdict while a write blocks.
- We accept a behavior change in the goroutine mode's stall rule: a write that returns at its deadline reports the bytes it took, so a Replica that took part of a 1 MiB chunk in 30 s is kept. Today it is closed, because progress is seen only when the chunk's write returns.
- We accept that the model is small (limits of 2 to 5 bytes, about 150,000 states per mode, 3 s) in exchange for running it in every `go test`. The rules compare distances, so they do not depend on the size of the numbers. That is an argument, not a proof.
- We accept a 630-line test for a 350-line file. A third of it is the seven past rules, kept so that the model's power is itself tested.

## Alternatives considered

- **Two lanes.** Replies in a buffer only the connection's goroutine appends to, pushes in another, and a run list that gives the order. No reply is ever encoded under the lock, so 75f9704 and 04772a0 disappear, and held pushes need no move. It lost because it changes the hot path's memory behavior, which PR #36 measured, and it does nothing for the 11 commits about decisions. It can replace `chain` later without touching the Ledger.
- **A dedicated writer goroutine per connection.** The handler never writes, so there is no handover and no seal. It hides the most, but every reply costs a goroutine wakeup. The handler writes its own replies today for that reason. Not measured here.
- **Keep one file, replace the counters with a `positions` struct and add tests.** The smallest change, and 5a28666 already went half way. It lost on the evidence: the rules would stay reachable from `eventloop.go`, and the second copy of the gate is the one that is still wrong.
- **Keep the stall timer and model it.** The model would then check that the timer asks the writer. It lost because the timer is what three fixes were about, and deleting it deletes them.

## Implementation reconciliation

Nothing implemented yet beyond the Ledger prototype.
The prototype omits `EndStream` (`ending`, `Effects.ShutWrite`) and the Push budget, which the real Ledger needs.

## Open questions and risks

- Should the event loop hold requests back by the same gate as the goroutine mode? The design says yes, and it changes `--event-loop`: a Replica's acknowledgements are read while its stream waits, and a subscriber more than 1 MiB behind on messages pushed after its latest reply still has its requests run. Its Push limit then bounds it, as in the default mode.
- Should a closing event-loop connection close once its replies are out, as the goroutine mode does since 721a457? Today it waits for everything, pushes included.
- Should `Drain` be bounded? A peer that shut its write side and never reads keeps a goroutine in `Drain` until shutdown, which is the second half of the Bugbot finding. The design makes this one line: `Take` returns a `Deadline` while a connection is draining, as it does for a Replica. It closes such a peer after the stall timeout, which is a product decision.
- Is the partial-chunk change to the stall rule (tradeoff 4) wanted?
- Does `SetWriteDeadline` on every Replica chunk cost anything measurable? It is one call per 1 MiB. Not measured.
- Risk: the event loop needs a timer to wake at `TryAgainAt`. It holds the loop, not a connection, but it is a new thing for the loop to get wrong.

## Next implementation step

Move `ledger.go` and `model_test.go` to `internal/server/output/internal/ledger`, add `EndStream` to both, and make today's `Output` call it for `Gate` and `RepliesOut` only, in both modes, which fixes the event loop's gate first.
