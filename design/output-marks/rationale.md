# Output: rationale

## Problem

`internal/server/output.go` is one 1001-line type, `Output`, behind an `outputDriver` for each networking mode. Over two days it took 15 follow-up fixes, and several of them redid each other. Read together, the fixes fall into one class. Each rule about a connection's output (when Flush waits, when the teardown may close, what the push limit counts, when the stall count runs, what a batch holds) picks a region of the output, such as "unwritten bytes up to the latest Reply" or "bytes offered and not taken". Each rule then measures that region with whichever of 15 mutable fields was nearest: `backlog`, `replyWaiting`, `replyEnd`, `written`, `held`, `inflight`/`sent`, `ahead`/`queued`, or the driver's `writing`. Nothing stated a region in one place, and nothing checked a measure against its region. A fix that was right for the case in hand was wrong for the next one:

- ca505a9's `replyWaiting` was wrong for pushes ahead of a Reply, so 5a28666 followed.
- 22d6ba9 still counted bytes no writer had offered, so 1bc2076 followed.
- ca505a9 fixed the goroutine mode and left the event loop on the old rule, where it still is.

The Bugbot finding on PR #110 is the same class. It noticed a wait whose measure includes bytes the waiter does not need.

Constraints to honor:

- The lock order in AGENTS.md: a registry's or the Master's lock, then `Output.mu`, then the budget's. Nothing waits on the socket under any of them.
- `replication.Output` as the Master's port.
- `HoldPushes` inside the cut that changes what is pushed.
- Go 1.21 and the standard library only.
- Both networking modes.
- The memory lessons of e3fce43 and ee2c8d2.

## Usage (caller's view)

See `usage.md`. Call sites change little. The handler calls `out.Finish()` once in place of `Drain` plus `Close`. The event loop calls `conn.out.MayReadNext()` and `conn.out.WriteSome(w)` in place of comparing `PendingOutputBytes()` with a constant. Registries and the Master hold an `output.Port`, unchanged except that it lacks the owner's methods.

## Shape

Data first. A connection's output is one sequence of byte positions, and five numbers say where it stands (`Marks`):

- `Written`: what the socket took.
- `Offered`: the end of the batch a writer holds.
- `Queued`: the end of what can be written now.
- `ReplyEnd`: the end of the latest Reply.
- `Held`: bytes behind a Reply that has not come, which have no position yet.

Each of these has one meaning and one writer in the core. Every region a rule needs is a one-line method on `Marks` (`UntilReply`, `AfterReply`, `Unwritten`, `OnOffer`). Every rule is a one-line comparison in `rules.go`, which holds nothing else. This follows principle-model-the-domain: the regions are the domain, and the fields were a scattered encoding of them.

The core (`stream`) is pure. It holds no lock, reads no clock, does no I/O and starts no goroutine, and each transition returns an `Effects` value that the shell (`Output`) executes after it releases the lock (principle-boundary-discipline). Being pure is what lets a model checker enumerate every sequence of operations. It is also what makes some mistakes unrepresentable:

- `push` and `reply` take no `time.Time`, so they cannot start the stall count (22d6ba9, 1bc2076).
- Only `take` writes `Offered`, so a batch cannot grow (878dcef).
- `close` leaves marks that say nothing waits, and drops every buffer. A missing `closed` check then finds nothing to act on (145fa8b), and a leaked reference keeps a struct, not a snapshot (e3fce43, ee2c8d2).
- `tick` cannot close a stream held by a Polled writer. It can only ask for an attempt, and `wrote` gives the verdict (c8a0832). This removes the `stallDue` phase.

This follows principle-type-system-discipline and principle-encode-lessons-in-structure.

The drivers keep only I/O. In the goroutine mode, the connection's goroutine and the pump ask `flushStep(goal)` what to do next (write a batch, wait for the other writer, or return), with two goals, `UntilMayRead` and `UntilDrained`. In the event loop, the loop calls `WriteSome` and asks `MayReadNext`. Neither mode compares byte counts, so ca505a9's rule cannot be fixed in one mode and left in the other.

Two types split callers by role. `*Output` belongs to the connection's own side. `Port` is what `ClientState.Output()` returns to registries and the Master. Its methods never wait, so a call that would wait on a socket under a registry's lock does not compile.

The model test is the check that catches a new mistake at once. A reference keeps one tag per byte in the order the peer receives them, and defines each decision over those bytes in the plainest terms. For example, "flush waits while more than the high-water mark of unwritten bytes lies at or before the last Reply byte". The checker runs every sequence up to depth 6 or 7 and reports the shortest disagreement. `design/output-marks/proto` runs it on a cut-down core. It finds no disagreement in about 14 million sequences, and it rejects nine rules reconstructed from the parents of fix commits, each with the counterexample that commit's message describes.

Interface depth: the public surface is the same size as today's or smaller (`Drain` and `MarkReplica` timing go away, and `Finish`, `MayReadNext`, `WriteSome` and `FlushIfFull` come in). Behind it sit the whole queue, the writer hand-off, both limits, the budget, the stall rule and the buffer reuse.

## Synthesis decision

This is left for arena. This document is one runner's candidate.

## Tradeoffs accepted

- We accept a second representation of the same state, counters in `Marks` next to the buffers, in exchange for O(1) rules. The property test checks that the sum of the buffer lengths equals `Queued - Written`. Deriving the counters from the buffers would lose `Written`, which is cumulative.
- We accept one extra copy of every small Reply in exchange for deleting `isLargeReply`, the `ahead`/`queued` split and their documentation exceptions. Reply encodes into a scratch buffer outside the lock, then appends under it. This is unmeasured. It needs `scripts/bench.sh` on the handler benchmarks before it lands, and the fallback is to keep `isLargeReply` inside `Output.Reply` with the core unchanged.
- We accept an `Effects` struct returned by every transition, a small allocation-free value, in exchange for a core with no side effects.
- We accept a stall verdict at any Polled attempt that takes nothing once the timeout has passed, without waiting for the timer to have fired. This can close a stalled Replica a little sooner than today, never one that took a byte.
- We accept a reference model that states the stall rule with the same formula as the core. The value of the reference is in the regions it counts byte by byte, not in restating the timing.

## Alternatives considered

- **Keep `Output` and add tests only.** Its rules are spread over a mutex, a condition variable, a goroutine-starting driver and `time.Now`, so a test can sample schedules but cannot enumerate them. The past fixes each added a scenario test, and those tests did not catch the next case. This option hides nothing new and exposes the same 15 fields to every editor.
- **One Output type per networking mode.** Each type would be simpler, and each would carry its own copy of the rules. That copy is the drift that left the event loop on ca505a9's parent rule. The complexity moves into two places that must agree.
- **An actor per connection that owns its output, with pushes sent as messages.** This removes the lock, but a send under the Master's lock must never block, so the channel would need an unbounded queue, which is today's queue again. It also costs a goroutine per event-loop connection, which defeats that mode.
- **A TLA+ or statechart model kept beside the code.** It would find the design errors, but not the drift between the model and `output.go`, which is where several of these bugs lived. The checker here runs the shipped core under `go test`.

## Implementation reconciliation

Nothing is implemented yet beyond `design/output-marks/proto`. There, the prototype dropped `stallDue` (invariant 6) and found no disagreement in Polled mode.

## Open questions and risks

- **Should Finish be bounded?** This is the second half of the Bugbot finding. A subscriber that stops reading keeps its handler goroutine in `Finish` until a push limit or shutdown closes it. The shape supports a one-line change: the stall rule also counts while the connection is finishing (`StallClockRuns = (replica || finishing) && OnOffer > 0`), which closes such a peer after 30 seconds of taking nothing. That changes what clients see, so it is the owner's decision.
- **Should the event loop use the goroutine mode's read gate (invariant 2)?** Read from code, not run: today the event loop stops reading a replica's connection while more than 1 MiB of its stream waits, so `REPLCONF ACK`s go unread, and INFO's acknowledged offset should lag under `--event-loop`. Unifying the gate fixes that. It also lets a subscriber that is more than 1 MiB behind on messages keep having requests read, up to its 32 MiB push limit, as in the goroutine mode. Is that the intended behavior?
- **Can a large snapshot hit the event loop's write-buffer bound?** Read from code, not run: `ConnMachine.bufferResponses` fails a connection whose total backlog passes 512 MiB, so a FULLRESYNC snapshot larger than that should fail under `--event-loop`, while `docs/guides/replication.md` says a snapshot larger than 256 MiB can be sent. Which measure should `OverWriteBuffer` use, `UntilReply` or `Unwritten`?
- **Does the extra copy of small Replies cost throughput?** This is unmeasured. See Tradeoffs.
- **Where should the package live?** `internal/server/output` keeps it next to its one importer, and `server` must not be imported by it. Is a new package acceptable to the owner, as a pure `git mv` commit first?

## Next implementation step

Create `internal/server/output` with `stream.go`, `rules.go` and `model_test.go`, ported from `design/output-marks/proto` with byte buffers and the order check added, and with no callers. Then move `Output`'s shell onto it in a second commit.
