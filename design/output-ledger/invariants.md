# Invariants the design checks

"Run" means the check exists in `design/output-ledger/ledger` and its result is in `proof.txt`.
"Designed" means the sketch states it and nothing runs yet.

## Checked by the exhaustive model (`ledger/model_test.go`)

The model drives a Ledger with small limits through every sequence of events, in both networking modes, and compares each decision with a restatement that keeps the waiting bytes as tags and no positions.
`TestTheModelRejectsEachRuleAPastCommitHadToReplace` puts each past rule back and requires the model to reject it.

| # | Invariant | Past bug it catches | Status |
|---|---|---|---|
| L1 | `Gate` is open exactly when no more than the high-water mark is left to write before the latest reply is out. Bytes pushed after that reply never close it. | Flush before ca505a9, and the event loop at 93c4c76 (the PR #110 Bugbot finding): rejected after `push of 3`. | Run, both modes |
| L2 | `Gate` counts what was pushed ahead of the latest reply. | ca505a9, replaced by 5a28666: rejected after `push of 3, reply of 1`. | Run |
| L3 | `RepliesOut` is true exactly when no reply byte is unwritten. | The handler before 721a457: rejected after `reply of 1`. | Run |
| L4 | A Replica is closed as stalled only by a try of its socket that took nothing, the stall timeout or more after the socket last took a byte or was first offered the bytes. Such a try always closes it. | The rule before 22d6ba9 and 1bc2076 (counted held and unflushed bytes), and 1bc2076's timer that c8a0832 replaced (never asked the event loop to try): both rejected. | Run |
| L5 | A Requester is never given a byte queued after it took the socket. | Flush before 878dcef: rejected. Removes the caveat f6190ed documents. | Run |
| L6 | A closed Ledger returns no Effects: no pump, no signal, no deadline. | MarkReplica before 145fa8b: rejected. | Run |
| L7 | Held bytes are never offered to the socket, and enter the stream directly behind the reply that releases them. | 98bc2fe (order of a push and a reply), 22d6ba9. | Run, no past rule reconstructed |
| L8 | While a pushed byte is writable, somebody has the socket. | The handover 75f9704 and 878dcef changed (`errMoreQueued`). A hand mutation of `Reply` was rejected after `hold, push of 1, reply of 1`. | Run |
| L9 | A Push is accepted exactly when what counts against the class's limit fits: everything for a Subscriber, the bytes behind the latest reply for a Replica. | `TestAReplicasSnapshotReplyIsNotCountedAgainstItsStreamLimit` (5a28666's first use of `replyBacklogLocked`). | Run, no past rule reconstructed |
| L10 | An event that opens the gate or writes the last reply byte signals the waiters. | The `drained.Broadcast` conditions that ca505a9, 5a28666 and 721a457 each edited. | Run, no past rule reconstructed |
| L11 | `Waiting`, `Writable` and `Full` agree with the bytes. | The handler's and the event loop's own readings of `Backlog()`. | Run |

## Checked by types and package boundaries

| # | Invariant | How | Past bug it catches | Status |
|---|---|---|---|---|
| T1 | No file outside the Ledger can compare a byte count with a threshold. | The limits and positions are unexported in `internal/server/output/internal/ledger`. `Output` exports no `Backlog`. The loop gets `Status.MayRead`. | The second copy of the gate in `eventloop.go`, which ca505a9 left as it was. | Designed. The measurement in `rationale.md` shows the copy is wrong today. |
| T2 | A stall verdict cannot be reached without a try's result. | The verdict is the return value of `Took(n, now)`. No other method returns `ErrStalled`, and there is no timer entry point. | 22d6ba9, 1bc2076, c8a0832. | Run: the Ledger has no such method, and L4 checks the value. |
| T3 | Nothing outside a connection holds its Output for the stall rule. | The package starts no timer. The goroutine mode uses the socket's write deadline, the event loop one timer of its own. | e3fce43, 145fa8b. | Designed |
| T4 | A Replica is not counted toward the Push budget. | `Class` is one value. | The `replica` and `counted` pair that `Push`, `MarkReplica` and `Close` each keep in step. | Run (the type), budget designed |
| T5 | Every effect of an event is carried out. | `Effects` is one struct, acted on by one function. | The wakeup, pump start and budget update repeated in `Reply`, `Push`, `write` and `Close`. | Designed |
| T6 | The event loop closes a connection only through its Output. | `closeReqs` is gone. `Status.Closed` is the request. | ee2c8d2. | Designed |

## Checked by other tests

| # | Invariant | How | Past bug it catches | Status |
|---|---|---|---|---|
| P1 | The bytes on the wire are the replies and pushes in the order they were produced. `chain` length equals `Ledger.Waiting()` after every operation. | Property test: random events on a real `Output` in both modes, with tagged frames, against an in-memory socket that takes random amounts. | 98bc2fe, `TestALargeReplyKeepsItsPlaceAmongWhatIsQueued`. | Designed |
| P2 | Both modes give the same decisions for the same events. | The model already runs both. One table of scenarios through `NewBlocking` and `NewPolled`, as 7470fcf does. | ca505a9's "the event loop's high-water checks are unchanged". | Model run, table designed |
| P3 | `Waiting()` has no caller that decides from it. | A test in `internal/server` that scans the module for calls and fails on a file outside its list (INFO, `ConnMachine`'s 512 MiB cap). | `handler.go` and `eventloop.go` reading `Backlog()`. | Designed |
| P4 | A closed Replica's Output is collectable at once. | Keep `TestAClosedReplicasOutputIsNotKeptForTheStallTimeout` and `TestEventLoopKeepsNoConnectionItClosedOnRequest`. | e3fce43, 145fa8b, ee2c8d2. | Existing tests |
| P5 | A large reply is encoded before the lock. | Keep `TestALargeReplyIsEncodedWhileAPushHoldsTheOutputsLock`. | 75f9704. | Existing test. The design does not improve on it. |

## Not caught by this design

- The doc drift of 04772a0 (`isLargeReply` counts only top-level bulk strings). The rule stays in `chain` with its comment.
- A handler that returns after `HoldPushes` without calling `Reply`. Both networking modes call `Reply` in one place each, and no past commit got it wrong.
- 48789d0. The event loop can still take two stall timeouts to close a Replica whose socket takes a few bytes at the first try. `Took` restarts the count on any progress, by design.
