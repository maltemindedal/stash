# Output design evidence

Two independent sketches of a model-checked Output, made for the agent-proofing plan in this branch's pull request. They are evidence for that plan, not code to merge. Delete this directory before the branch merges, or when the Output migration lands.

- `output-ledger/` is the chosen base: a pure `Ledger` sealed in an `internal` package, checked over every reachable state. Run `go test ./design/output-ledger/ledger -v`.
- `output-marks/` is the other candidate: five byte positions and a depth-bounded checker. Four of its ideas are grafted onto the base. Run `go test ./design/output-marks/proto -v`.

Both tests replay rules reconstructed from the parents of past fix commits and require each to be rejected.
