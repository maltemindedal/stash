# AOF persistence

With `--aof`, every write is logged, and a restarted server replays the log so the data survives.

## Sub-features

- `aof-write` records a SET in the AOF file on disk.
- `aof-replay` returns the value for `GET` after a restart on the same AOF.

## How to get to it (user POV)

- Run `stash --aof appendonly.aof`, write, stop, and start it again with the same flag.

## Driving it with verify.sh

Preconditions:

- `doctor` passes.

- **Persist.** Run `verify.sh persist`. The output shows `PASS AOF on disk records the SET` and `PASS GET after restart returns the AOF value`.
- **Proof.** `appendonly.aof.copy`, `persist-set.*` and `persist-get.*`.

## Gotchas

- `--dump ""` is passed so shutdown writes no `dump.rdb`. The test proves AOF replay alone.
