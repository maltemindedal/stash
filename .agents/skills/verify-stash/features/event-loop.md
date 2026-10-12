# Event-loop mode

`--event-loop` serves clients through epoll (Linux) or kqueue (macOS). Every feature must work in both networking modes.

## Sub-features

- `el-strings` runs the strings journey under `--event-loop`.

## How to get to it (user POV)

- Start the server with `--event-loop`.

## Driving it with verify.sh

Preconditions:

- Linux or macOS.

- **Launch.** Run `verify.sh launch --event-loop`.
- **Drive.** Run `verify.sh drive`, then `verify.sh stop`. Every reply matches.

## Gotchas

- `all` does not cover this mode. Run it separately whenever networking or command code changes.
- CI never runs the macOS kqueue poller. A Linux pass here says nothing about it.
