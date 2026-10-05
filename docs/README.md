# Stash documentation

Everything documented here is verified against the code. When the two disagree, the code wins. [Open an issue](https://github.com/maltemindedal/stash/issues) or fix the doc.

## Tutorial

Start here if you have never run Stash.

| Document | What it covers |
| --- | --- |
| [Getting started](getting-started.md) | Clone → build → run → store data → shut down. For newcomers; under 15 minutes. |

## How-to guides

Task-oriented. For someone with a server running who needs to accomplish something specific.

| Document | What it covers |
| --- | --- |
| [Persistence](guides/persistence.md) | Choosing between AOF and RDB, fsync policies, `BGREWRITEAOF`, load precedence. |
| [Setting up replication](guides/replication.md) | Running a master and replica, authenticating replicas, using `WAIT`. |
| [Securing a server](guides/securing-a-server.md) | `--requirepass`, binding non-loopback interfaces, and the current security limitations. |
| [Memory limits and eviction](guides/memory-and-eviction.md) | TTL eviction tuning and `--maxmemory` pressure eviction. |
| [Observability](guides/observability.md) | Reading `INFO`, configuring `SLOWLOG`, streaming with `MONITOR`. |

## Reference

Exhaustive and factual. For lookup, not reading front to back.

| Document | What it covers |
| --- | --- |
| [Configuration](reference/configuration.md) | Every command-line flag with type, default, and validation rules. |
| [Commands](reference/commands.md) | Every implemented command, its arity, and whether it replicates or persists. |

## Explanation

The "why" behind the design. For contributors and anyone reading the source.

| Document | What it covers |
| --- | --- |
| [Architecture overview](architecture/overview.md) | Package responsibilities, request flow, and the rationale behind sharding, the opt-in event loop, and the persistence/replication split. |
| [`GLOSSARY.md`](../GLOSSARY.md) | Domain glossary: the project's vocabulary. Lives at the repo root because agent tooling requires it there. |

## Contributing

| Document | What it covers |
| --- | --- |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Dev setup, the verification commands CI runs, test layout, and project conventions. |
| [`AGENTS.md`](../AGENTS.md) | Behavioral guidelines for LLM coding agents working in this repo. |
| [Agent support docs](agents/) | Issue tracker conventions, triage label vocabulary, and domain-doc consumption rules. |
