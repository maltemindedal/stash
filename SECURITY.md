# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability** (<https://github.com/maltemindedal/stash/security/advisories/new>). Do not open a public issue for a suspected vulnerability.

Include the Stash version or commit, how the server was started (flags, `--event-loop` or the default mode), and the smallest input that reproduces the problem.

## Supported versions

Stash has no tagged releases. Only the current `main` branch receives fixes.

## Scope

In scope: crashes, hangs, resource exhaustion, data loss or corruption, and authentication bypass reachable through the network protocol, the AOF and RDB loaders, or replication.

Out of scope, because they are documented limits (see [Securing a server](docs/guides/securing-a-server.md)): the lack of TLS and ACLs, and exposure that follows from binding a public interface without `--requirepass`.
