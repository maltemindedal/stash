# Domain docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`docs/adr/`.** Read any ADRs that cover the area you're about to work in.

If `docs/adr/` does not exist, proceed silently. Do not flag its absence or suggest creating it upfront. The `/domain-modeling` skill creates ADRs lazily when decisions get resolved. Reach it via `/grill-with-docs` and `/improve-codebase-architecture`.

## File structure

Single-context repo:

```
/
├── docs/adr/
│   ├── 0001-event-sourced-orders.md
│   └── 0002-postgres-for-write-model.md
└── src/
```

## Flag ADR conflicts

If your output contradicts an existing ADR, state the conflict instead of silently overriding the decision:

> _This contradicts ADR-0007, "Event-sourced orders," but it is worth reopening because..._
