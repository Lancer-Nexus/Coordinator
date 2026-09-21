# AGENTS.md – Lancer Nexus Coordinator

## Mission

Make deterministic, observable and failure-tolerant placement decisions for the cluster.

## Rules

- Treat Agent heartbeats and instance leases as time-bounded facts.
- Never assign a player to an instance that is not ready and registered.
- Keep group assignment atomic where possible; do not silently split a formation.
- Use stable instance IDs and idempotent assignment IDs.
- Prefer draining an instance over assigning new players to it.
- Event-server reservations require explicit capacity and lifecycle state.
- Do not mutate authoritative character data owned by Gateway or game servers.
- Record the reason for placement and rejection decisions for diagnostics.

## Verification

Test concurrent assignments, full capacity, stale heartbeats, Coordinator restart, Agent loss, group affinity and event reservation races.
