# AGENTS.md – Lancer Nexus Coordinator

## Mission

Make deterministic, observable and failure-tolerant placement decisions for the cluster.

## MVP architecture baseline

- Coordinator owns placement, instance reservations and group affinity; it does not own identity, character persistence or live simulation.
- It coordinates idempotent transfers through `Requested -> Reserved -> Prepared -> SourceFrozen -> TargetAccepted -> Committed -> SourceReleased`. The source instance remains authoritative until the atomic MySQL lease commit.
- Character authority uses MySQL leases with monotonic `lease_version` fencing. The Coordinator must never permit a stale instance to become authoritative again.
- Redis is limited to transient distribution and presence. All coordination messages use versioned `Protocol` contracts and negotiated capabilities.

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
