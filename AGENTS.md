# AGENTS.md – Lancer Nexus Coordinator

## Mission

Make deterministic, observable and failure-tolerant placement decisions for the cluster.

## MVP architecture baseline

- Coordinator owns placement, instance reservations and group affinity; it does not own identity, character persistence or live simulation.
- Registry freshness is determined by sequenced Agent/instance heartbeats; protected internal and placement HTTP routes require a configured bearer key.
- Registry snapshots persist to the configured filesystem path and recover on restart. The file provider is single-writer and single-process; do not claim multi-replica consistency until a transactional shared store with fencing is implemented.
- The optional QUIC listener is disabled by default, requires TLS 1.3 mTLS and a configured CA, and binds certificate DNS SAN identity to `ClusterHello.NodeId`.
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

Test concurrent assignments, full capacity, stale/replayed heartbeats, Agent loss, group affinity, reservation idempotency, filesystem restart recovery and client-certificate trust/identity. Multi-replica races require a transactional shared store and remain unimplemented.

For this repository's current implementation, update `Protocol` first and verify with:

```bash
git submodule update --init --remote --merge Protocol
dotnet restore tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj
dotnet format tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --verify-no-changes --no-restore
dotnet build tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-restore --warnaserror
dotnet test tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-build
```

## Working-model escalation

- If a task requires complex reasoning beyond the current model's reliable scope, ask the user whether switching to a stronger model is desired before continuing.
- Do not switch models silently or broaden the task because a stronger model may be useful.
