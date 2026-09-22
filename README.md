# Lancer Nexus Coordinator

The Coordinator maintains cluster membership and makes placement decisions for Lancer Nexus game instances.

## Responsibilities

- Register Agents and game instances
- Track readiness, capacity, draining and failure state
- Assign players and groups to suitable instances
- Preserve group and formation affinity
- Reserve event-server capacity
- Coordinate transfers without owning the game simulation
- Expose operator and health information

The Coordinator is a control-plane service. It does not become the authoritative source for player inventory, credits or world state.

## Shared Protocol

The shared contracts are checked out in the `Protocol` submodule. Update it before local builds with:

```bash
git submodule update --init --remote --merge Protocol
```

CI performs the same update before restoring and building the Coordinator.

## Heartbeat registry and placement API

Agents register through `POST /internal/v1/agents/heartbeat`; instances report readiness and capacity through `POST /internal/v1/instances/heartbeat`. `GET /internal/v1/registry` returns the current operator snapshot. `POST /api/v1/placement` selects a fresh, ready instance and reserves one player slot for 15 seconds. Agent and instance heartbeats expire after 15 seconds. Heartbeats require increasing sequence numbers; exact replays are accepted as duplicates without extending freshness.

All `/internal/*` and `/api/v1/placement` routes require `Authorization: Bearer <key>`. Configure `Coordinator__InternalApiKey` with a random secret of at least 32 UTF-8 bytes. If it is absent or too short, protected routes fail closed with HTTP 503. Terminate TLS and restrict network access at the deployment boundary; the Coordinator does not provide TLS itself.

Registry heartbeats, reservations, idempotency records and group affinity are saved to `data/coordinator-state.json` by default. Set `Coordinator__StateFile` to choose another path (for containers, mount a persistent writable volume there). Snapshots are written to a temporary file, flushed, then atomically renamed; malformed or unsupported state fails startup rather than silently discarding it. Configure backups and protect the file because it contains live cluster metadata.

The storage boundary is `ICoordinatorRegistryStore`, so a transactional database-backed implementation can replace the filesystem provider later. The current file provider is single-writer and only coordinates within one process; do not run multiple Coordinator replicas against the same file or assume multi-replica placement safety. For replicas, the replacement store must offer cross-process atomic transactions/locking, fencing and shared durable storage.

## Development

```bash
git submodule update --init --remote --merge Protocol
dotnet restore tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj
dotnet build tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-restore --warnaserror
dotnet test tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-build
```

The implementation provides deterministic placement for registered, ready, fresh and non-draining instances. It prefers group affinity, then lower utilization, and rejects requests when there is no eligible capacity. The heartbeat/placement endpoints above are protected by the configured internal key.
