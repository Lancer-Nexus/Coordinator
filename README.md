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

## QUIC mTLS handshake

The Coordinator can expose a QUIC/TLS 1.3 handshake listener. It is disabled by default. To enable it, configure `Coordinator__Quic__Enabled=true`, a stable `Coordinator__Quic__NodeId`, `Coordinator__Quic__ServerCertificatePath` (PFX with private key and Server Authentication EKU), and `Coordinator__Quic__ClientCaCertificatePath` (trusted CA certificate). The PFX password is supplied via `Coordinator__Quic__ServerCertificatePassword`; never commit certificate files or passwords. `Coordinator__Quic__ListenAddress` defaults to loopback and `Coordinator__Quic__Port` to UDP 7443. Keep the listener on a private interface.

Client certificates must chain to that configured CA, include Client Authentication EKU and contain exactly one non-wildcard DNS SAN. The SAN value must match the peer `ClusterHello.NodeId`. Revocation is not checked by this initial listener; issue short-lived client certificates and rotate them. Peers must separately trust the Coordinator server certificate. The Agent keeps the authenticated connection open and sends one `AgentHeartbeat` request per bidirectional QUIC stream; the Coordinator validates the heartbeat NodeId against the client certificate SAN and acknowledges its sequence. The Coordinator also accepts `InstanceHeartbeat` streams, requiring the heartbeat's AgentId to map to the authenticated certificate NodeId and a fresh registered Agent. The current Agent worker does not yet produce instance telemetry, so this receiver is not a claim of live instance readiness. Lifecycle commands are not yet carried over this control channel. On Linux, install `libmsquic` 2.2+ and allow the configured UDP port. If QUIC is enabled but its platform dependency is unavailable, the Coordinator fails startup instead of advertising the listener as active.

Registry and placement timeouts can be overridden with `Coordinator__Registry__AgentHeartbeatTimeoutSeconds`, `Coordinator__Registry__InstanceHeartbeatTimeoutSeconds`, `Coordinator__Placement__MaximumHeartbeatAgeSeconds`, `Coordinator__Placement__ReservationLifetimeSeconds` and `Coordinator__Placement__GroupAffinityLifetimeSeconds`. All must be positive; defaults are 15, 15, 15, 15 and 30 seconds respectively.

## Development

```bash
git submodule update --init --remote --merge Protocol
dotnet restore tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj
dotnet build tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-restore --warnaserror
dotnet test tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-build
```

The implementation provides deterministic placement for registered, ready, fresh and non-draining instances. It prefers group affinity, then lower utilization, and rejects requests when there is no eligible capacity. The heartbeat/placement endpoints above are protected by the configured internal key.
