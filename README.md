# Lancer Nexus Coordinator

NPC recovery checks exact ownership versions as well as instance identity. A
committed target may restore only the captured source version plus one; an
aborted source may restore only the captured source version. Older journals
remain durable but are excluded from discovery and target replay after later
transfers, including round trips back to the same instance. Recovery pages retain
only ID/version metadata while scanning stored snapshots and batch lease reads.

Instance heartbeats may advertise a separate `NpcTransferEndpoint` in
`quic://host:port` form. NPC preparation and target resolution prefer that private
endpoint; player placement continues to use the game endpoint. Legacy reports
without the field retain the configured NPC-port fallback on the sending server.

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

## Transfer reservation lifecycle

`POST /internal/v1/transfers/prepare` accepts the shared `TransferPrepareRequest`. It requires a fresh source instance and the exact requested target instance to be fresh, ready, non-draining, on the requested system and below capacity. It reserves one target slot until the request expires (maximum two minutes). Repeated requests with the same transfer ID and matching payload return the original prepared result; reusing an ID or idempotency key with different transfer data is rejected.

The source and target progress a transfer with `POST /internal/v1/transfers/{id}/source-frozen` and `/target-accepted`. Gateway confirms the lease switch using `/commit/{leaseVersion}` only after the MySQL lease transaction succeeds. Frozen, target-accepted and committed transfers are retained past ticket expiry for crash recovery and hold their target reservation until release. `/source-released` is valid only after commit and releases the target reservation; completed and aborted records remain for 24 hours to make retries idempotent. `POST /internal/v1/transfers/abort` releases reservations before commit; committed transfers cannot be rolled back through abort. `GET /internal/v1/transfers` exposes transfer state for internal diagnostics; `GET /internal/v1/transfers/{id}` retrieves one transfer for Gateway's target-ticket admission check. All endpoints require the internal bearer key.

Transfer reservations and lifecycle state are included in the registry snapshot. The file schema upgrades version 1 snapshots in memory and writes schema version 2 on the next state change. This is a Coordinator state-machine foundation; it does not implement the Gateway's MySQL lease transaction, game-state snapshot transport, jump-gate hook, target attach ticket validation or client reconnect yet. Those steps are required for a complete in-game instance switch.

## NPC identity and ownership registry

NPC transfer peers must advertise `npc_ownership_v1` before using the central identity registry. Configure `ConnectionStrings__NpcOwnership` (or `Coordinator__NpcOwnershipConnectionString`) and apply `db/migrations/001_npc_ownership_registry.sql` before enabling the capability. Without a connection string, the registry endpoint returns HTTP 503 and the capability is omitted.

`POST /internal/v1/npcs/allocate` reserves a bounded block of IDs for the registered ready instance and owned system. The Coordinator generates the IDs and inserts the allocation journal and every lease in one MySQL transaction. The NPC ID is the lease table's primary key; exact retries with the same request ID return the original block, while changed retries are rejected. New leases start at `ownership_version=1`. This database record is authoritative; process memory and Redis are not ownership stores. The tables are separate from the registry JSON snapshot because simultaneous Coordinator replicas need database uniqueness and transaction semantics.

`POST /internal/v1/npc-transfers/prepare` reserves up to 256 currently leased NPCs as one unit. Both registered instances must be ready and advertise `npc_transfer_v1`; the target must own the requested system. The request and source leases are committed in one MySQL transaction, and transfer/idempotency conflicts are rejected. `/internal/v1/npc-transfers/{id}/phase` durably records `SourceFrozen` with its validated versioned snapshot and SHA-256, accepts `TargetAccepted`, and atomically changes all NPC owners plus increments their fencing versions at `Committed`. Retries of the current phase are idempotent; abort releases reservations only before commit. The `npc_transfer_journal` retains state and snapshot bytes across Coordinator restarts. Either transfer peer can fetch journal metadata and the snapshot hash from `GET /internal/v1/npc-transfers/{id}/recovery?instanceId=...`; set `includeSnapshot=true` only when recovery needs the full persisted payload. A target can enumerate committed transfers with `GET /internal/v1/npc-transfers/recovery?instanceId=...&limit=128&afterTransferId=...`; pages are keyset-paginated and include only transfers whose complete NPC set is still leased to that target. A source can enumerate pending handoffs with `GET /internal/v1/npc-transfers/source-recovery?instanceId=...&limit=128&afterTransferId=...`. Both pages use the shared `NpcTransferRecoveryPageV1` Protocol contract. Other instance IDs receive 404 on per-transfer recovery. Apply `db/migrations/001_npc_ownership_registry.sql` and `db/migrations/002_npc_transfer_recovery_indexes.sql` before enabling NPC identity and transfer capabilities.

The Client repository's LLServer overlays implement the NPC handoff path: asynchronous ID allocation, source-side snapshot and freeze, private mTLS QUIC transfer to the target, durable target staging, journal recovery and target restore/activation after lease commit. Mission NPC snapshots carry their player/mission association so the player and NPC ownership changes can be coordinated. The runtime snapshot contract is versioned; unsupported target capabilities are rejected. Apply the Client patch stack before enabling this path, and configure both peers with `npc_ownership_v1`, `npc_transfer_v1` and the private QUIC trust/certificate settings. The Coordinator journal remains the recovery authority across process restarts.

## QUIC mTLS handshake

The Coordinator can expose a QUIC/TLS 1.3 handshake listener. It is disabled by default. To enable it, configure `Coordinator__Quic__Enabled=true`, a stable `Coordinator__Quic__NodeId`, `Coordinator__Quic__ServerCertificatePath` (PFX with private key and Server Authentication EKU), and `Coordinator__Quic__ClientCaCertificatePath` (trusted CA certificate). The PFX password is supplied via `Coordinator__Quic__ServerCertificatePassword`; never commit certificate files or passwords. `Coordinator__Quic__ListenAddress` defaults to loopback and `Coordinator__Quic__Port` to UDP 7443. Keep the listener on a private interface.

Client certificates must chain to that configured CA, include Client Authentication EKU and contain exactly one non-wildcard DNS SAN. The SAN value must match the peer `ClusterHello.NodeId`. Revocation is not checked by this initial listener; issue short-lived client certificates and rotate them. Peers must separately trust the Coordinator server certificate. The Agent keeps the authenticated connection open and sends one `AgentHeartbeat` request per bidirectional QUIC stream; the Coordinator validates the heartbeat NodeId against the client certificate SAN and acknowledges its sequence. The Coordinator also accepts `InstanceHeartbeat` streams, requiring the heartbeat's AgentId to map to the authenticated certificate NodeId and a fresh registered Agent. When configured with a fresh LLServer status file, the Agent now reports instance readiness, player count and drain state; a drain flag makes the Coordinator exclude that instance from new placements while existing players remain connected. Lifecycle commands are not yet carried over this control channel. On Linux, install `libmsquic` 2.2+ and allow the configured UDP port. If QUIC is enabled but its platform dependency is unavailable, the Coordinator fails startup instead of advertising the listener as active.

Registry and placement timeouts can be overridden with `Coordinator__Registry__AgentHeartbeatTimeoutSeconds`, `Coordinator__Registry__InstanceHeartbeatTimeoutSeconds`, `Coordinator__Placement__MaximumHeartbeatAgeSeconds`, `Coordinator__Placement__ReservationLifetimeSeconds` and `Coordinator__Placement__GroupAffinityLifetimeSeconds`. All must be positive; defaults are 15, 15, 15, 15 and 30 seconds respectively.

## Development

```bash
git submodule update --init --remote --merge Protocol
dotnet restore tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj
dotnet build tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-restore --warnaserror
dotnet test tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-build
```

The optional MySQL journal recovery integration test runs when `LANCER_NEXUS_COORDINATOR_TEST_MYSQL` points to an isolated test server. It creates and drops a uniquely named test database and verifies transfer replay across fresh store instances.

The journal also binds each `SourceFrozen` snapshot to its reserved target system and optional mission runtime ID. A mismatched target or added, removed or substituted mission association returns `snapshot_reservation_mismatch`; state and snapshot remain unchanged. The MySQL integration test covers these rejections and valid mission association recovery after recreating the store.

### Mission character authority

Configure `Coordinator:NpcMissionAuthorityBaseUrl` as the private Gateway HTTPS origin and `Coordinator:NpcMissionAuthorityApiKey` with the dedicated key configured as `Gateway:NpcMissionAuthorityApiKey`. Use a distinct credential from game-instance keys and the Coordinator's inbound internal key. Gateway requires migration 005 and its decision-aware build. TLS trust uses the normal service certificate trust store; redirects are disabled and requests time out after eight seconds.

For mission-bound transfers, the mission runtime ID must equal the shared character transfer ID. Before NPC `Committed` or `Aborted`, the store holds its journal row lock while asking Gateway for the same permanent character decision. Gateway confirms the SQL character commit or durably vetoes later character commit before acknowledging abort. Only a successful reply bound to transfer, peers, system and requested decision authorizes the NPC transaction. If the reply is lost, no NPC journal or lease changes; retry the same operation. Terminal mission snapshot recovery also requires Gateway confirmation plus exact NPC ownership fences. Missing configuration/authority rejects mission operations and restoration; autonomous population transfers continue through their existing MySQL transaction.

This is a recoverable saga across service-owned schemas. SQL character commit and NPC commit occur in sequence, and the target simulation still waits for both. The MySQL tests cover rejected/unavailable authority without journal changes, confirmed commit/abort, retries and restart restoration; HTTP-client tests reject unavailable, malformed and mismatched replies. Real process-failure and live player/mission acceptance remain to be verified.

The implementation provides deterministic placement for registered, ready, fresh and non-draining instances. It prefers group affinity, then lower utilization, and rejects requests when there is no eligible capacity. The heartbeat/placement endpoints above are protected by the configured internal key.

Nexus group instances advertise multiple `InstanceHeartbeat.SystemIds`. Placement matches any owned system and returns the requested world; shared instance capacity and reservations are counted once. The canonical eight-group inventory is maintained in the Scripts repository.
