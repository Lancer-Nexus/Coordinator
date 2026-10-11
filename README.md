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

The cluster-wide dynamic goods market is being implemented in stages. The Coordinator has a shared MySQL snapshot projection, authenticated internal read and reservation routes, an immutable catalog importer, and a transactional/idempotent trade-effect store with a QUIC handler. Gateway exposes public system snapshots and authenticated character reputation reads. The Coordinator can request snapshot-revision-bound reputation from Gateway over a dedicated HTTPS service credential. LLServer/Agent durable event production, initial live-stock seeding, reputation-aware price quotes and atomic trade booking remain open; the `dynamic_market_v1` feature capability stays disabled until those paths are complete. See [docs/dynamic-market-plan.md](docs/dynamic-market-plan.md).

## Shared Protocol

The shared contracts are checked out in the `Protocol` submodule. Update it before local builds with:

```bash
git submodule update --init --remote --merge Protocol
```

CI recursively checks out the `Protocol` revision pinned by the Coordinator repository before restoring and building.

## Heartbeat registry and placement API

Agents register through `POST /internal/v1/agents/heartbeat`; instances report readiness and capacity through `POST /internal/v1/instances/heartbeat`. `GET /internal/v1/registry` returns the current operator snapshot. `POST /api/v1/placement` selects a fresh, ready instance and reserves one player slot for 15 seconds. Agent and instance heartbeats expire after 15 seconds. Heartbeats require increasing sequence numbers; exact replays are accepted as duplicates without extending freshness.

All `/internal/*` and `/api/v1/placement` routes require `Authorization: Bearer <key>`. Configure `Coordinator__InternalApiKey` with a random secret of at least 32 UTF-8 bytes. If it is absent or too short, protected routes fail closed with HTTP 503. Terminate TLS and restrict network access at the deployment boundary; the Coordinator does not provide TLS itself.

Registry heartbeats, player and transfer reservations, event server assignments, idempotency records and group affinity are saved to `data/coordinator-state.json` by default. Set `Coordinator__StateFile` to choose another path (for containers, mount a persistent writable volume there). Snapshots are written to a temporary file, flushed, then atomically renamed; malformed or unsupported state fails startup rather than silently discarding it. Configure backups and protect the file because it contains live cluster metadata.

The storage boundary is `ICoordinatorRegistryStore`, so a transactional database-backed implementation can replace the filesystem provider later. The current file provider is single-writer and only coordinates within one process; do not run multiple Coordinator replicas against the same file or assume multi-replica placement safety. For replicas, the replacement store must offer cross-process atomic transactions/locking, fencing and shared durable storage. Registry schema 3 adds durable event assignments and migrates schema 1 and 2 snapshots without dropping existing records.

## Event server assignments

`POST /internal/v1/events/server-reservations` assigns one fresh, ready, non-draining instance that owns the requested system and has enough capacity for the event's participant maximum. The request binds the event ID, revision, time window, capacity and idempotency key. A matching retry returns the stable reservation and instance; changed event data conflicts. Concurrent requests are serialized by the registry lock. Overlapping event windows consume capacity together; non-overlapping events may share an instance. Ordinary player and transfer placement counts every unreleased event reservation conservatively, including future events.

`POST /internal/v1/events/server-reservations/release` releases capacity using the event and reservation IDs plus a release idempotency key. There is no automatic expiry: the Events service must release only after its authoritative terminal lifecycle decision. Release tombstones and assignments survive Coordinator restarts. Both routes require the internal bearer key and are advertised by `event_server_reservations_v1`.

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

The listener also accepts `FlHookEventV1` when `flhook_events_v1` was
negotiated. Configure `ConnectionStrings__FlHookEvents` or
`Coordinator__FlHookEventsConnectionString`; if unset, the existing NPC
ownership connection string is reused. Apply
`db/migrations/005_flhook_server_events.sql` first. The receiver checks that
the event's live instance belongs to the certificate-authenticated Agent and
persists the exact MessagePack payload under `(instance_id, producer_epoch,
sequence_number)`. It returns `Stored` or `Duplicate` only after the transaction
commits, `Conflict` for a reused identity with different content and
`Unavailable` when storage cannot be reached. The service capability is omitted
when no event database connection is configured. Authenticated Agent peers can
request bounded event replay for instances they currently own. Replay results
report cursor gaps and producer-epoch changes; responses stay within the
Protocol envelope payload limit. A local live check on 2026-10-10 used two real
.NET Agents over QUIC/mTLS to register LI01 and LI02 in the same Coordinator
registry. Fresh LLServer status showed LI01 draining and LI02 ready; Gateway
placed the test client on LI02. Its login/disconnect ran through Gateway and
LLServer, and the Coordinator MySQL event store acknowledged the ordered player
and character lifecycle events through sequence 38. This local check used
short-lived test certificates and is not a production deployment result.
Downstream event consumers and cross-system transfer remain outstanding.

## Dynamic market snapshot projection

The shared MySQL market read projection is introduced by
`db/migrations/006_dynamic_market_projection.sql`. Configure
`ConnectionStrings__Market` or `Coordinator__MarketConnectionString`; when
unset, the Coordinator reuses the FLHook event database connection. The
authenticated internal route `POST /internal/v1/market/snapshots/read` accepts
the versioned Protocol request and returns a keyset-paged system snapshot pinned
to its system revision. The revision table is provisioned by the catalog seed
importer. Unknown systems
and unavailable storage return HTTP 503; a changed pinned revision returns HTTP
409. The market is still staged: the public Gateway read facade and internal
Coordinator read/write/reservation paths exist, but live stock initialization,
LLServer/Agent event production, reputation-aware quotes and Gateway booking
are not connected end to end. The `dynamic_market_v1` capability remains off.

Generate and validate an import report from the local Freelancer installation:

```bash
python3 ../Scripts/bin/analyze-market-catalog.py --game "$FREELANCER_ROOT" --output /tmp/market-catalog.json
dotnet run --project tools/MarketCatalogSeeder/MarketCatalogSeeder.csproj --configuration Release -- --validate /tmp/market-catalog.json
```

The internal `POST /internal/v1/market/catalog/read` endpoint provides bounded keyset pages from `market_catalog_goods`, including goods without static offers. Pages are pinned to the imported seed SHA-256 so every Coordinator replica serves the same catalog revision.

Apply migrations 006 and 007 to the shared market database, configure
`ConnectionStrings__Market`, and run the same tool without `--validate` to
import. The import is one transaction, verifies complete catalog coverage, and
is idempotent for the exact report bytes. A different report is rejected until
an explicit catalog migration is provided. It loads all goods, all mapped
systems and market locations, and static offer rules. It does not treat INI
`Max` as live inventory. Consequently the snapshot route can return a valid
empty page after import; a stock initialization policy and live writer are still
required before the offers appear as market state.

Migration `008_market_trade_events.sql` adds the idempotency journal and the
Coordinator's transactional market trade store. Migration
`009_market_trade_reservations.sql` adds durable Gateway trade reservations and
reserved buy-stock accounting. Apply migrations 008 and 009 after 006/007
before enabling their consumers. On a negotiated
`dynamic_market_v1` QUIC session, the handler verifies that the mTLS Agent owns
the event's live instance, then commits event identity, stock delta, V1 price,
and market/system revisions atomically. Buy events reduce stock; sell events
add stock. The target market row must exist and permit that side. Retries return
the recorded market revision, while changed payloads under either event ID or
producer sequence conflict. The service does not advertise `dynamic_market_v1`
yet: LLServer/Agent durable event production, initial live-stock seeding and
the Gateway credit/inventory booking boundary remain required before runtime
enablement.

Coordinator can relay revision-bound character reputation through
`POST /internal/v1/market/reputation/read`. Configure
`Coordinator:GatewayMarketBaseUrl` with the private Gateway HTTPS origin and
`Coordinator:GatewayMarketApiKey` with the dedicated credential configured as
`Gateway:CoordinatorMarketApiKey`; this key is distinct from both the outbound
trade-reservation credential and other Gateway service keys. The call times out
after five seconds by default and fails closed if Gateway is unavailable or the
expected SaveGame snapshot revision has changed. The internal
`POST /internal/v1/market/quotes/read` uses this relay and the shared market row
to return a reputation-aware, revision-bound preview; it does not reserve stock
or authorize a trade. V1 applies a 10% price adjustment per full reputation
point (buy discount, sell premium), bounded to 0.90–1.10, after each market
marginal price is calculated.

The internal `POST /internal/v1/market/trades/reserve` and
`POST /internal/v1/market/trades/resolve` routes implement the reservation half
of the planned Gateway saga. Configure a distinct
`Coordinator:MarketGatewayApiKey` for them; the general internal key is not
accepted on these routes. Repeated `tradeId` plus identical reservation fields
returns its stored state; changed fields conflict. Buy reservations reduce
snapshot-visible stock until an explicit commit or abort. Reservations do not
expire on timeout because Gateway's durable decision may be unknown. The quote
uses the dynamic marginal price for each unit in the requested quantity:
`QuoteUnitPrice` is the first unit and `TotalPrice` sums the quantity across
successive stock levels. The Gateway Trade-Ledger/outbox and its caller are not
implemented yet, and this reservation price does not yet include faction
reputation.

Registry and placement timeouts can be overridden with `Coordinator__Registry__AgentHeartbeatTimeoutSeconds`, `Coordinator__Registry__InstanceHeartbeatTimeoutSeconds`, `Coordinator__Placement__MaximumHeartbeatAgeSeconds`, `Coordinator__Placement__ReservationLifetimeSeconds` and `Coordinator__Placement__GroupAffinityLifetimeSeconds`. All must be positive; defaults are 15, 15, 15, 15 and 30 seconds respectively.

## Development

```bash
git submodule update --init --remote --merge Protocol
dotnet restore tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj
dotnet build tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-restore --warnaserror
dotnet test tests/LancerNexus.Coordinator.Tests/LancerNexus.Coordinator.Tests.csproj --configuration Release --no-build
```

The MySQL integration tests run when `LANCER_NEXUS_COORDINATOR_TEST_MYSQL` points to an isolated test server. They create and drop uniquely named databases and exercise transfer-journal replay, checkpoint/retirement transactions, and mission-authority fail-closed behavior. CI provisions an ephemeral MySQL 8.0 service and runs these tests; locally, set the connection string only for a disposable test server.

The journal also binds each `SourceFrozen` snapshot to its reserved target system and optional mission runtime ID. A mismatched target or added, removed or substituted mission association returns `snapshot_reservation_mismatch`; state and snapshot remain unchanged. The MySQL integration test covers these rejections and valid mission association recovery after recreating the store.

### Mission character authority

Configure `Coordinator:NpcMissionAuthorityBaseUrl` as the private Gateway HTTPS origin and `Coordinator:NpcMissionAuthorityApiKey` with the dedicated key configured as `Gateway:NpcMissionAuthorityApiKey`. Use a distinct credential from game-instance keys and the Coordinator's inbound internal key. Gateway requires migration 005 and its decision-aware build. TLS trust uses the normal service certificate trust store; redirects are disabled and requests time out after eight seconds.

For mission-bound transfers, the mission runtime ID must equal the shared character transfer ID. Before NPC `Committed` or `Aborted`, the store holds its journal row lock while asking Gateway for the same permanent character decision. Gateway confirms the SQL character commit or durably vetoes later character commit before acknowledging abort. Only a successful reply bound to transfer, peers, system and requested decision authorizes the NPC transaction. If the reply is lost, no NPC journal or lease changes; retry the same operation. Terminal mission snapshot recovery also requires Gateway confirmation plus exact NPC ownership fences. Missing configuration/authority rejects mission operations and restoration; autonomous population transfers continue through their existing MySQL transaction.

This is a recoverable saga across service-owned schemas. SQL character commit and NPC commit occur in sequence, and the target simulation still waits for both. The MySQL tests cover rejected/unavailable authority without journal changes, confirmed commit/abort, retries and restart restoration; HTTP-client tests reject unavailable, malformed and mismatched replies. Real process-failure and live player/mission acceptance remain to be verified.

The implementation provides deterministic placement for registered, ready, fresh and non-draining instances. It prefers group affinity, then lower utilization, and rejects requests when there is no eligible capacity. The heartbeat/placement endpoints above are protected by the configured internal key.

Nexus group instances advertise multiple `InstanceHeartbeat.SystemIds`. Placement matches any owned system and returns the requested world; shared instance capacity and reservations are counted once. The canonical eight-group inventory is maintained in the Scripts repository.

## Durable NPC retirement

Apply `db/migrations/003_npc_retirement.sql` after migrations 001 and 002 before
starting this revision. The protected `POST /internal/v1/npc-retirements` route
implements Protocol's `npc_retirement_v1` batches. MySQL owns the tombstone;
retirement increments the ownership fence, preserves the identity/allocation row
permanently and rejects stale owners, stale versions and any pending transfer.
Retired IDs are excluded from new transfer claims and terminal snapshot recovery.
Allocation retries append `IsRetired` so consumers cannot treat retired IDs as
fresh spawns.

The request ID, exact MessagePack request and response are committed in the same
transaction as the lease changes. After uncertain delivery, repeat that ID and
payload to receive the persisted result. A conflicting payload is rejected.
Individual entry failures are returned inside a successful `processed` batch;
clients must inspect every result. A known `npc_transfer_in_progress` rejection
requires a new request ID after transfer resolution, whereas an unknown outcome
requires retrying the original request. Instance ownership is checked in SQL,
including for draining instances; registry timeout never authorizes retirement.

At most 256 distinct entries are accepted. Rows are locked in sorted ID order and
updated in one SQL statement. A stale entry does not block valid entries in the
same batch. Concurrent identical request IDs converge on one durable response.
Integration tests use separate temporary databases and cover duplicate delivery,
store recreation, stale/foreign ownership, pending transfers, 256-entry batches,
allocation replay and journal recovery exclusion.

GameServer death/docking notification and a durable outbox are not yet connected.
Old formation snapshots are currently eligible only when every member retains its
exact active fence. Retiring one member therefore blocks replay of that old group
snapshot; checkpoints of surviving members are still required for complete group
recovery. This service change alone does not establish crash-safe NPC lifecycle
or exact runtime continuation after death/docking.

Verification for the retirement implementation: 57 Protocol tests and all 44
Coordinator tests passed, with MySQL integration tests enabled against the isolated
local test server. The Coordinator Release build completed with zero warnings and
errors. New retirement source/test files pass the scoped format verification. The
full test-project format check still reports existing whitespace issues in
`MySqlNpcTransferStoreTests.cs` and `NpcMissionAuthorityClientTests.cs`; unrelated
formatting was preserved. No running cluster service has been upgraded yet.

### NPC runtime checkpoints

Migration `004_npc_checkpoints.sql` follows NPC retirement migration 003. The
protected `POST /internal/v1/npc-checkpoints` endpoint stores an ambient NPC
checkpoint, expected per-member revisions, formation state and optional retirement
entries in one MySQL transaction. Replaying the same request ID and exact payload
returns its recorded response. A changed payload under that ID conflicts.

Recovery discovery is available at `GET /internal/v1/npc-checkpoints/recovery` and
can be filtered by `instanceId` and `systemId`; system nicknames are compared
case-insensitively and normalized to lowercase. The per-checkpoint endpoint returns
bytes only after checking every active lease, owner, system, transfer and resulting
revision. GameServers drain pending writes and recover each system before publishing
the world. Transfer preparation must include complete prior checkpoint groups.
Legacy retirement cannot remove a checkpointed NPC alone.

The Coordinator does not yet accept mission checkpoint state: Gateway character
lease arbitration and deployment of migration 004 remain necessary before advertising
`npc_checkpoint_v1`. Mission NPCs remain excluded from this checkpoint path.
