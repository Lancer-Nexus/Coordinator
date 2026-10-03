# AGENTS.md – Lancer Nexus Coordinator

## Mission

Make deterministic, observable and failure-tolerant placement decisions for the cluster.

## MVP architecture baseline

- Coordinator owns placement, instance reservations and group affinity; it does not own identity, character persistence or live simulation.
- Registry freshness is determined by sequenced Agent/instance heartbeats; protected internal and placement HTTP routes require a configured bearer key.
- Registry snapshots persist to the configured filesystem path and recover on restart. The file provider is single-writer and single-process; do not claim multi-replica consistency until a transactional shared store with fencing is implemented.
- The optional QUIC listener is disabled by default, requires TLS 1.3 mTLS and a configured CA, and binds certificate DNS SAN identity to `ClusterHello.NodeId`.
- It coordinates idempotent transfers through `Requested -> Reserved -> Prepared -> SourceFrozen -> TargetAccepted -> Committed -> SourceReleased`. The source instance remains authoritative until the atomic MySQL lease commit.
- Never expire `SourceFrozen`, `TargetAccepted` or `Committed` transfers automatically; they may represent frozen source state or a completed MySQL lease switch and require recovery. Hold target capacity until `SourceReleased`; retain released/aborted records long enough for idempotent retries.
- Character authority uses MySQL leases with monotonic `lease_version` fencing. The Coordinator must never permit a stale instance to become authoritative again.
- NPC identities are globally unique in the MySQL `npc_ownership_leases` table; its primary key and registration-key constraint are authoritative. Do not use the registry JSON snapshot, process memory or Redis to decide NPC ownership.
- NPC transfer snapshots and phase state live in the transactional MySQL journal. Keep the source lease authoritative through target acceptance; switch all ownership rows and increment every `ownership_version` in the same commit transaction. Never auto-expire a frozen NPC transfer; recover it from the journal.
- Bind SourceFrozen snapshots to the reserved target system (case insensitive) and exact optional MissionRuntimeId. Reject added, omitted or substituted mission associations before storing snapshot bytes or changing state.
- MissionRuntimeId must equal the shared character transfer ID before commit/abort. Confirm the permanent Gateway mission decision while holding the NPC journal lock; unconfigured, unavailable or mismatched authority leaves leases and journal unchanged. Require the same proof before returning terminal mission snapshots for simulation recovery. Never infer mission rollback authority from timeout or NPC journal state alone.
- Recovery requires exact snapshot fences: committed target leases equal captured source versions plus one; aborted source leases equal their captured versions. An instance match alone does not authorize replay after another round trip. Apply this to direct recovery and discovery pages.
- Redis is limited to transient distribution and presence. All coordination messages use versioned `Protocol` contracts and negotiated capabilities.

## Rules

- Treat Agent heartbeats and instance leases as time-bounded facts.
- On QUIC, bind every instance heartbeat to the Agent identity in the peer certificate; do not accept a heartbeat solely because it names an AgentId.
- Never assign a player to an instance that is not ready and registered.
- Keep group assignment atomic where possible; do not silently split a formation.
- Use stable instance IDs and idempotent assignment IDs.
- Prefer draining an instance over assigning new players to it.
- Event-server reservations require explicit capacity and lifecycle state.
- Do not mutate authoritative character data owned by Gateway or game servers.
- Record the reason for placement and rejection decisions for diagnostics.
- Validate and retain the optional private NpcTransferEndpoint independently of the game endpoint. NPC prepare/resolve responses prefer this advertised QUIC address and explicit port.

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

## Nexus baseline system groups

The base Nexus topology uses eight game instances, one per group: BR01-BR06 (`br-01`), BW01-BW10 (`bw-01`), EW01-EW05 (`ew-01`), IW01-IW06 (`iw-01`), KU01-KU06 (`ku-01`), LI01-LI05 (`li-01`), RH01-RH05 (`rh-01`), and `mixed-01` for all remaining registered systems. System nicknames are compared case insensitively and emitted lowercase. Folder names are not always world nicknames: `fp7` contains `fp7_system`; `intro` and `miners` are asset directories, not registered worlds.
InstanceHeartbeat.SystemIds lists all systems owned by an instance; an absent/empty list retains the legacy primary SystemId. Match every owned system during placement and return the requested system, while counting reservations/capacity once per instance. Reject ownership-set changes for an already registered instance; require an explicit drain/recovery/re-registration procedure. Do not create fake heartbeat registrations from configuration.

## NPC retirement

- Apply migration 003 before this revision uses the NPC database. NPC retirement
  is authoritative in MySQL; retain IDs permanently and increment ownership fences.
- Retirement requires the exact current instance/version and no active transfer.
  Never retire an NPC merely because a source/target heartbeat or timeout expired.
- Batch response persistence and lease updates commit together. Unknown outcomes
  retry the same request ID/payload; a different payload under that ID is rejected.
  Check individual entry results even when the batch HTTP response is successful.
- Keep terminal journal recovery restricted to active exact fences. Surviving group
  checkpoints and GameServer durable lifecycle notifications remain required work;
  do not claim those implemented from Coordinator tombstone tests alone.
