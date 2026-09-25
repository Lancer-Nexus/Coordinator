using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record CoordinatorRegistryOptions(
    TimeSpan AgentHeartbeatTimeout,
    TimeSpan InstanceHeartbeatTimeout,
    TimeSpan GroupAffinityLifetime)
{
    public static CoordinatorRegistryOptions Default { get; } = new(
        TimeSpan.FromSeconds(15),
        TimeSpan.FromSeconds(15),
        TimeSpan.FromSeconds(30));
}

public sealed record RegistryOperationResult(bool Accepted, string ReasonCode, bool Duplicate = false);

public sealed record AgentRegistryView(
    string AgentId,
    string NodeId,
    string BuildVersion,
    ulong Sequence,
    DateTimeOffset LastHeartbeatUtc,
    bool IsAlive);

public sealed record InstanceRegistryView(
    string AgentId,
    string InstanceId,
    string SystemId,
    bool IsReady,
    bool IsDraining,
    int CurrentPlayers,
    int ReservedPlayers,
    int MaxPlayers,
    string Endpoint,
    ulong Sequence,
    DateTimeOffset LastHeartbeatUtc,
    bool AgentIsAlive,
    bool IsAlive);

public sealed record PlacementOutcome(PlacementDecision Decision, bool Duplicate = false);
public sealed record TransferOperationOutcome(
    TransferPrepared Decision,
    TransferState State,
    string? TargetEndpoint,
    bool Duplicate = false);
public sealed record TransferOperationResult(
    bool Accepted,
    string ReasonCode,
    TransferState State,
    bool Duplicate = false);

public sealed class CoordinatorRegistry
{
    private readonly object sync = new();
    private readonly PlacementPolicy placementPolicy;
    private readonly CoordinatorRegistryOptions options;
    private readonly ICoordinatorRegistryStore store;
    private readonly Dictionary<string, AgentEntry> agents = new(StringComparer.Ordinal);
    private readonly Dictionary<string, InstanceEntry> instances = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ReservationEntry> reservations = new(StringComparer.Ordinal);
    private readonly Dictionary<string, GroupAffinityEntry> groupAffinities = new(StringComparer.Ordinal);
    private readonly Dictionary<Guid, TransferEntry> transfers = [];

    public CoordinatorRegistry(
        PlacementPolicy placementPolicy,
        CoordinatorRegistryOptions options,
        ICoordinatorRegistryStore? store = null)
    {
        this.placementPolicy = placementPolicy;
        this.options = options;
        this.store = store ?? new InMemoryCoordinatorRegistryStore();
        Restore(this.store.Load());
    }

    public RegistryOperationResult ApplyAgentHeartbeat(AgentHeartbeat heartbeat, DateTimeOffset receivedAtUtc)
    {
        ArgumentNullException.ThrowIfNull(heartbeat);
        lock (sync)
        {
            if (string.IsNullOrWhiteSpace(heartbeat.AgentId) || string.IsNullOrWhiteSpace(heartbeat.NodeId) ||
                string.IsNullOrWhiteSpace(heartbeat.BuildVersion) || heartbeat.Sequence == 0)
                return new(false, "invalid_heartbeat");
            if (heartbeat.ProtocolVersion != ProtocolConstants.ProtocolVersion)
                return new(false, "unsupported_protocol_version");

            if (agents.TryGetValue(heartbeat.AgentId, out var existing))
            {
                if (!string.Equals(existing.Heartbeat.NodeId, heartbeat.NodeId, StringComparison.Ordinal))
                    return new(false, "agent_identity_changed");
                if (heartbeat.Sequence < existing.Heartbeat.Sequence)
                    return new(false, "stale_sequence");
                if (heartbeat.Sequence == existing.Heartbeat.Sequence)
                    return new(true, "duplicate", Duplicate: true);
            }

            agents[heartbeat.AgentId] = new AgentEntry(heartbeat, receivedAtUtc);
            try
            {
                Persist();
            }
            catch
            {
                if (existing is null)
                    agents.Remove(heartbeat.AgentId);
                else
                    agents[heartbeat.AgentId] = existing;
                throw;
            }

            return new(true, "accepted");
        }
    }

    public RegistryOperationResult ApplyInstanceHeartbeat(InstanceHeartbeat heartbeat, DateTimeOffset receivedAtUtc)
        => ApplyInstanceHeartbeat(heartbeat, receivedAtUtc, authenticatedNodeId: null);

    public RegistryOperationResult ApplyInstanceHeartbeat(
        InstanceHeartbeat heartbeat,
        DateTimeOffset receivedAtUtc,
        string? authenticatedNodeId)
    {
        ArgumentNullException.ThrowIfNull(heartbeat);
        lock (sync)
        {
            if (string.IsNullOrWhiteSpace(heartbeat.AgentId) || string.IsNullOrWhiteSpace(heartbeat.InstanceId) ||
                string.IsNullOrWhiteSpace(heartbeat.SystemId) || string.IsNullOrWhiteSpace(heartbeat.Endpoint) ||
                heartbeat.Sequence == 0 || heartbeat.CurrentPlayers < 0 || heartbeat.MaxPlayers <= 0 ||
                heartbeat.CurrentPlayers > heartbeat.MaxPlayers)
                return new(false, "invalid_heartbeat");
            if (!agents.TryGetValue(heartbeat.AgentId, out var agent) || !IsFresh(agent.LastHeartbeatUtc, receivedAtUtc, options.AgentHeartbeatTimeout))
                return new(false, "agent_not_registered_or_stale");
            if (authenticatedNodeId is not null &&
                !string.Equals(agent.Heartbeat.NodeId, authenticatedNodeId, StringComparison.OrdinalIgnoreCase))
                return new(false, "agent_certificate_mismatch");

            if (instances.TryGetValue(heartbeat.InstanceId, out var existing))
            {
                if (!string.Equals(existing.Heartbeat.AgentId, heartbeat.AgentId, StringComparison.Ordinal))
                    return new(false, "instance_owner_changed");
                if (!string.Equals(existing.Heartbeat.SystemId, heartbeat.SystemId, StringComparison.Ordinal))
                    return new(false, "instance_system_changed");
                if (heartbeat.Sequence < existing.Heartbeat.Sequence)
                    return new(false, "stale_sequence");
                if (heartbeat.Sequence == existing.Heartbeat.Sequence)
                    return new(true, "duplicate", Duplicate: true);
            }

            instances[heartbeat.InstanceId] = new InstanceEntry(heartbeat, receivedAtUtc);
            try
            {
                Persist();
            }
            catch
            {
                if (existing is null)
                    instances.Remove(heartbeat.InstanceId);
                else
                    instances[heartbeat.InstanceId] = existing;
                throw;
            }

            return new(true, "accepted");
        }
    }

    public PlacementOutcome Place(PlacementRequest request, DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(request);
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            if (request.RequestId == Guid.Empty || request.SessionId == Guid.Empty ||
                string.IsNullOrWhiteSpace(request.TargetSystem) || string.IsNullOrWhiteSpace(request.IdempotencyKey))
                return new(Rejected(request.RequestId, "invalid_request", nowUtc));

            if (reservations.TryGetValue(request.IdempotencyKey, out var sameKey))
            {
                if (!Matches(sameKey, request))
                    return new(Rejected(request.RequestId, "idempotency_conflict", nowUtc));
                return new(WithRequestId(sameKey.Decision, request.RequestId), Duplicate: true);
            }

            var activeSessionReservation = reservations.Values
                .FirstOrDefault(entry => entry.SessionId == request.SessionId && entry.ExpiresUtc > nowUtc);
            if (activeSessionReservation is not null)
            {
                if (!Matches(activeSessionReservation, request))
                    return new(Rejected(request.RequestId, "session_already_reserved", nowUtc));
                return new(WithRequestId(activeSessionReservation.Decision, request.RequestId), Duplicate: true);
            }

            GroupAffinityEntry? affinity = null;
            if (!string.IsNullOrWhiteSpace(request.GroupId) &&
                groupAffinities.TryGetValue(request.GroupId, out var storedAffinity) &&
                storedAffinity.ExpiresUtc > nowUtc &&
                string.Equals(storedAffinity.SystemId, request.TargetSystem, StringComparison.Ordinal))
            {
                affinity = storedAffinity;
            }

            var hasGroupAffinity = affinity is not null;

            var candidates = BuildCandidates(nowUtc, hasGroupAffinity ? affinity : null).ToArray();
            if (hasGroupAffinity && candidates.All(candidate => !candidate.HasGroupAffinity ||
                    !placementPolicyCanAccept(request, candidate, nowUtc)))
            {
                return new(Rejected(request.RequestId, "group_instance_unavailable", nowUtc));
            }

            var decision = placementPolicy.Decide(request, candidates, nowUtc);
            if (!decision.Accepted || decision.InstanceId is null)
                return new(decision);

            var expiresUtc = new DateTimeOffset(DateTime.SpecifyKind(decision.ExpiresUtc, DateTimeKind.Utc));
            reservations[request.IdempotencyKey] = new ReservationEntry(
                request.SessionId,
                request.TargetSystem,
                request.GroupId,
                decision.InstanceId,
                decision,
                expiresUtc);

            GroupAffinityEntry? previousAffinity = null;
            var hadPreviousAffinity = !string.IsNullOrWhiteSpace(request.GroupId) &&
                                      groupAffinities.TryGetValue(request.GroupId, out previousAffinity);
            if (!string.IsNullOrWhiteSpace(request.GroupId))
            {
                groupAffinities[request.GroupId] = new GroupAffinityEntry(
                    request.TargetSystem,
                    decision.InstanceId,
                    nowUtc.Add(options.GroupAffinityLifetime));
            }

            try
            {
                Persist();
            }
            catch
            {
                reservations.Remove(request.IdempotencyKey);
                if (!string.IsNullOrWhiteSpace(request.GroupId))
                {
                    if (hadPreviousAffinity && previousAffinity is not null)
                        groupAffinities[request.GroupId] = previousAffinity;
                    else
                        groupAffinities.Remove(request.GroupId);
                }
                throw;
            }

            return new(decision);
        }
    }

    public TransferOperationOutcome PrepareTransfer(TransferPrepareRequest request, DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(request);
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();

            var invalid = ValidateTransferRequest(request, nowUtc);
            if (invalid is not null)
                return RejectedTransfer(request.TransferId, invalid, nowUtc);

            if (transfers.TryGetValue(request.TransferId, out var sameTransfer))
            {
                if (!Matches(sameTransfer.Request, request))
                    return RejectedTransfer(request.TransferId, "transfer_id_conflict", nowUtc);
                return TransferOutcome(sameTransfer, duplicate: true);
            }

            if (transfers.Values.Any(entry =>
                    string.Equals(entry.Request.IdempotencyKey, request.IdempotencyKey, StringComparison.Ordinal)))
                return RejectedTransfer(request.TransferId, "idempotency_conflict", nowUtc);

            var now = nowUtc.ToUniversalTime();
            if (!instances.TryGetValue(request.SourceInstanceId, out var source) ||
                !IsFresh(source.LastHeartbeatUtc, now, options.InstanceHeartbeatTimeout) ||
                !agents.TryGetValue(source.Heartbeat.AgentId, out var sourceAgent) ||
                !IsFresh(sourceAgent.LastHeartbeatUtc, now, options.AgentHeartbeatTimeout))
                return RejectedTransfer(request.TransferId, "source_instance_unavailable", nowUtc);

            if (string.Equals(request.SourceInstanceId, request.TargetInstanceId, StringComparison.Ordinal))
                return RejectedTransfer(request.TransferId, "same_instance", nowUtc);

            var target = BuildCandidates(now, affinity: null)
                .FirstOrDefault(candidate => string.Equals(candidate.InstanceId, request.TargetInstanceId, StringComparison.Ordinal));
            if (target is null)
                return RejectedTransfer(request.TransferId, "target_instance_unavailable", nowUtc);

            var placementRequest = new PlacementRequest
            {
                RequestId = request.TransferId,
                SessionId = request.SessionId,
                CharacterId = request.CharacterId,
                TargetSystem = request.TargetSystemId,
                GroupId = request.GroupId,
                IdempotencyKey = "transfer:" + request.IdempotencyKey
            };
            var placement = placementPolicy.Decide(placementRequest, [target], now);
            if (!placement.Accepted)
                return RejectedTransfer(request.TransferId,
                    target.SystemId == request.TargetSystemId ? "target_instance_unavailable" : "target_system_mismatch",
                    nowUtc);

            var reservationKey = placementRequest.IdempotencyKey;
            var expiresUtc = new DateTimeOffset(DateTime.SpecifyKind(request.ExpiresUtc, DateTimeKind.Utc));
            var reservedDecision = new PlacementDecision
            {
                RequestId = request.TransferId,
                Accepted = true,
                InstanceId = target.InstanceId,
                SystemId = target.SystemId,
                Endpoint = target.Endpoint,
                ReasonCode = "transfer_reserved",
                ExpiresUtc = expiresUtc.UtcDateTime
            };
            var transfer = new TransferEntry(request, reservationKey, TransferState.Prepared, expiresUtc, 0);
            reservations[reservationKey] = new ReservationEntry(
                request.SessionId, request.TargetSystemId, request.GroupId, target.InstanceId, reservedDecision, expiresUtc);
            transfers[request.TransferId] = transfer;
            try
            {
                Persist();
            }
            catch
            {
                transfers.Remove(request.TransferId);
                reservations.Remove(reservationKey);
                throw;
            }

            return new TransferOperationOutcome(
                new TransferPrepared
                {
                    TransferId = request.TransferId,
                    Accepted = true,
                    ExpiresUtc = expiresUtc.UtcDateTime,
                    ReasonCode = "prepared"
                },
                TransferState.Prepared,
                target.Endpoint);
        }
    }

    public TransferOperationResult AdvanceTransfer(
        Guid transferId,
        TransferState nextState,
        DateTimeOffset nowUtc,
        long leaseVersion = 0)
    {
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            if (!transfers.TryGetValue(transferId, out var entry))
                return new(false, "transfer_not_found", TransferState.Expired);
            if (entry.ExpiresUtc <= nowUtc)
                return new(false, "transfer_expired", entry.State);
            if (entry.State == nextState)
                return new(true, "duplicate", entry.State, Duplicate: true);
            if (!CanTransition(entry.State, nextState))
                return new(false, "invalid_transfer_transition", entry.State);
            if (nextState == TransferState.Committed && leaseVersion <= 0)
                return new(false, "invalid_lease_version", entry.State);

            var updated = entry with
            {
                State = nextState,
                LeaseVersion = nextState == TransferState.Committed ? leaseVersion : entry.LeaseVersion
            };
            transfers[transferId] = updated;
            if (nextState == TransferState.SourceReleased)
                reservations.Remove(entry.ReservationKey);
            try
            {
                Persist();
            }
            catch
            {
                transfers[transferId] = entry;
                if (nextState == TransferState.SourceReleased)
                    RestoreReservation(entry);
                throw;
            }
            return new(true, "accepted", nextState);
        }
    }

    public TransferOperationResult AbortTransfer(TransferAbort abort, DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(abort);
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            if (abort.TransferId == Guid.Empty || string.IsNullOrWhiteSpace(abort.ReasonCode))
                return new(false, "invalid_request", TransferState.Aborted);
            if (!transfers.TryGetValue(abort.TransferId, out var entry))
                return new(false, "transfer_not_found", TransferState.Expired);
            if (entry.State >= TransferState.Committed && entry.State <= TransferState.SourceReleased)
                return new(false, "transfer_already_committed", entry.State);
            if (entry.State == TransferState.Aborted)
                return new(true, "duplicate", entry.State, Duplicate: true);

            transfers[abort.TransferId] = entry with { State = TransferState.Aborted };
            reservations.Remove(entry.ReservationKey);
            try
            {
                Persist();
            }
            catch
            {
                transfers[abort.TransferId] = entry;
                RestoreReservation(entry);
                throw;
            }
            return new(true, "accepted", TransferState.Aborted);
        }
    }

    public TransferOperationResult CommitTransfer(Guid transferId, long characterId, long leaseVersion, DateTimeOffset nowUtc)
    {
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            if (!transfers.TryGetValue(transferId, out var entry))
                return new(false, "transfer_not_found", TransferState.Expired);
            if (entry.ExpiresUtc <= nowUtc)
                return new(false, "transfer_expired", entry.State);
            if (entry.Request.CharacterId != characterId || characterId <= 0)
                return new(false, "character_mismatch", entry.State);
            if (leaseVersion <= 0)
                return new(false, "invalid_lease_version", entry.State);
            if (entry.State == TransferState.Committed)
                return entry.LeaseVersion == leaseVersion
                    ? new(true, "duplicate", TransferState.Committed, Duplicate: true)
                    : new(false, "lease_version_conflict", entry.State);
            if (entry.State != TransferState.TargetAccepted)
                return new(false, "invalid_transfer_transition", entry.State);
            return AdvanceTransfer(transferId, TransferState.Committed, nowUtc, leaseVersion);
        }
    }

    public IReadOnlyList<PersistedTransfer> TransferSnapshot(DateTimeOffset nowUtc)
    {
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            return transfers.Select(pair => new PersistedTransfer(
                    pair.Key, pair.Value.Request, pair.Value.ReservationKey, pair.Value.State,
                    pair.Value.ExpiresUtc, pair.Value.LeaseVersion))
                .OrderBy(entry => entry.TransferId)
                .ToArray();
        }
    }

    public PersistedTransfer? GetTransfer(Guid transferId, DateTimeOffset nowUtc)
    {
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            return transfers.TryGetValue(transferId, out var entry)
                ? new PersistedTransfer(transferId, entry.Request, entry.ReservationKey, entry.State,
                    entry.ExpiresUtc, entry.LeaseVersion)
                : null;
        }
    }

    public (IReadOnlyList<AgentRegistryView> Agents, IReadOnlyList<InstanceRegistryView> Instances) Snapshot(DateTimeOffset nowUtc)
    {
        lock (sync)
        {
            if (CleanupExpired(nowUtc))
                Persist();
            var agentViews = agents.Values
                .OrderBy(entry => entry.Heartbeat.AgentId, StringComparer.Ordinal)
                .Select(entry => new AgentRegistryView(
                    entry.Heartbeat.AgentId,
                    entry.Heartbeat.NodeId,
                    entry.Heartbeat.BuildVersion,
                    entry.Heartbeat.Sequence,
                    entry.LastHeartbeatUtc,
                    IsFresh(entry.LastHeartbeatUtc, nowUtc, options.AgentHeartbeatTimeout)))
                .ToArray();

            var instanceViews = instances.Values
                .OrderBy(entry => entry.Heartbeat.InstanceId, StringComparer.Ordinal)
                .Select(entry =>
                {
                    var agentAlive = agents.TryGetValue(entry.Heartbeat.AgentId, out var agent) &&
                                     IsFresh(agent.LastHeartbeatUtc, nowUtc, options.AgentHeartbeatTimeout);
                    var instanceAlive = IsFresh(entry.LastHeartbeatUtc, nowUtc, options.InstanceHeartbeatTimeout);
                    return new InstanceRegistryView(
                        entry.Heartbeat.AgentId,
                        entry.Heartbeat.InstanceId,
                        entry.Heartbeat.SystemId,
                        entry.Heartbeat.IsReady,
                        entry.Heartbeat.IsDraining,
                        entry.Heartbeat.CurrentPlayers,
                        CountReservations(entry.Heartbeat.InstanceId, nowUtc),
                        entry.Heartbeat.MaxPlayers,
                        entry.Heartbeat.Endpoint,
                        entry.Heartbeat.Sequence,
                        entry.LastHeartbeatUtc,
                        agentAlive,
                        agentAlive && instanceAlive);
                })
                .ToArray();

            return (agentViews, instanceViews);
        }
    }

    private IEnumerable<InstanceCandidate> BuildCandidates(DateTimeOffset nowUtc, GroupAffinityEntry? affinity)
    {
        foreach (var entry in instances.Values)
        {
            var heartbeat = entry.Heartbeat;
            var agentAlive = agents.TryGetValue(heartbeat.AgentId, out var agent) &&
                             IsFresh(agent.LastHeartbeatUtc, nowUtc, options.AgentHeartbeatTimeout);
            var hasAffinity = affinity is not null &&
                              string.Equals(affinity.InstanceId, heartbeat.InstanceId, StringComparison.Ordinal);
            if (affinity is not null && !hasAffinity)
                continue;

            yield return new InstanceCandidate(
                heartbeat.InstanceId,
                heartbeat.SystemId,
                agentAlive,
                heartbeat.IsReady,
                heartbeat.IsDraining,
                heartbeat.CurrentPlayers,
                heartbeat.MaxPlayers,
                entry.LastHeartbeatUtc,
                heartbeat.Endpoint,
                hasAffinity,
                CountReservations(heartbeat.InstanceId, nowUtc));
        }
    }

    private bool placementPolicyCanAccept(PlacementRequest request, InstanceCandidate candidate, DateTimeOffset nowUtc)
    {
        var decision = placementPolicy.Decide(request, [candidate], nowUtc);
        return decision.Accepted;
    }

    private int CountReservations(string instanceId, DateTimeOffset nowUtc) => reservations.Values
        .Where(entry => string.Equals(entry.InstanceId, instanceId, StringComparison.Ordinal) && entry.ExpiresUtc > nowUtc)
        .Select(entry => entry.SessionId)
        .Distinct()
        .Count();

    private bool CleanupExpired(DateTimeOffset nowUtc)
    {
        var removedAny = false;
        foreach (var key in reservations.Where(pair => pair.Value.ExpiresUtc <= nowUtc).Select(pair => pair.Key).ToArray())
            removedAny |= reservations.Remove(key);
        foreach (var key in groupAffinities.Where(pair => pair.Value.ExpiresUtc <= nowUtc).Select(pair => pair.Key).ToArray())
            removedAny |= groupAffinities.Remove(key);
        foreach (var pair in transfers.Where(pair => pair.Value.ExpiresUtc <= nowUtc).ToArray())
        {
            if (pair.Value.State is TransferState.SourceReleased or TransferState.Aborted or TransferState.Expired or
                TransferState.Rejected or TransferState.TimedOut)
            {
                removedAny |= transfers.Remove(pair.Key);
            }
            else
            {
                transfers[pair.Key] = pair.Value with { State = TransferState.Expired };
                removedAny = true;
            }
        }
        return removedAny;
    }

    private void Restore(CoordinatorRegistryState state)
    {
        if (state.SchemaVersion != CoordinatorRegistryState.CurrentSchemaVersion ||
            state.Agents is null || state.Instances is null || state.Reservations is null ||
            state.GroupAffinities is null || state.Transfers is null)
            throw new InvalidDataException("Coordinator registry state has an unsupported or invalid schema.");

        foreach (var entry in state.Agents)
            if (!agents.TryAdd(entry.Heartbeat.AgentId, new AgentEntry(entry.Heartbeat, entry.LastHeartbeatUtc)))
                throw new InvalidDataException($"Duplicate persisted Agent ID '{entry.Heartbeat.AgentId}'.");
        foreach (var entry in state.Instances)
            if (!instances.TryAdd(entry.Heartbeat.InstanceId, new InstanceEntry(entry.Heartbeat, entry.LastHeartbeatUtc)))
                throw new InvalidDataException($"Duplicate persisted instance ID '{entry.Heartbeat.InstanceId}'.");
        foreach (var entry in state.Reservations)
            if (!reservations.TryAdd(entry.IdempotencyKey, new ReservationEntry(
                    entry.SessionId, entry.TargetSystem, entry.GroupId, entry.InstanceId, entry.Decision, entry.ExpiresUtc)))
                throw new InvalidDataException($"Duplicate persisted idempotency key '{entry.IdempotencyKey}'.");
        foreach (var entry in state.GroupAffinities)
            if (!groupAffinities.TryAdd(entry.GroupId, new GroupAffinityEntry(entry.SystemId, entry.InstanceId, entry.ExpiresUtc)))
                throw new InvalidDataException($"Duplicate persisted group ID '{entry.GroupId}'.");
        foreach (var entry in state.Transfers)
            if (!transfers.TryAdd(entry.TransferId, new TransferEntry(
                    entry.Request, entry.ReservationKey, entry.State, entry.ExpiresUtc, entry.LeaseVersion)))
                throw new InvalidDataException($"Duplicate persisted transfer ID '{entry.TransferId}'.");
    }

    private void Persist() => store.Save(new CoordinatorRegistryState(
        CoordinatorRegistryState.CurrentSchemaVersion,
        agents.Values.Select(entry => new PersistedAgent(entry.Heartbeat, entry.LastHeartbeatUtc)).ToArray(),
        instances.Values.Select(entry => new PersistedInstance(entry.Heartbeat, entry.LastHeartbeatUtc)).ToArray(),
        reservations.Select(pair => new PersistedReservation(
            pair.Value.SessionId,
            pair.Value.TargetSystem,
            pair.Value.GroupId,
            pair.Value.InstanceId,
            pair.Value.Decision,
            pair.Value.ExpiresUtc,
            pair.Key)).ToArray(),
        groupAffinities.Select(pair => new PersistedGroupAffinity(
            pair.Value.SystemId,
            pair.Value.InstanceId,
            pair.Value.ExpiresUtc,
            pair.Key)).ToArray())
    {
        Transfers = transfers.Select(pair => new PersistedTransfer(
            pair.Key,
            pair.Value.Request,
            pair.Value.ReservationKey,
            pair.Value.State,
            pair.Value.ExpiresUtc,
            pair.Value.LeaseVersion)).ToArray()
    });

    private static string? ValidateTransferRequest(TransferPrepareRequest request, DateTimeOffset nowUtc)
    {
        if (request.TransferId == Guid.Empty || request.SessionId == Guid.Empty || request.CharacterId <= 0 ||
            string.IsNullOrWhiteSpace(request.SourceInstanceId) ||
            string.IsNullOrWhiteSpace(request.TargetInstanceId) ||
            string.IsNullOrWhiteSpace(request.TargetSystemId) ||
            string.IsNullOrWhiteSpace(request.IdempotencyKey) || request.IdempotencyKey.Length > 128)
            return "invalid_request";
        if (request.ExpiresUtc.Kind != DateTimeKind.Utc)
            return "invalid_expiry";
        var expiry = new DateTimeOffset(request.ExpiresUtc);
        if (expiry <= nowUtc || expiry > nowUtc.AddMinutes(2))
            return "invalid_expiry";
        return null;
    }

    private static bool Matches(TransferPrepareRequest left, TransferPrepareRequest right) =>
        left.TransferId == right.TransferId && left.SessionId == right.SessionId &&
        left.CharacterId == right.CharacterId &&
        string.Equals(left.SourceInstanceId, right.SourceInstanceId, StringComparison.Ordinal) &&
        string.Equals(left.TargetInstanceId, right.TargetInstanceId, StringComparison.Ordinal) &&
        string.Equals(left.TargetSystemId, right.TargetSystemId, StringComparison.Ordinal) &&
        string.Equals(left.GroupId, right.GroupId, StringComparison.Ordinal) &&
        left.ExpiresUtc == right.ExpiresUtc &&
        string.Equals(left.IdempotencyKey, right.IdempotencyKey, StringComparison.Ordinal);

    private static bool CanTransition(TransferState current, TransferState next) => (current, next) switch
    {
        (TransferState.Prepared, TransferState.SourceFrozen) => true,
        (TransferState.SourceFrozen, TransferState.TargetAccepted) => true,
        (TransferState.TargetAccepted, TransferState.Committed) => true,
        (TransferState.Committed, TransferState.SourceReleased) => true,
        _ => false
    };

    private TransferOperationOutcome TransferOutcome(TransferEntry entry, bool duplicate)
    {
        var decision = new TransferPrepared
        {
            TransferId = entry.Request.TransferId,
            Accepted = entry.State is not (TransferState.Aborted or TransferState.Expired or TransferState.Rejected or TransferState.TimedOut),
            ExpiresUtc = entry.ExpiresUtc.UtcDateTime,
            ReasonCode = entry.State.ToString().ToLowerInvariant()
        };
        var endpoint = instances.TryGetValue(entry.Request.TargetInstanceId, out var target)
            ? target.Heartbeat.Endpoint
            : null;
        return new TransferOperationOutcome(decision, entry.State, endpoint, duplicate);
    }

    private static TransferOperationOutcome RejectedTransfer(Guid transferId, string reasonCode, DateTimeOffset nowUtc) =>
        new(new TransferPrepared
        {
            TransferId = transferId,
            Accepted = false,
            ExpiresUtc = nowUtc.UtcDateTime,
            ReasonCode = reasonCode
        }, TransferState.Rejected, null);

    private void RestoreReservation(TransferEntry transfer)
    {
        if (!instances.TryGetValue(transfer.Request.TargetInstanceId, out var instance))
            return;
        var decision = new PlacementDecision
        {
            RequestId = transfer.Request.TransferId,
            Accepted = true,
            InstanceId = instance.Heartbeat.InstanceId,
            SystemId = instance.Heartbeat.SystemId,
            Endpoint = instance.Heartbeat.Endpoint,
            ReasonCode = "transfer_reserved",
            ExpiresUtc = transfer.ExpiresUtc.UtcDateTime
        };
        reservations[transfer.ReservationKey] = new ReservationEntry(
            transfer.Request.SessionId, transfer.Request.TargetSystemId, transfer.Request.GroupId,
            transfer.Request.TargetInstanceId, decision, transfer.ExpiresUtc);
    }

    private static bool IsFresh(DateTimeOffset lastSeenUtc, DateTimeOffset nowUtc, TimeSpan timeout)
    {
        var age = nowUtc - lastSeenUtc;
        return age >= TimeSpan.Zero && age <= timeout;
    }

    private static bool Matches(ReservationEntry entry, PlacementRequest request) =>
        entry.SessionId == request.SessionId &&
        string.Equals(entry.TargetSystem, request.TargetSystem, StringComparison.Ordinal) &&
        string.Equals(entry.GroupId, request.GroupId, StringComparison.Ordinal);

    private static PlacementDecision WithRequestId(PlacementDecision source, Guid requestId) => new()
    {
        RequestId = requestId,
        Accepted = source.Accepted,
        InstanceId = source.InstanceId,
        SystemId = source.SystemId,
        Endpoint = source.Endpoint,
        ReasonCode = source.ReasonCode,
        ExpiresUtc = source.ExpiresUtc
    };

    private static PlacementDecision Rejected(Guid requestId, string reasonCode, DateTimeOffset nowUtc) => new()
    {
        RequestId = requestId,
        Accepted = false,
        ReasonCode = reasonCode,
        ExpiresUtc = nowUtc.UtcDateTime
    };

    private sealed record AgentEntry(AgentHeartbeat Heartbeat, DateTimeOffset LastHeartbeatUtc);
    private sealed record InstanceEntry(InstanceHeartbeat Heartbeat, DateTimeOffset LastHeartbeatUtc);
    private sealed record ReservationEntry(
        Guid SessionId,
        string TargetSystem,
        string? GroupId,
        string InstanceId,
        PlacementDecision Decision,
        DateTimeOffset ExpiresUtc);
    private sealed record GroupAffinityEntry(string SystemId, string InstanceId, DateTimeOffset ExpiresUtc);
    private sealed record TransferEntry(
        TransferPrepareRequest Request,
        string ReservationKey,
        TransferState State,
        DateTimeOffset ExpiresUtc,
        long LeaseVersion);
}
