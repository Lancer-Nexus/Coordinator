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

public sealed class CoordinatorRegistry
{
    private readonly object sync = new();
    private readonly PlacementPolicy placementPolicy;
    private readonly CoordinatorRegistryOptions options;
    private readonly Dictionary<string, AgentEntry> agents = new(StringComparer.Ordinal);
    private readonly Dictionary<string, InstanceEntry> instances = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ReservationEntry> reservations = new(StringComparer.Ordinal);
    private readonly Dictionary<string, GroupAffinityEntry> groupAffinities = new(StringComparer.Ordinal);

    public CoordinatorRegistry(PlacementPolicy placementPolicy, CoordinatorRegistryOptions options)
    {
        this.placementPolicy = placementPolicy;
        this.options = options;
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
            return new(true, "accepted");
        }
    }

    public RegistryOperationResult ApplyInstanceHeartbeat(InstanceHeartbeat heartbeat, DateTimeOffset receivedAtUtc)
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
            return new(true, "accepted");
        }
    }

    public PlacementOutcome Place(PlacementRequest request, DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(request);
        lock (sync)
        {
            CleanupExpired(nowUtc);
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

            if (!string.IsNullOrWhiteSpace(request.GroupId))
            {
                groupAffinities[request.GroupId] = new GroupAffinityEntry(
                    request.TargetSystem,
                    decision.InstanceId,
                    nowUtc.Add(options.GroupAffinityLifetime));
            }

            return new(decision);
        }
    }

    public (IReadOnlyList<AgentRegistryView> Agents, IReadOnlyList<InstanceRegistryView> Instances) Snapshot(DateTimeOffset nowUtc)
    {
        lock (sync)
        {
            CleanupExpired(nowUtc);
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

    private void CleanupExpired(DateTimeOffset nowUtc)
    {
        foreach (var key in reservations.Where(pair => pair.Value.ExpiresUtc <= nowUtc).Select(pair => pair.Key).ToArray())
            reservations.Remove(key);
        foreach (var key in groupAffinities.Where(pair => pair.Value.ExpiresUtc <= nowUtc).Select(pair => pair.Key).ToArray())
            groupAffinities.Remove(key);
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
}
