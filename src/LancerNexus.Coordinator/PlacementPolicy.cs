using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record InstanceCandidate(
    string InstanceId,
    string SystemId,
    bool IsRegistered,
    bool IsReady,
    bool IsDraining,
    int CurrentPlayers,
    int MaxPlayers,
    DateTimeOffset LastHeartbeatUtc,
    string? Endpoint,
    bool HasGroupAffinity = false,
    int ReservedPlayers = 0);

public sealed record PlacementPolicyOptions(TimeSpan MaximumHeartbeatAge, TimeSpan ReservationLifetime)
{
    public static PlacementPolicyOptions Default { get; } = new(TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
}

public sealed class PlacementPolicy(PlacementPolicyOptions? options = null)
{
    private readonly PlacementPolicyOptions policyOptions = options ?? PlacementPolicyOptions.Default;

    public PlacementDecision Decide(
        PlacementRequest request,
        IEnumerable<InstanceCandidate> candidates,
        DateTimeOffset nowUtc)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(candidates);

        if (request.RequestId == Guid.Empty || request.SessionId == Guid.Empty ||
            string.IsNullOrWhiteSpace(request.TargetSystem) || string.IsNullOrWhiteSpace(request.IdempotencyKey))
        {
            return Rejected(request.RequestId, "invalid_request", nowUtc);
        }

        var viable = candidates
            .Where(candidate => IsViable(candidate, request.TargetSystem, nowUtc))
            .OrderByDescending(candidate => candidate.HasGroupAffinity)
            .ThenBy(candidate => (double)(candidate.CurrentPlayers + candidate.ReservedPlayers) / candidate.MaxPlayers)
            .ThenBy(candidate => candidate.CurrentPlayers + candidate.ReservedPlayers)
            .ThenBy(candidate => candidate.InstanceId, StringComparer.Ordinal)
            .ToArray();

        if (viable.Length == 0)
            return Rejected(request.RequestId, "no_ready_capacity", nowUtc);

        var selected = viable[0];
        return new PlacementDecision
        {
            RequestId = request.RequestId,
            Accepted = true,
            InstanceId = selected.InstanceId,
            SystemId = selected.SystemId,
            Endpoint = selected.Endpoint,
            ReasonCode = selected.HasGroupAffinity ? "group_affinity" : "least_loaded",
            ExpiresUtc = nowUtc.Add(policyOptions.ReservationLifetime).UtcDateTime
        };
    }

    private bool IsViable(InstanceCandidate candidate, string systemId, DateTimeOffset nowUtc)
    {
        var age = nowUtc - candidate.LastHeartbeatUtc;
        return candidate.IsRegistered && candidate.IsReady && !candidate.IsDraining &&
               string.Equals(candidate.SystemId, systemId, StringComparison.Ordinal) &&
               candidate.CurrentPlayers >= 0 && candidate.ReservedPlayers >= 0 && candidate.MaxPlayers > 0 &&
               candidate.CurrentPlayers + candidate.ReservedPlayers < candidate.MaxPlayers &&
               age >= TimeSpan.Zero && age <= policyOptions.MaximumHeartbeatAge &&
               !string.IsNullOrWhiteSpace(candidate.InstanceId) &&
               !string.IsNullOrWhiteSpace(candidate.Endpoint);
    }

    private static PlacementDecision Rejected(Guid requestId, string reasonCode, DateTimeOffset nowUtc) => new()
    {
        RequestId = requestId,
        Accepted = false,
        ReasonCode = reasonCode,
        ExpiresUtc = nowUtc.UtcDateTime
    };
}
