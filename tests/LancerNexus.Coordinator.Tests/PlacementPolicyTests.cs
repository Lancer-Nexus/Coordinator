using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class PlacementPolicyTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 22, 12, 0, 0, TimeSpan.Zero);
    private readonly PlacementPolicy policy = new();

    [Fact]
    public void Decide_SelectsLeastLoadedCandidateDeterministically()
    {
        var candidates = new[]
        {
            Ready("liberty-b", 50, 100),
            Ready("liberty-a", 10, 100),
            Ready("liberty-c", 20, 100)
        };

        var result = policy.Decide(Request(), candidates, Now);

        Assert.True(result.Accepted);
        Assert.Equal("liberty-a", result.InstanceId);
        Assert.Equal("least_loaded", result.ReasonCode);
    }

    [Fact]
    public void Decide_PrefersGroupAffinityOverLoad()
    {
        var candidates = new[]
        {
            Ready("liberty-a", 10, 100),
            Ready("liberty-b", 90, 100) with { HasGroupAffinity = true }
        };

        var result = policy.Decide(Request(), candidates, Now);

        Assert.Equal("liberty-b", result.InstanceId);
        Assert.Equal("group_affinity", result.ReasonCode);
    }

    [Fact]
    public void Decide_RejectsDrainingStaleUnregisteredAndFullInstances()
    {
        var candidates = new[]
        {
            Ready("draining", 0, 10) with { IsDraining = true },
            Ready("stale", 0, 10) with { LastHeartbeatUtc = Now.AddSeconds(-16) },
            Ready("unregistered", 0, 10) with { IsRegistered = false },
            Ready("full", 10, 10)
        };

        var result = policy.Decide(Request(), candidates, Now);

        Assert.False(result.Accepted);
        Assert.Equal("no_ready_capacity", result.ReasonCode);
    }

    [Fact]
    public void Decide_RejectsInvalidRequest()
    {
        var result = policy.Decide(Request(idempotencyKey: ""), [Ready("a", 0, 10)], Now);

        Assert.False(result.Accepted);
        Assert.Equal("invalid_request", result.ReasonCode);
    }

    private static PlacementRequest Request(string idempotencyKey = "assignment-1") => new()
    {
        RequestId = Guid.NewGuid(),
        SessionId = Guid.NewGuid(),
        TargetSystem = "li01",
        IdempotencyKey = idempotencyKey
    };

    private static InstanceCandidate Ready(string id, int current, int max) => new(
        id, "li01", true, true, false, current, max, Now, $"quic://{id}:7443");
}
