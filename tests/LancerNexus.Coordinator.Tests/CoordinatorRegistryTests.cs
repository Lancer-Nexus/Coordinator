using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class CoordinatorRegistryTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 22, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Heartbeats_RegisterOnlyFreshAgentsAndRejectReplayedSequences()
    {
        var registry = CreateRegistry();

        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 2), Now).Accepted);
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 2), Now.AddSeconds(1)).Duplicate);
        Assert.Equal("stale_sequence", registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).ReasonCode);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance(sequence: 2), Now).Accepted);
        Assert.Equal("stale_sequence", registry.ApplyInstanceHeartbeat(Instance(sequence: 1), Now).ReasonCode);
        Assert.Equal("agent_identity_changed", registry.ApplyAgentHeartbeat(Agent(nodeId: "other", sequence: 3), Now).ReasonCode);

        var snapshot = registry.Snapshot(Now.AddSeconds(16));
        Assert.False(Assert.Single(snapshot.Agents).IsAlive);
        Assert.False(Assert.Single(snapshot.Instances).IsAlive);
    }

    [Fact]
    public void Place_IsIdempotentAndHonorsCapacityReservations()
    {
        var registry = CreateRegistry();
        Register(registry, maxPlayers: 1);
        var request = Request("first");

        var accepted = registry.Place(request, Now);
        var retry = registry.Place(new PlacementRequest
        {
            RequestId = Guid.NewGuid(),
            SessionId = request.SessionId,
            TargetSystem = request.TargetSystem,
            IdempotencyKey = request.IdempotencyKey
        }, Now.AddSeconds(1));
        var rejected = registry.Place(Request("second"), Now.AddSeconds(1));

        Assert.True(accepted.Decision.Accepted);
        Assert.True(retry.Duplicate);
        Assert.Equal(accepted.Decision.InstanceId, retry.Decision.InstanceId);
        Assert.False(rejected.Decision.Accepted);
        Assert.Equal("no_ready_capacity", rejected.Decision.ReasonCode);
    }

    [Fact]
    public void Place_ConflictingIdempotencyKeyIsRejected()
    {
        var registry = CreateRegistry();
        Register(registry);
        Assert.True(registry.Place(Request("same-key"), Now).Decision.Accepted);

        var conflict = registry.Place(Request("same-key", targetSystem: "li02"), Now);

        Assert.False(conflict.Decision.Accepted);
        Assert.Equal("idempotency_conflict", conflict.Decision.ReasonCode);
    }

    [Fact]
    public void Place_ConcurrentAssignmentsCannotOverbookLastSlot()
    {
        var registry = CreateRegistry();
        Register(registry, maxPlayers: 1);
        var outcomes = new System.Collections.Concurrent.ConcurrentBag<PlacementOutcome>();

        Parallel.For(0, 16, index => outcomes.Add(registry.Place(Request($"parallel-{index}"), Now)));

        Assert.Single(outcomes, outcome => outcome.Decision.Accepted);
        Assert.Equal(15, outcomes.Count(outcome => !outcome.Decision.Accepted));
    }

    [Fact]
    public void Place_GroupAffinityDoesNotSilentlyMoveToAnotherInstance()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("one", 1, 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("two", 1, 10), Now).Accepted);
        var first = Request("member-1", groupId: "group");
        Assert.Equal("one", registry.Place(first, Now).Decision.InstanceId);

        var nextMember = Request("member-2", groupId: "group");
        var result = registry.Place(nextMember, Now);

        Assert.False(result.Decision.Accepted);
        Assert.Equal("group_instance_unavailable", result.Decision.ReasonCode);
    }

    private static CoordinatorRegistry CreateRegistry() => new(new PlacementPolicy(), CoordinatorRegistryOptions.Default);

    private static void Register(CoordinatorRegistry registry, int maxPlayers = 20)
    {
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance(sequence: 1, maxPlayers: maxPlayers), Now).Accepted);
    }

    private static AgentHeartbeat Agent(string nodeId = "node-1", ulong sequence = 1) => new()
    {
        AgentId = "agent-1",
        NodeId = nodeId,
        BuildVersion = "test",
        Sequence = sequence
    };

    private static InstanceHeartbeat Instance(string id = "instance-1", ulong sequence = 1, int maxPlayers = 20) => new()
    {
        AgentId = "agent-1",
        InstanceId = id,
        SystemId = "li01",
        Sequence = sequence,
        IsReady = true,
        CurrentPlayers = 0,
        MaxPlayers = maxPlayers,
        Endpoint = $"quic://{id}:7443"
    };

    private static PlacementRequest Request(string key, string targetSystem = "li01", string? groupId = null) => new()
    {
        RequestId = Guid.NewGuid(),
        SessionId = Guid.NewGuid(),
        TargetSystem = targetSystem,
        GroupId = groupId,
        IdempotencyKey = key
    };
}
