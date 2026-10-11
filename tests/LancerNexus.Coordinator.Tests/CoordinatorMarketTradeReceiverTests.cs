using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class CoordinatorMarketTradeReceiverTests
{
    [Fact]
    public async Task RejectsTradeFromAgentThatDoesNotOwnTheLiveInstance()
    {
        var trade = Trade();
        var receiver = new CoordinatorMarketTradeReceiver(CreateRegisteredRegistry(),
            new MySqlMarketTradeStore(null), TimeProvider.System);
        var result = await receiver.ApplyAsync((ulong)trade.Sequence, "other-node", trade);
        Assert.Equal(MarketTradeIngestStatusV1.Rejected, result.Status);
        Assert.True(result.Matches(trade));
    }

    [Fact]
    public async Task FrameSequenceMustMatchTradeSequence()
    {
        var trade = Trade();
        var receiver = new CoordinatorMarketTradeReceiver(CreateRegisteredRegistry(),
            new MySqlMarketTradeStore(null), TimeProvider.System);
        await Assert.ThrowsAsync<ProtocolViolationException>(() =>
            receiver.ApplyAsync((ulong)(trade.Sequence + 1), "node-1", trade));
    }

    private static CoordinatorRegistry CreateRegisteredRegistry()
    {
        var now = DateTimeOffset.UtcNow;
        var options = CoordinatorRegistryOptions.Default;
        var registry = new CoordinatorRegistry(new PlacementPolicy(), new CoordinatorRegistryOptions(
            TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), options.GroupAffinityLifetime));
        Assert.True(registry.ApplyAgentHeartbeat(new AgentHeartbeat
        {
            AgentId = "agent-1",
            NodeId = "node-1",
            BuildVersion = "test",
            Sequence = 1
        }, now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(new InstanceHeartbeat
        {
            AgentId = "agent-1",
            InstanceId = "li-01",
            SystemId = "li01",
            Endpoint = "quic://li-01:7443",
            Sequence = 1,
            IsReady = true,
            MaxPlayers = 200
        }, now, "node-1").Accepted);
        return registry;
    }

    private static MarketTradeEventV1 Trade() => new()
    {
        EventId = Guid.NewGuid(),
        InstanceId = "li-01",
        ProducerEpoch = Guid.NewGuid(),
        Sequence = 10,
        OccurredUtc = DateTimeOffset.UtcNow,
        MarketId = "li01_01_base",
        SystemId = "li01",
        GoodId = "commodity_food",
        Side = MarketTradeSideV1.Buy,
        Quantity = 1
    };
}
