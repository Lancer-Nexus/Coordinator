using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class CoordinatorFlHookEventReceiverTests
{
    [Fact]
    public async Task AuthenticatedLiveInstanceIsDurablyAcknowledged()
    {
        var now = DateTimeOffset.UtcNow;
        var store = new RecordingEventStore(FlHookEventIngestStatusV1.Stored);
        var receiver = new CoordinatorFlHookEventReceiver(CreateRegisteredRegistry(now), store, TimeProvider.System);
        var value = Event();

        var result = await receiver.IngestAsync(Guid.NewGuid(), 17, "node-1", value);

        Assert.Equal(FlHookEventIngestStatusV1.Stored, result.Status);
        Assert.True(result.IsDurablyAccepted);
        Assert.Equal(value, store.LastValue);
    }

    [Fact]
    public async Task CertificateForAnotherAgentCannotPublishForInstance()
    {
        var now = DateTimeOffset.UtcNow;
        var store = new RecordingEventStore(FlHookEventIngestStatusV1.Stored);
        var receiver = new CoordinatorFlHookEventReceiver(CreateRegisteredRegistry(now), store, TimeProvider.System);

        var result = await receiver.IngestAsync(Guid.NewGuid(), 17, "other-node", Event());

        Assert.Equal(FlHookEventIngestStatusV1.Rejected, result.Status);
        Assert.Null(store.LastValue);
    }

    [Fact]
    public async Task FrameSequenceMustMatchEventSequence()
    {
        var now = DateTimeOffset.UtcNow;
        var receiver = new CoordinatorFlHookEventReceiver(CreateRegisteredRegistry(now),
            new RecordingEventStore(FlHookEventIngestStatusV1.Stored), TimeProvider.System);

        await Assert.ThrowsAsync<ProtocolViolationException>(() =>
            receiver.IngestAsync(Guid.NewGuid(), 18, "node-1", Event()));
    }

    private static CoordinatorRegistry CreateRegisteredRegistry(DateTimeOffset now)
    {
        var options = CoordinatorRegistryOptions.Default;
        var registry = new CoordinatorRegistry(new PlacementPolicy(), new CoordinatorRegistryOptions(
            TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), options.GroupAffinityLifetime));
        Assert.True(registry.ApplyAgentHeartbeat(new AgentHeartbeat
        {
            AgentId = "agent-1", NodeId = "node-1", BuildVersion = "test", Sequence = 1
        }, now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(new InstanceHeartbeat
        {
            AgentId = "agent-1", InstanceId = "li-01", SystemId = "li01", Endpoint = "quic://li-01:7443",
            Sequence = 1, IsReady = true, MaxPlayers = 200
        }, now, "node-1").Accepted);
        return registry;
    }

    private static FlHookEventV1 Event() => new()
    {
        InstanceId = "li-01", ProducerEpoch = Guid.NewGuid(), Sequence = 17,
        OccurredUtc = DateTimeOffset.UtcNow, Kind = FlHookEventKind.PlayerConnected, PlayerId = "player-01"
    };

    private sealed class RecordingEventStore(FlHookEventIngestStatusV1 status) : IFlHookEventStore
    {
        public bool IsEnabled => true;
        public FlHookEventV1? LastValue { get; private set; }
        public Task<FlHookEventIngestStatusV1> IngestAsync(FlHookEventV1 value,
            CancellationToken cancellationToken = default)
        {
            LastValue = value;
            return Task.FromResult(status);
        }
    }
}
