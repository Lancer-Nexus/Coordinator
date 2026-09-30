using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class NpcTransferTargetResolverTests
{
    [Fact]
    public void Resolve_SelectsLeastLoadedReadyOwnerAndExcludesSourceAndLegacyPeers()
    {
        var instances = new[]
        {
            Instance("source", "rh01", load: 0, capabilities: [ClusterCapabilities.NpcTransferV1]),
            Instance("legacy", "rh01", load: 0, capabilities: []),
            Instance("busy", "rh01", load: 18, capabilities: [ClusterCapabilities.NpcTransferV1]),
            Instance("ready", "li01", load: 0, capabilities: [ClusterCapabilities.NpcTransferV1], systems: ["li01", "rh01"]),
            Instance("draining", "rh01", load: 0, draining: true, capabilities: [ClusterCapabilities.NpcTransferV1])
        };
        var request = new NpcTransferTargetResolveRequestV1
        {
            SourceInstanceId = "source",
            TargetSystemId = "RH01"
        };

        var target = NpcTransferTargetResolver.Resolve(instances, request);

        Assert.NotNull(target);
        Assert.Equal("ready", target.InstanceId);
    }

    [Fact]
    public void Resolve_ReturnsNullWhenNoReadyPeerAdvertisesNpcTransfer()
    {
        var target = NpcTransferTargetResolver.Resolve(
            [Instance("legacy", "rh01", load: 0, capabilities: [])],
            new NpcTransferTargetResolveRequestV1 { SourceInstanceId = "source", TargetSystemId = "rh01" });

        Assert.Null(target);
    }

    private static InstanceRegistryView Instance(string id, string system, int load,
        bool draining = false, string[]? capabilities = null, string[]? systems = null) =>
        new("agent", id, system, true, draining, load, 0, 20, $"quic://{id}:7443", 1,
            DateTimeOffset.UtcNow, true, true, systems ?? [system], capabilities ?? []);
}
