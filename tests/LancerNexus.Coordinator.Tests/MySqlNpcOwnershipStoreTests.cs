using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlNpcOwnershipStoreTests
{
    [Fact]
    public async Task AllocationFailsClosedWhenNoDatabaseIsConfigured()
    {
        var store = new MySqlNpcOwnershipStore(null);

        var result = await store.AllocateAsync(new NpcIdBatchAllocationRequest
        {
            RequestId = Guid.NewGuid(),
            InstanceId = "li-01",
            SystemId = "li01",
            Count = 1
        });

        Assert.False(store.IsEnabled);
        Assert.False(result.Accepted);
        Assert.Equal("npc_ownership_unavailable", result.ReasonCode);
    }

    [Fact]
    public async Task AllocationRejectsInvalidRequestBeforeOpeningDatabase()
    {
        var store = new MySqlNpcOwnershipStore("Server=127.0.0.1;Database=unused;User ID=unused");

        var result = await store.AllocateAsync(new NpcIdBatchAllocationRequest
        {
            RequestId = Guid.Empty,
            InstanceId = "li-01",
            SystemId = "li01",
            Count = 1
        });

        Assert.Equal("invalid_npc_allocation_request", result.ReasonCode);
    }
}
