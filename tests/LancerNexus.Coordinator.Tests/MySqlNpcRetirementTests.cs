using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlNpcRetirementTests
{
    [Fact]
    public async Task RetirementFailsClosedWithoutDatabase()
    {
        var request = Request("source-01", Guid.NewGuid(), 1);
        Assert.Equal("npc_ownership_unavailable", (await new MySqlNpcOwnershipStore(null).RetireAsync(request)).ReasonCode);
        Assert.Equal("invalid_npc_retirement_request", (await new MySqlNpcOwnershipStore("unused").RetireAsync(
            request with { Npcs = [] })).ReasonCode);
    }

    [MySqlFact]
    public async Task RetirementSurvivesRestartFencesTransfersAndPreventsJournalReplay()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlNpcOwnershipStore(connectionString);
            var allocation = new NpcIdBatchAllocationRequest
            {
                RequestId = Guid.NewGuid(),
                InstanceId = "source-01",
                SystemId = "li01",
                Count = 2
            };
            var ids = (await store.AllocateAsync(allocation)).Npcs.Select(npc => npc.NpcId).ToArray();
            var transferStore = new MySqlNpcTransferStore(connectionString);
            var transfer = Guid.NewGuid();
            var prepare = new NpcTransferPrepareRequest
            {
                TransferId = transfer,
                SourceInstanceId = "source-01",
                TargetInstanceId = "target-01",
                TargetSystemId = "li02",
                NpcIds = [ids[0]],
                ExpiresUtc = DateTime.UtcNow.AddMinutes(1),
                IdempotencyKey = transfer.ToString("D")
            };
            Assert.True((await transferStore.PrepareAsync(prepare, DateTime.UtcNow)).Accepted);
            var blocked = await store.RetireAsync(Request("source-01", ids[0], 1));
            Assert.Equal("npc_transfer_in_progress", Assert.Single(blocked.Npcs).ReasonCode);
            foreach (var state in new[] { NpcTransferState.SourceFrozen, NpcTransferState.TargetAccepted,
                         NpcTransferState.Committed, NpcTransferState.SourceReleased })
                Assert.True((await transferStore.AdvanceAsync(new NpcTransferPhaseRequest
                {
                    TransferId = transfer,
                    State = state,
                    Snapshot = state == NpcTransferState.SourceFrozen
                        ? MySqlNpcTransferStoreTests.CreateSnapshot(transfer, ids[0]) : null
                }, DateTime.UtcNow)).Accepted);
            Assert.Single((await transferStore.GetRecoverableTransfersAsync("target-01", null, 20)).TransferIds);

            var request = Request("target-01", ids[0], 2);
            var outcomes = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ =>
                new MySqlNpcOwnershipStore(connectionString).RetireAsync(request)));
            Assert.All(outcomes, result =>
            {
                Assert.True(Assert.Single(result.Npcs).Accepted);
                Assert.Equal(3, result.Npcs[0].OwnershipVersion);
                Assert.Equal("retired", result.Npcs[0].ReasonCode);
            });
            store = new MySqlNpcOwnershipStore(connectionString);
            Assert.Equal("npc_retirement_request_id_conflict", (await store.RetireAsync(
                request with { InstanceId = "other" })).ReasonCode);
            Assert.Equal("already_retired", Assert.Single((await store.RetireAsync(
                request with { RequestId = Guid.NewGuid() })).Npcs).ReasonCode);
            var replay = await store.AllocateAsync(allocation);
            Assert.True(replay.Npcs[0].IsRetired);
            Assert.Equal(3, replay.Npcs[0].OwnershipVersion);
            Assert.False(replay.Npcs[1].IsRetired);
            Assert.Empty((await transferStore.GetRecoverableTransfersAsync("target-01", null, 20)).TransferIds);
            Assert.Null(await transferStore.GetRecoveryRecordAsync(transfer, "target-01", true));
            Assert.False((await transferStore.PrepareAsync(new NpcTransferPrepareRequest
            {
                TransferId = Guid.NewGuid(),
                SourceInstanceId = "target-01",
                TargetInstanceId = "source-01",
                TargetSystemId = "li01",
                NpcIds = [ids[0]],
                ExpiresUtc = DateTime.UtcNow.AddMinutes(1),
                IdempotencyKey = Guid.NewGuid().ToString("D")
            }, DateTime.UtcNow)).Accepted);
        });
    }

    [MySqlFact]
    public async Task BatchRetiresValidEntriesEvenWhenAnotherFenceIsStale()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlNpcOwnershipStore(connectionString);
            var allocation = await store.AllocateAsync(new()
            {
                RequestId = Guid.NewGuid(),
                InstanceId = "source-01",
                SystemId = "li01",
                Count = 256
            });
            var request = new NpcRetirementRequestV1
            {
                RequestId = Guid.NewGuid(),
                InstanceId = "source-01",
                Npcs = allocation.Npcs.Select((npc, i) => new NpcRetirementEntryV1
                {
                    NpcId = npc.NpcId,
                    OwnershipVersion = i == 0 ? 2 : 1,
                    Reason = NpcRetirementReasonV1.Docked
                }).ToArray()
            };
            var response = await store.RetireAsync(request);
            Assert.False(response.Npcs[0].Accepted);
            Assert.Equal(255, response.Npcs.Count(npc => npc.Accepted));
            Assert.All(response.Npcs.Skip(1), npc => Assert.Equal(2, npc.OwnershipVersion));
            Assert.Equal("npc_ownership_conflict", Assert.Single((await store.RetireAsync(
                Request("other", allocation.Npcs[0].NpcId, 1))).Npcs).ReasonCode);
        });
    }

    private static NpcRetirementRequestV1 Request(string instance, Guid npc, long version) => new()
    {
        RequestId = Guid.NewGuid(),
        InstanceId = instance,
        Npcs = [new() { NpcId = npc, OwnershipVersion = version, Reason = NpcRetirementReasonV1.Destroyed }]
    };

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var database = $"npc_retirement_test_{Guid.NewGuid():N}";
        var admin = new MySqlConnectionStringBuilder(configured) { Database = "information_schema" };
        await using var connection = new MySqlConnection(admin.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
        await command.ExecuteNonQueryAsync();
        try
        {
            var isolated = new MySqlConnectionStringBuilder(configured) { Database = database };
            await MySqlNpcTransferStoreTests.ApplySchemaAsync(isolated.ConnectionString);
            await action(isolated.ConnectionString);
        }
        finally
        {
            command.CommandText = $"DROP DATABASE `{database}`";
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }
}
