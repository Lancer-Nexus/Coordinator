using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlNpcCheckpointTests
{
    [Fact]
    public async Task CheckpointAtomicallyReplacesSurvivorsRetiresTerminalMembersAndRecoversAfterRestart()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlNpcOwnershipStore(connectionString);
            var allocation = await store.AllocateAsync(new()
            { RequestId = Guid.NewGuid(), InstanceId = "source-01", SystemId = "li01", Count = 2 });
            var first = allocation.Npcs[0];
            var second = allocation.Npcs[1];
            var request = Request(Guid.NewGuid(), "source-01", first, second);
            var initial = await store.WriteCheckpointAsync(request);
            Assert.True(initial.Accepted, initial.ReasonCode);
            Assert.All(initial.Revisions, result => Assert.Equal(1, result.Revision));

            var staleRevision = Request(Guid.NewGuid(), "source-01", first, second) with
            {
                ExpectedRevisions = [new() { NpcId = first.NpcId, OwnershipVersion = 1, Revision = 0 },
                    new() { NpcId = second.NpcId, OwnershipVersion = 1, Revision = 1 }]
            };
            Assert.Equal("npc_checkpoint_revision_conflict", (await store.WriteCheckpointAsync(staleRevision)).ReasonCode);
            Assert.NotNull(await store.GetCheckpointAsync(request.RequestId, "source-01"));

            var replay = await new MySqlNpcOwnershipStore(connectionString).WriteCheckpointAsync(request);
            Assert.Equal(initial.RequestId, replay.RequestId);
            Assert.True(replay.Accepted);
            Assert.Equal(initial.Revisions.Select(entry => (entry.NpcId, entry.OwnershipVersion, entry.Revision)),
                replay.Revisions.Select(entry => (entry.NpcId, entry.OwnershipVersion, entry.Revision)));
            Assert.Equal("npc_checkpoint_request_id_conflict", (await store.WriteCheckpointAsync(
                request with { SimulationTick = request.SimulationTick + 1 })).ReasonCode);

            // Reject partial replacement: both leases point at the first durable group.
            var partial = Request(Guid.NewGuid(), "source-01", first, second) with
            {
                Npcs = [Snapshot(second)],
                ExpectedRevisions = [new() { NpcId = second.NpcId,
                    OwnershipVersion = second.OwnershipVersion, Revision = 1 }]
            };
            Assert.Equal("npc_checkpoint_group_incomplete", (await store.WriteCheckpointAsync(partial)).ReasonCode);
            Assert.Equal("npc_checkpoint_required", Assert.Single((await store.RetireAsync(new()
            {
                RequestId = Guid.NewGuid(),
                InstanceId = "source-01",
                Npcs = [new()
                { NpcId = first.NpcId, OwnershipVersion = 1, Reason = NpcRetirementReasonV1.Destroyed }]
            })).Npcs).ReasonCode);

            var next = new NpcCheckpointWriteRequestV1
            {
                RequestId = Guid.NewGuid(),
                InstanceId = "source-01",
                SystemId = "li01",
                SimulationTick = 900,
                Npcs = [Snapshot(second)],
                Retirements = [new()
                    { NpcId = first.NpcId, OwnershipVersion = 1, Reason = NpcRetirementReasonV1.Docked }],
                ExpectedRevisions = [new() { NpcId = first.NpcId, OwnershipVersion = 1, Revision = 1 },
                    new() { NpcId = second.NpcId, OwnershipVersion = 1, Revision = 1 }]
            };
            var committed = await store.WriteCheckpointAsync(next);
            Assert.True(committed.Accepted, committed.ReasonCode);
            Assert.Contains(committed.Revisions, entry => entry.NpcId == first.NpcId &&
                entry.OwnershipVersion == 2 && entry.Revision == 2);
            Assert.Contains(committed.Revisions, entry => entry.NpcId == second.NpcId &&
                entry.OwnershipVersion == 1 && entry.Revision == 2);

            var ids = await store.GetRecoverableCheckpointsAsync("source-01", null, 20);
            Assert.Contains(next.RequestId, ids.CheckpointIds);
            var recovery = await new MySqlNpcOwnershipStore(connectionString).GetCheckpointAsync(next.RequestId, "source-01");
            Assert.NotNull(recovery);
            Assert.Equal(1, Assert.Single(recovery.Snapshot.Npcs).NpcId == second.NpcId ? 1 : 0);
            Assert.Equal(1, recovery.Snapshot.ExpectedRevisions[1].Revision);
            Assert.Equal(2, recovery.Result.Revisions.Single(entry => entry.NpcId == second.NpcId).Revision);
            Assert.Null(await store.GetCheckpointAsync(next.RequestId, "other-instance"));

            var stale = next with { RequestId = Guid.NewGuid(), ExpectedRevisions = next.ExpectedRevisions.Select(e => e with { Revision = 1 }).ToArray() };
            Assert.Equal("npc_ownership_conflict", (await store.WriteCheckpointAsync(stale)).ReasonCode);
        });
    }

    [Fact]
    public async Task TransferMustCarryCompleteCheckpointGroupAndSuccessfulTransferInvalidatesOldCheckpoint()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var store = new MySqlNpcOwnershipStore(connectionString);
            var allocation = await store.AllocateAsync(new()
            { RequestId = Guid.NewGuid(), InstanceId = "source-01", SystemId = "li01", Count = 2 });
            var first = allocation.Npcs[0]; var second = allocation.Npcs[1];
            var checkpoint = Request(Guid.NewGuid(), "source-01", first, second);
            Assert.True((await store.WriteCheckpointAsync(checkpoint)).Accepted);
            var transferStore = new MySqlNpcTransferStore(connectionString);
            var transfer = Prepare(Guid.NewGuid(), [first.NpcId], "source-01", "target-01");
            Assert.Equal("npc_checkpoint_group_incomplete",
                (await transferStore.PrepareAsync(transfer, DateTime.UtcNow)).ReasonCode);
            transfer = Prepare(Guid.NewGuid(), [first.NpcId, second.NpcId], "source-01", "target-01");
            Assert.True((await transferStore.PrepareAsync(transfer, DateTime.UtcNow)).Accepted);
            Assert.True((await transferStore.AdvanceAsync(new()
            {
                TransferId = transfer.TransferId,
                State = NpcTransferState.SourceFrozen,
                Snapshot = new NpcTransferSnapshot
                {
                    TransferId = transfer.TransferId,
                    NpcIds = [first.NpcId, second.NpcId],
                    TargetSystemId = "li02",
                    Npcs = [Snapshot(first), Snapshot(second)]
                }
            }, DateTime.UtcNow)).Accepted);
            foreach (var phase in new[] { NpcTransferState.TargetAccepted, NpcTransferState.Committed })
                Assert.True((await transferStore.AdvanceAsync(new() { TransferId = transfer.TransferId, State = phase }, DateTime.UtcNow)).Accepted);
            Assert.Null(await store.GetCheckpointAsync(checkpoint.RequestId, "source-01"));
        });
    }

    [Fact]
    public async Task MissionCheckpointsFailClosedUntilCharacterAuthorityIsImplemented()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            var allocation = await new MySqlNpcOwnershipStore(connectionString).AllocateAsync(new()
            { RequestId = Guid.NewGuid(), InstanceId = "source-01", SystemId = "li01", Count = 1 });
            var npc = allocation.Npcs[0];
            var request = Request(Guid.NewGuid(), "source-01", npc) with
            {
                Mission = new()
                {
                    RuntimeId = Guid.NewGuid(),
                    CharacterId = 55,
                    CharacterLeaseVersion = 3,
                    RuntimeState = MessagePackSerializer.Serialize(new NpcMissionRuntimeStateV1
                    { MissionNickname = "Mission_01a", RandomState = 1 })
                }
            };
            var result = await new MySqlNpcOwnershipStore(connectionString).WriteCheckpointAsync(request);
            Assert.Equal("npc_mission_checkpoint_authority_unavailable", result.ReasonCode);
            Assert.Null(await new MySqlNpcOwnershipStore(connectionString).GetCheckpointAsync(request.RequestId, "source-01"));
        });
    }

    private static NpcCheckpointWriteRequestV1 Request(Guid id, string owner, params NpcOwnershipLease[] npcs) => new()
    {
        RequestId = id,
        InstanceId = owner,
        SystemId = "li01",
        SimulationTick = 10,
        Npcs = npcs.Select(Snapshot).ToArray(),
        ExpectedRevisions = npcs.Select(npc => new NpcCheckpointRevisionV1
        { NpcId = npc.NpcId, OwnershipVersion = npc.OwnershipVersion }).ToArray()
    };

    private static NpcRuntimeSnapshot Snapshot(NpcOwnershipLease npc) => new()
    {
        NpcId = npc.NpcId,
        OwnershipVersion = npc.OwnershipVersion,
        SystemId = "li01",
        RuntimeState = MessagePackSerializer.Serialize(new NpcRuntimeStateV1
        {
            Position = new() { X = 1, Y = 2, Z = 3 },
            Orientation = new() { W = 1 },
            LoadoutArchetype = "freighter",
            Ai = new() { StateId = "idle", PreviousStateId = "idle" }
        })
    };

    private static NpcTransferPrepareRequest Prepare(Guid id, Guid[] npcs, string source, string target) => new()
    {
        TransferId = id,
        SourceInstanceId = source,
        TargetInstanceId = target,
        TargetSystemId = "li02",
        NpcIds = npcs,
        ExpiresUtc = DateTime.UtcNow.AddMinutes(1),
        IdempotencyKey = id.ToString("D")
    };

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var database = $"npc_checkpoint_test_{Guid.NewGuid():N}";
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
        finally { command.CommandText = $"DROP DATABASE `{database}`"; await command.ExecuteNonQueryAsync(); }
    }
}
