using System.Security.Cryptography;
using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlNpcTransferStoreTests
{
    [MySqlFact]
    public async Task JournalSurvivesStoreRestartAndReplaysEveryCommittedPhaseIdempotently()
    {
        var configuredConnection = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL");
        if (string.IsNullOrWhiteSpace(configuredConnection))
            throw new InvalidOperationException("MySqlFact must skip this test when no test server is configured.");

        var databaseName = $"npc_transfer_test_{Guid.NewGuid():N}";
        var admin = new MySqlConnectionStringBuilder(configuredConnection) { Database = "information_schema" };
        await using (var adminConnection = new MySqlConnection(admin.ConnectionString))
        {
            await adminConnection.OpenAsync();
            await using var create = adminConnection.CreateCommand();
            create.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
            await create.ExecuteNonQueryAsync();
        }

        var database = new MySqlConnectionStringBuilder(configuredConnection) { Database = databaseName };
        try
        {
            await ApplySchemaAsync(database.ConnectionString);
            var transferId = Guid.NewGuid();
            var npcId = Guid.NewGuid();
            var allocationId = Guid.NewGuid();
            var now = DateTime.UtcNow;
            await SeedLeaseAsync(database.ConnectionString, npcId, allocationId, now);

            var ownershipStore = new MySqlNpcOwnershipStore(database.ConnectionString);
            var allocationRetry = await ownershipStore.AllocateAsync(new NpcIdBatchAllocationRequest
            {
                RequestId = allocationId,
                InstanceId = "source-01",
                SystemId = "li01",
                Count = 1
            });
            Assert.True(allocationRetry.Accepted);
            Assert.True(allocationRetry.Duplicate);
            Assert.Equal(npcId, Assert.Single(allocationRetry.Npcs).NpcId);

            var prepare = new NpcTransferPrepareRequest
            {
                TransferId = transferId,
                SourceInstanceId = "source-01",
                TargetInstanceId = "target-01",
                TargetSystemId = "li02",
                NpcIds = [npcId],
                ExpiresUtc = now.AddMinutes(1),
                IdempotencyKey = $"{transferId:N}:mission-npcs"
            };
            var store = new MySqlNpcTransferStore(database.ConnectionString);
            Assert.Null(await store.GetRecoveryRecordAsync(Guid.NewGuid(), "source-01"));
            var prepared = await store.PrepareAsync(prepare, now);
            Assert.True(prepared.Accepted);
            Assert.Equal(NpcTransferState.Reserved,
                (await store.GetRecoveryRecordAsync(transferId, "source-01"))!.State);
            Assert.Null(await store.GetRecoveryRecordAsync(transferId, "other-01", includeSnapshot: true));

            await AssertReservationMismatchAsync(store, transferId,
                CreateSnapshot(transferId, npcId, targetSystem: "li03"), now);
            await AssertReservationMismatchAsync(store, transferId,
                CreateSnapshot(transferId, npcId, missionId: Guid.NewGuid()), now);

            var snapshot = CreateSnapshot(transferId, npcId);
            var sourceFrozenRequest = new NpcTransferPhaseRequest
            {
                TransferId = transferId,
                State = NpcTransferState.SourceFrozen,
                Snapshot = snapshot
            };
            Assert.True((await store.AdvanceAsync(sourceFrozenRequest, now)).Accepted);

            // A fresh store object has no in-memory state; the journal and bytes must be sufficient.
            store = new MySqlNpcTransferStore(database.ConnectionString);
            var recovered = await store.GetRecoveryRecordAsync(transferId, "source-01", includeSnapshot: true);
            Assert.Equal(NpcTransferState.SourceFrozen, recovered!.State);
            Assert.Equal(snapshot.NpcIds, recovered.Snapshot!.NpcIds);
            Assert.Equal(Convert.ToHexString(SHA256.HashData(MessagePackSerializer.Serialize(snapshot))),
                recovered.SnapshotSha256);
            Assert.True((await store.AdvanceAsync(sourceFrozenRequest, now)).Accepted);

            var conflictingSnapshot = CreateSnapshot(transferId, npcId, positionX: 99);
            Assert.False((await store.AdvanceAsync(new NpcTransferPhaseRequest
            {
                TransferId = transferId,
                State = NpcTransferState.SourceFrozen,
                Snapshot = conflictingSnapshot
            }, now)).Accepted);

            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
            {
                TransferId = transferId,
                State = NpcTransferState.TargetAccepted
            }, now)).Accepted);
            store = new MySqlNpcTransferStore(database.ConnectionString);
            Assert.Equal(NpcTransferState.TargetAccepted,
                (await store.GetRecoveryRecordAsync(transferId, "target-01"))!.State);

            var commit = new NpcTransferPhaseRequest { TransferId = transferId, State = NpcTransferState.Committed };
            Assert.True((await store.AdvanceAsync(commit, now)).Accepted);
            store = new MySqlNpcTransferStore(database.ConnectionString);
            var committed = await store.GetRecoveryRecordAsync(transferId, "target-01", includeSnapshot: true);
            Assert.Equal(NpcTransferState.Committed, committed!.State);
            Assert.NotNull(committed.Snapshot);
            Assert.False((await store.AdvanceAsync(new NpcTransferPhaseRequest
            {
                TransferId = transferId,
                State = NpcTransferState.Aborted
            }, now)).Accepted);

            await using var connection = new MySqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await using var leaseCommand = connection.CreateCommand();
            leaseCommand.CommandText = "SELECT instance_id, system_id, ownership_version, active_transfer_id FROM npc_ownership_leases WHERE npc_id=@npc";
            leaseCommand.Parameters.AddWithValue("@npc", npcId.ToString("D"));
            await using (var reader = await leaseCommand.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("target-01", reader.GetString(0));
                Assert.Equal("li02", reader.GetString(1));
                Assert.Equal(2, reader.GetInt64(2));
                Assert.True(reader.IsDBNull(3));
            }

            var sourceReleased = new NpcTransferPhaseRequest
            {
                TransferId = transferId,
                State = NpcTransferState.SourceReleased
            };
            Assert.True((await store.AdvanceAsync(sourceReleased, now)).Accepted);
            store = new MySqlNpcTransferStore(database.ConnectionString);
            Assert.Equal(NpcTransferState.SourceReleased,
                (await store.GetRecoveryRecordAsync(transferId, "target-01"))!.State);
            Assert.True((await store.AdvanceAsync(sourceReleased, now)).Accepted);

            // A round trip returns to the same instance with a different fence.
            // Neither old journal discovery nor direct lookup may revive version 2.
            await CompleteTransferAsync(store, npcId, "target-01", "source-01", "li01", 2, now);
            var latest = await CompleteTransferAsync(store, npcId, "source-01", "target-01", "li02", 3, now);
            Assert.Null(await store.GetRecoveryRecordAsync(transferId, "target-01"));
            Assert.Null(await store.GetRecoveryRecordAsync(transferId, "target-01", includeSnapshot: true));
            Assert.Equal(latest, Assert.Single((await store.GetRecoverableTransfersAsync("target-01", null, 128)).TransferIds));
            Assert.NotNull(await store.GetRecoveryRecordAsync(latest, "target-01"));
            Assert.Null((await store.GetRecoveryRecordAsync(latest, "target-01"))!.Snapshot);
            Assert.Equal(3, (await store.GetRecoveryRecordAsync(latest, "target-01", includeSnapshot: true))!
                .Snapshot!.Npcs[0].OwnershipVersion);

            // Aborted rollback snapshots need their original source fence too.
            var aborted = Guid.NewGuid();
            Assert.True((await store.PrepareAsync(new NpcTransferPrepareRequest
            {
                TransferId = aborted, SourceInstanceId = "target-01", TargetInstanceId = "source-01",
                TargetSystemId = "li01", NpcIds = [npcId], ExpiresUtc = now.AddMinutes(1),
                IdempotencyKey = aborted.ToString("N")
            }, now)).Accepted);
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
            {
                TransferId = aborted, State = NpcTransferState.SourceFrozen,
                Snapshot = CreateSnapshot(aborted, npcId, ownershipVersion: 4, sourceSystem: "li02", targetSystem: "li01")
            }, now)).Accepted);
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
                { TransferId = aborted, State = NpcTransferState.Aborted }, now)).Accepted);
            Assert.NotNull(await store.GetRecoveryRecordAsync(aborted, "target-01", includeSnapshot: true));
            await CompleteTransferAsync(store, npcId, "target-01", "source-01", "li01", 4, now);
            var final = await CompleteTransferAsync(store, npcId, "source-01", "target-01", "li02", 5, now);
            Assert.Null(await store.GetRecoveryRecordAsync(aborted, "target-01", includeSnapshot: true));
            Assert.Null(await store.GetRecoveryRecordAsync(latest, "target-01"));
            Assert.Equal(final, Assert.Single((await store.GetRecoverableTransfersAsync("target-01", null, 128)).TransferIds));

            // Mission binding must survive restart, including omission or replacement
            // of the association by an otherwise valid runtime payload.
            var missionNpc = Guid.NewGuid();
            var missionTransfer = Guid.NewGuid();
            await SeedLeaseAsync(database.ConnectionString, missionNpc, Guid.NewGuid(), now);
            Assert.True((await store.PrepareAsync(new NpcTransferPrepareRequest
            {
                TransferId = missionTransfer, SourceInstanceId = "source-01", TargetInstanceId = "target-01",
                TargetSystemId = "li02", NpcIds = [missionNpc], MissionRuntimeId = missionTransfer,
                ExpiresUtc = now.AddMinutes(1), IdempotencyKey = missionTransfer.ToString("N")
            }, now)).Accepted);
            store = new MySqlNpcTransferStore(database.ConnectionString);
            await AssertReservationMismatchAsync(store, missionTransfer,
                CreateSnapshot(missionTransfer, missionNpc), now);
            await AssertReservationMismatchAsync(store, missionTransfer,
                CreateSnapshot(missionTransfer, missionNpc, missionId: Guid.NewGuid()), now);
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
            {
                TransferId = missionTransfer, State = NpcTransferState.SourceFrozen,
                Snapshot = CreateSnapshot(missionTransfer, missionNpc, targetSystem: "LI02", missionId: missionTransfer)
            }, now)).Accepted);
            var missionRecovery = await new MySqlNpcTransferStore(database.ConnectionString)
                .GetRecoveryRecordAsync(missionTransfer, "source-01", includeSnapshot: true);
            Assert.Equal(missionTransfer, missionRecovery!.Snapshot!.MissionRuntimeId);
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
                { TransferId = missionTransfer, State = NpcTransferState.TargetAccepted }, now)).Accepted);
            foreach (var phase in new[] { NpcTransferState.Committed, NpcTransferState.Aborted })
            {
                var denied = await store.AdvanceAsync(new NpcTransferPhaseRequest
                    { TransferId = missionTransfer, State = phase }, now);
                Assert.False(denied.Accepted);
                Assert.Equal("mission_authority_not_configured", denied.ReasonCode);
                Assert.Equal(NpcTransferState.TargetAccepted, denied.State);
            }
            var authority = new TestMissionAuthority();
            store = new MySqlNpcTransferStore(database.ConnectionString, authority);
            foreach (var phase in new[] { NpcTransferState.Committed, NpcTransferState.Aborted })
            {
                Assert.False((await store.AdvanceAsync(new NpcTransferPhaseRequest
                    { TransferId = missionTransfer, State = phase }, now)).Accepted);
                Assert.Equal(NpcTransferState.TargetAccepted,
                    (await store.GetRecoveryRecordAsync(missionTransfer, "source-01", includeSnapshot: true))!.State);
            }
            authority.Authorized = true;
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
                { TransferId = missionTransfer, State = NpcTransferState.Committed }, now)).Accepted);
            Assert.NotNull(await new MySqlNpcTransferStore(database.ConnectionString, authority)
                .GetRecoveryRecordAsync(missionTransfer, "target-01", includeSnapshot: true));
            Assert.Null(await new MySqlNpcTransferStore(database.ConnectionString)
                .GetRecoveryRecordAsync(missionTransfer, "target-01", includeSnapshot: true));
            authority.Authorized = false;
            Assert.Null(await store.GetRecoveryRecordAsync(missionTransfer, "target-01", includeSnapshot: true));
            authority.Authorized = true;
            var calls = authority.Calls;
            Assert.False((await store.AdvanceAsync(new NpcTransferPhaseRequest
                { TransferId = missionTransfer, State = NpcTransferState.Aborted }, now)).Accepted);
            Assert.Equal(calls, authority.Calls);

            // An uncertain authority reply leaves the frozen source reservation
            // and snapshot intact. Retry after durable abort confirmation rolls back.
            var rollbackNpc = Guid.NewGuid();
            var rollback = Guid.NewGuid();
            await SeedLeaseAsync(database.ConnectionString, rollbackNpc, Guid.NewGuid(), now);
            Assert.True((await store.PrepareAsync(new NpcTransferPrepareRequest
            {
                TransferId = rollback, SourceInstanceId = "source-01", TargetInstanceId = "target-01",
                TargetSystemId = "li02", NpcIds = [rollbackNpc], MissionRuntimeId = rollback,
                ExpiresUtc = now.AddMinutes(1), IdempotencyKey = rollback.ToString("N")
            }, now)).Accepted);
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
            {
                TransferId = rollback, State = NpcTransferState.SourceFrozen,
                Snapshot = CreateSnapshot(rollback, rollbackNpc, missionId: rollback)
            }, now)).Accepted);
            authority.Authorized = false;
            var abortRequest = new NpcTransferPhaseRequest { TransferId = rollback, State = NpcTransferState.Aborted };
            Assert.False((await store.AdvanceAsync(abortRequest, now)).Accepted);
            Assert.Equal(NpcTransferState.SourceFrozen,
                (await store.GetRecoveryRecordAsync(rollback, "source-01", includeSnapshot: true))!.State);
            authority.Authorized = true;
            Assert.True((await store.AdvanceAsync(abortRequest, now)).Accepted);
            Assert.True((await new MySqlNpcTransferStore(database.ConnectionString, authority)
                .AdvanceAsync(abortRequest, now)).Accepted);
            Assert.Equal(NpcTransferState.Aborted,
                (await store.GetRecoveryRecordAsync(rollback, "source-01", includeSnapshot: true))!.State);
            Assert.Null(await new MySqlNpcTransferStore(database.ConnectionString)
                .GetRecoveryRecordAsync(rollback, "source-01", includeSnapshot: true));
            authority.Authorized = false;
            Assert.Null(await store.GetRecoveryRecordAsync(rollback, "source-01", includeSnapshot: true));
        }
        finally
        {
            await using var adminConnection = new MySqlConnection(admin.ConnectionString);
            await adminConnection.OpenAsync();
            await using var drop = adminConnection.CreateCommand();
            drop.CommandText = $"DROP DATABASE IF EXISTS `{databaseName}`";
            await drop.ExecuteNonQueryAsync();
        }
    }

    private static async Task AssertReservationMismatchAsync(MySqlNpcTransferStore store, Guid transferId,
        NpcTransferSnapshot snapshot, DateTime now)
    {
        var rejected = await store.AdvanceAsync(new NpcTransferPhaseRequest
        {
            TransferId = transferId, State = NpcTransferState.SourceFrozen, Snapshot = snapshot
        }, now);
        Assert.False(rejected.Accepted);
        Assert.Equal("snapshot_reservation_mismatch", rejected.ReasonCode);
        var unchanged = await store.GetRecoveryRecordAsync(transferId, "source-01", includeSnapshot: true);
        Assert.Equal(NpcTransferState.Reserved, unchanged!.State);
        Assert.Null(unchanged.Snapshot);
        Assert.Null(unchanged.SnapshotSha256);
    }

    private static async Task<Guid> CompleteTransferAsync(MySqlNpcTransferStore store, Guid npcId,
        string source, string target, string targetSystem, long version, DateTime now)
    {
        var transfer = Guid.NewGuid();
        Assert.True((await store.PrepareAsync(new NpcTransferPrepareRequest
        {
            TransferId = transfer, SourceInstanceId = source, TargetInstanceId = target,
            TargetSystemId = targetSystem, NpcIds = [npcId], ExpiresUtc = now.AddMinutes(1),
            IdempotencyKey = transfer.ToString("N")
        }, now)).Accepted);
        Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
        {
            TransferId = transfer, State = NpcTransferState.SourceFrozen,
            Snapshot = CreateSnapshot(transfer, npcId, ownershipVersion: version,
                sourceSystem: targetSystem == "li01" ? "li02" : "li01", targetSystem: targetSystem)
        }, now)).Accepted);
        foreach (var phase in new[] { NpcTransferState.TargetAccepted, NpcTransferState.Committed, NpcTransferState.SourceReleased })
            Assert.True((await store.AdvanceAsync(new NpcTransferPhaseRequest
                { TransferId = transfer, State = phase }, now)).Accepted);
        return transfer;
    }

    internal static async Task ApplySchemaAsync(string connectionString)
    {
        var directory = Path.Combine(AppContext.BaseDirectory, "db", "migrations");
        var schema = string.Join("\n", await Task.WhenAll(Directory.GetFiles(directory, "*.sql").Order().Select(path => File.ReadAllTextAsync(path))));
        var statements = schema.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (var statement in statements)
        {
            if (string.IsNullOrWhiteSpace(statement)) continue;
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task SeedLeaseAsync(string connectionString, Guid npcId, Guid allocationId, DateTime now)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        await using (var batch = connection.CreateCommand())
        {
            batch.CommandText = "INSERT INTO npc_id_allocation_batches (request_id, instance_id, system_id, requested_count, created_at_utc) VALUES (@request, 'source-01', 'li01', 1, @now)";
            batch.Parameters.AddWithValue("@request", allocationId.ToString("D"));
            batch.Parameters.AddWithValue("@now", now);
            await batch.ExecuteNonQueryAsync();
        }
        await using var lease = connection.CreateCommand();
        lease.CommandText = "INSERT INTO npc_ownership_leases (npc_id, instance_id, system_id, ownership_version, allocation_request_id, allocation_ordinal, created_at_utc, updated_at_utc) VALUES (@npc, 'source-01', 'li01', 1, @request, 0, @now, @now)";
        lease.Parameters.AddWithValue("@npc", npcId.ToString("D"));
        lease.Parameters.AddWithValue("@request", allocationId.ToString("D"));
        lease.Parameters.AddWithValue("@now", now);
        await lease.ExecuteNonQueryAsync();
    }

    internal static NpcTransferSnapshot CreateSnapshot(Guid transferId, Guid npcId, float positionX = 0,
        long ownershipVersion = 1, string sourceSystem = "li01", string targetSystem = "li02", Guid? missionId = null)
    {
        var runtime = new NpcRuntimeStateV1
        {
            Position = new NpcVector3 { X = positionX },
            Orientation = new NpcQuaternion { W = 1 },
            LoadoutArchetype = "npc_fighter",
            Health = 100,
            Autopilot = new NpcAutopilotState { Behavior = "None" },
            Ai = new NpcAiState { StateId = "none", PreviousStateId = "none" }
        };
        return new NpcTransferSnapshot
        {
            TransferId = transferId,
            NpcIds = [npcId],
            TargetSystemId = targetSystem,
            MissionRuntimeId = missionId,
            MissionRuntimeState = missionId is null ? [] : MessagePackSerializer.Serialize(new NpcMissionRuntimeStateV1
            {
                MissionNickname = "mission_01", RandomState = 1
            }),
            Npcs =
            [
                new NpcRuntimeSnapshot
                {
                    NpcId = npcId,
                    OwnershipVersion = ownershipVersion,
                    SystemId = sourceSystem,
                    RuntimeState = MessagePackSerializer.Serialize(runtime)
                }
            ]
        };
    }

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }

    private sealed class TestMissionAuthority : INpcMissionAuthorityClient
    {
        public bool Authorized { get; set; }
        public int Calls { get; private set; }
        public Task<NpcMissionAuthorityCheck> AuthorizeAsync(NpcMissionAuthorityRequestV1 request,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            Assert.True(request.IsValid());
            Assert.Equal("source-01", request.SourceInstanceId);
            Assert.Equal("target-01", request.TargetInstanceId);
            Assert.Equal("li02", request.TargetSystemId);
            return Task.FromResult(new NpcMissionAuthorityCheck(Authorized,
                Authorized ? "confirmed" : "mission_authority_unavailable"));
        }
    }
}
