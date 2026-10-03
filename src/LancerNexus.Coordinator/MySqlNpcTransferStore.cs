using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Durable transfer journal and atomic NPC ownership fencing.</summary>
public sealed class MySqlNpcTransferStore(string? connectionString, INpcMissionAuthorityClient? missionAuthority = null)
{
    private const int MaximumDurableSnapshotBytes = 15 * 1024 * 1024;
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;
    public bool IsEnabled => connectionString is not null;

    public async Task<NpcTransferPrepared> PrepareAsync(NpcTransferPrepareRequest request, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        NpcTransferContractValidator.Validate(request);
        if (!IsEnabled)
            return Rejected(request.TransferId, "npc_ownership_unavailable");
        if (request.ExpiresUtc <= nowUtc || request.ExpiresUtc > nowUtc.AddMinutes(2))
            return Rejected(request.TransferId, "invalid_expiry");
        // MySQL DATETIME(6) stores microseconds. Compare retries at the same
        // precision or a valid retry can conflict after a database roundtrip.
        var requestedExpiry = new DateTime(request.ExpiresUtc.Ticks - request.ExpiresUtc.Ticks % 10,
            DateTimeKind.Utc);

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var existing = await ReadTransferAsync(connection, transaction, request.TransferId, request.IdempotencyKey, cancellationToken);
        if (existing is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            return Matches(existing, request, requestedExpiry)
                ? Prepared(request.TransferId, existing.State != NpcTransferState.Aborted, "duplicate_transfer", existing.ExpiresUtc)
                : Rejected(request.TransferId, "npc_transfer_id_conflict");
        }

        var ids = MessagePackSerializer.Serialize(request.NpcIds);
        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = """
                INSERT INTO npc_transfer_journal
                    (transfer_id, idempotency_key, source_instance_id, target_instance_id, target_system_id,
                     npc_ids, formation_id, mission_runtime_id, expires_at_utc, state, created_at_utc, updated_at_utc)
                VALUES (@transfer, @key, @source, @target, @system, @ids, @formation, @mission, @expires,
                        @state, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """;
            AddTransferIdentity(insert, request);
            insert.Parameters.AddWithValue("@ids", ids);
            insert.Parameters.AddWithValue("@expires", request.ExpiresUtc);
            insert.Parameters.AddWithValue("@state", (byte)NpcTransferState.Reserved);
            try { await insert.ExecuteNonQueryAsync(cancellationToken); }
            catch (MySqlException ex) when (ex.Number == 1062)
            {
                await transaction.RollbackAsync(cancellationToken);
                await using var retryTransaction = await connection.BeginTransactionAsync(cancellationToken);
                var raced = await ReadTransferAsync(connection, retryTransaction, request.TransferId,
                    request.IdempotencyKey, cancellationToken);
                await retryTransaction.CommitAsync(cancellationToken);
                return raced is not null && Matches(raced, request, requestedExpiry)
                    ? Prepared(request.TransferId, raced.State != NpcTransferState.Aborted,
                        "duplicate_transfer", raced.ExpiresUtc)
                    : Rejected(request.TransferId, "npc_transfer_id_conflict");
            }
        }

        await using (var claim = connection.CreateCommand())
        {
            claim.Transaction = transaction;
            claim.CommandText = $"UPDATE npc_ownership_leases SET active_transfer_id=@transfer, updated_at_utc=UTC_TIMESTAMP(6) WHERE instance_id=@source AND active_transfer_id IS NULL AND retired_at_utc IS NULL AND npc_id IN ({IdParameters(request.NpcIds.Length)})";
            claim.Parameters.AddWithValue("@transfer", request.TransferId.ToString("D"));
            claim.Parameters.AddWithValue("@source", request.SourceInstanceId);
            AddIds(claim, request.NpcIds);
            if (await claim.ExecuteNonQueryAsync(cancellationToken) != request.NpcIds.Length)
            {
                await transaction.RollbackAsync(cancellationToken);
                return Rejected(request.TransferId, "npc_ownership_conflict");
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return Prepared(request.TransferId, true, "reserved", request.ExpiresUtc);
    }

    public async Task<NpcTransferPhaseResult> AdvanceAsync(NpcTransferPhaseRequest request, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
            return new(false, "npc_ownership_unavailable", request.TransferId, request.State);
        if (request.TransferId == Guid.Empty)
            return new(false, "invalid_transfer_id", request.TransferId, request.State);

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var row = await ReadTransferAsync(connection, transaction, request.TransferId, null, cancellationToken);
        if (row is null)
            return new(false, "npc_transfer_not_found", request.TransferId, request.State);
        if (row.MissionRuntimeId is not null && request.State is NpcTransferState.Committed or NpcTransferState.Aborted)
        {
            if (row.MissionRuntimeId != row.TransferId)
                return new(false, "mission_transfer_id_mismatch", request.TransferId, row.State);
            if (request.State == NpcTransferState.Aborted &&
                row.State is NpcTransferState.Committed or NpcTransferState.SourceReleased)
                return new(false, "committed_transfer_cannot_abort", request.TransferId, row.State);
            if (missionAuthority is null)
                return new(false, "mission_authority_not_configured", request.TransferId, row.State);
            // Keep the NPC journal row locked until the permanent Gateway decision
            // is confirmed. If delivery is uncertain, no NPC state/lease changes;
            // retrying the same decision safely completes this recoverable saga.
            var proof = await missionAuthority.AuthorizeAsync(new NpcMissionAuthorityRequestV1
            {
                TransferId = row.TransferId, SourceInstanceId = row.SourceInstanceId,
                TargetInstanceId = row.TargetInstanceId, TargetSystemId = row.TargetSystemId,
                Decision = request.State
            }, cancellationToken);
            if (!proof.Authorized)
                return new(false, proof.ReasonCode, request.TransferId, row.State);
        }
        if (row.State == request.State)
        {
            if (request.State == NpcTransferState.SourceFrozen &&
                !SnapshotMatches(row.Snapshot, request.Snapshot))
                return new(false, "duplicate_snapshot_conflict", request.TransferId, row.State);
            await transaction.CommitAsync(cancellationToken);
            return new(true, "duplicate_phase", request.TransferId, row.State);
        }

        if (request.State == NpcTransferState.Aborted)
        {
            if (row.State is NpcTransferState.Committed or NpcTransferState.SourceReleased)
                return new(false, "committed_transfer_cannot_abort", request.TransferId, row.State);
            await ClearTransferLeasesAsync(connection, transaction, request.TransferId, cancellationToken);
            await SetStateAsync(connection, transaction, request.TransferId, NpcTransferState.Aborted, null, null, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new(true, "aborted", request.TransferId, NpcTransferState.Aborted);
        }

        byte[]? snapshotBytes = null;
        byte[]? snapshotHash = null;
        switch (request.State)
        {
            case NpcTransferState.SourceFrozen when row.State == NpcTransferState.Reserved:
                if (row.ExpiresUtc <= nowUtc)
                    return new(false, "npc_transfer_expired", request.TransferId, row.State);
                if (request.Snapshot is null)
                    return new(false, "snapshot_required", request.TransferId, row.State);
                NpcTransferContractValidator.Validate(request.Snapshot);
                if (request.Snapshot.TransferId != request.TransferId ||
                    !request.Snapshot.NpcIds.Order().SequenceEqual(row.NpcIds.Order()))
                    return new(false, "snapshot_identity_mismatch", request.TransferId, row.State);
                if (!string.Equals(request.Snapshot.TargetSystemId, row.TargetSystemId, StringComparison.OrdinalIgnoreCase) ||
                    request.Snapshot.MissionRuntimeId != row.MissionRuntimeId)
                    return new(false, "snapshot_reservation_mismatch", request.TransferId, row.State);
                if (!await SnapshotVersionsMatchAsync(connection, transaction, request.Snapshot, cancellationToken))
                    return new(false, "npc_ownership_version_conflict", request.TransferId, row.State);
                snapshotBytes = MessagePackSerializer.Serialize(request.Snapshot);
                if (snapshotBytes.Length > MaximumDurableSnapshotBytes)
                    return new(false, "npc_snapshot_exceeds_durable_limit", request.TransferId, row.State);
                snapshotHash = System.Security.Cryptography.SHA256.HashData(snapshotBytes);
                break;
            case NpcTransferState.TargetAccepted when row.State == NpcTransferState.SourceFrozen && row.Snapshot is not null:
                break;
            case NpcTransferState.Committed when row.State == NpcTransferState.TargetAccepted:
                if (!await CommitOwnershipAsync(connection, transaction, row, cancellationToken))
                    return new(false, "npc_ownership_commit_conflict", request.TransferId, row.State);
                break;
            case NpcTransferState.SourceReleased when row.State == NpcTransferState.Committed:
                break;
            default:
                return new(false, "invalid_npc_transfer_transition", request.TransferId, row.State);
        }

        await SetStateAsync(connection, transaction, request.TransferId, request.State, snapshotBytes, snapshotHash, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(true, "advanced", request.TransferId, request.State);
    }

    public async Task<NpcTransferRecoveryRecord?> GetRecoveryRecordAsync(Guid transferId, string requesterInstanceId,
        bool includeSnapshot = false,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || transferId == Guid.Empty || string.IsNullOrWhiteSpace(requesterInstanceId) ||
            requesterInstanceId.Length > 96)
            return null;
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var snapshotColumn = includeSnapshot ? "snapshot" : "CASE WHEN state IN (6, 7, 22) THEN snapshot ELSE NULL END";
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT transfer_id, source_instance_id, target_instance_id, target_system_id, npc_ids,
                   expires_at_utc, state, snapshot_sha256, {snapshotColumn}
            FROM npc_transfer_journal WHERE transfer_id=@transfer
            """;
        command.Parameters.AddWithValue("@transfer", transferId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            await reader.CloseAsync();
            await transaction.CommitAsync(cancellationToken);
            return null;
        }
        var record = new NpcTransferRecoveryRecord(
            ReadGuid(reader, 0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            MessagePackSerializer.Deserialize<Guid[]>(reader.GetFieldValue<byte[]>(4)),
            DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc), (NpcTransferState)reader.GetByte(6),
            reader.IsDBNull(7) ? null : Convert.ToHexString(reader.GetFieldValue<byte[]>(7)),
            reader.IsDBNull(8) ? null : MessagePackSerializer.Deserialize<NpcTransferSnapshot>(reader.GetFieldValue<byte[]>(8)));
        await reader.CloseAsync();
        if (record.SourceInstanceId != requesterInstanceId && record.TargetInstanceId != requesterInstanceId)
            return null;
        var targetRestore = record.TargetInstanceId == requesterInstanceId &&
            record.State is NpcTransferState.Committed or NpcTransferState.SourceReleased;
        var sourceRestore = record.SourceInstanceId == requesterInstanceId &&
            record.State == NpcTransferState.Aborted && record.Snapshot is not null;
        if (targetRestore || sourceRestore)
        {
            var versions = RecoveryVersions(record.Snapshot, record.NpcIds, targetRestore);
            if (!await OwnsNpcLeasesAsync(connection, transaction, requesterInstanceId, versions, cancellationToken))
                return null;
            if (record.Snapshot?.MissionRuntimeId is Guid missionId)
            {
                if (missionId != record.TransferId || missionAuthority is null)
                    return null;
                var proof = await missionAuthority.AuthorizeAsync(new NpcMissionAuthorityRequestV1
                {
                    TransferId = record.TransferId, SourceInstanceId = record.SourceInstanceId,
                    TargetInstanceId = record.TargetInstanceId, TargetSystemId = record.TargetSystemId,
                    Decision = targetRestore ? NpcTransferState.Committed : NpcTransferState.Aborted
                }, cancellationToken);
                if (!proof.Authorized)
                    return null;
            }
        }
        await transaction.CommitAsync(cancellationToken);
        return includeSnapshot ? record : record with { Snapshot = null };
    }

    // Keep only identity/version metadata after decoding; page scans must not retain runtime payloads.
    private static Dictionary<Guid, long> RecoveryVersions(NpcTransferSnapshot? snapshot, Guid[] ids,
        bool targetRestore)
    {
        if (snapshot is null || ids.Length == 0 || snapshot.Npcs.Length != ids.Length ||
            !snapshot.NpcIds.Order().SequenceEqual(ids.Order()))
            return [];
        var idSet = ids.ToHashSet();
        if (idSet.Count != ids.Length)
            return [];
        var versions = new Dictionary<Guid, long>(ids.Length);
        foreach (var npc in snapshot.Npcs)
        {
            if (!idSet.Contains(npc.NpcId) || npc.OwnershipVersion <= 0 ||
                targetRestore && npc.OwnershipVersion == long.MaxValue ||
                !versions.TryAdd(npc.NpcId, npc.OwnershipVersion + (targetRestore ? 1 : 0)))
                return [];
        }
        return versions;
    }

    private static async Task<bool> OwnsNpcLeasesAsync(MySqlConnection connection, MySqlTransaction transaction,
        string instanceId, Dictionary<Guid, long> expectedVersions, CancellationToken cancellationToken)
    {
        if (expectedVersions.Count == 0)
            return false;
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT npc_id, ownership_version FROM npc_ownership_leases WHERE instance_id=@instance AND active_transfer_id IS NULL AND retired_at_utc IS NULL AND npc_id IN ({IdParameters(expectedVersions.Count)})";
        command.Parameters.AddWithValue("@instance", instanceId);
        AddIds(command, expectedVersions.Keys.ToArray());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var ownedCount = 0;
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!expectedVersions.TryGetValue(ReadGuid(reader, 0), out var version) || version != reader.GetInt64(1))
                return false;
            ownedCount++;
        }
        return ownedCount == expectedVersions.Count;
    }

    public async Task<NpcTransferRecoveryPageV1> GetRecoverableTransfersAsync(string targetInstanceId,
        Guid? afterTransferId, int limit, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || string.IsNullOrWhiteSpace(targetInstanceId) || targetInstanceId.Length > 96)
            return new NpcTransferRecoveryPageV1();
        if (limit is < 1 or > 128)
            throw new ArgumentOutOfRangeException(nameof(limit));

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT transfer_id, npc_ids, snapshot
            FROM npc_transfer_journal
            WHERE target_instance_id=@target AND state IN (@committed, @released)
              AND (@after IS NULL OR transfer_id > @after)
            ORDER BY transfer_id
            LIMIT @take
            """;
        command.Parameters.AddWithValue("@target", targetInstanceId);
        command.Parameters.AddWithValue("@committed", (byte)NpcTransferState.Committed);
        command.Parameters.AddWithValue("@released", (byte)NpcTransferState.SourceReleased);
        command.Parameters.AddWithValue("@after", afterTransferId?.ToString("D"));
        command.Parameters.AddWithValue("@take", limit + 1);

        var candidates = new List<(Guid TransferId, Dictionary<Guid, long> Versions)>(limit + 1);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var ids = MessagePackSerializer.Deserialize<Guid[]>(reader.GetFieldValue<byte[]>(1));
                var snapshot = reader.IsDBNull(2) ? null :
                    MessagePackSerializer.Deserialize<NpcTransferSnapshot>(reader.GetFieldValue<byte[]>(2));
                candidates.Add((ReadGuid(reader, 0), RecoveryVersions(snapshot, ids, targetRestore: true)));
            }
        }

        var hasMore = candidates.Count > limit;
        if (hasMore)
            candidates.RemoveAt(candidates.Count - 1);

        var npcIds = candidates.SelectMany(candidate => candidate.Versions.Keys).Distinct().ToArray();
        var activeNpcVersions = new Dictionary<Guid, long>();
        if (npcIds.Length > 0)
        {
            await using var leaseCommand = connection.CreateCommand();
            var placeholders = string.Join(",", Enumerable.Range(0, npcIds.Length).Select(index => $"@id{index}"));
            leaseCommand.CommandText = $"SELECT npc_id, ownership_version FROM npc_ownership_leases WHERE instance_id=@target AND active_transfer_id IS NULL AND retired_at_utc IS NULL AND npc_id IN ({placeholders})";
            leaseCommand.Parameters.AddWithValue("@target", targetInstanceId);
            for (var i = 0; i < npcIds.Length; i++)
                leaseCommand.Parameters.AddWithValue($"@id{i}", npcIds[i].ToString("D"));
            await using var leaseReader = await leaseCommand.ExecuteReaderAsync(cancellationToken);
            while (await leaseReader.ReadAsync(cancellationToken))
                activeNpcVersions.Add(ReadGuid(leaseReader, 0), leaseReader.GetInt64(1));
        }

        var recoverableIds = candidates
            .Where(candidate => candidate.Versions.Count > 0 && candidate.Versions.All(expected =>
                activeNpcVersions.TryGetValue(expected.Key, out var current) && current == expected.Value))
            .Select(candidate => candidate.TransferId)
            .ToArray();
        return new NpcTransferRecoveryPageV1
        {
            TransferIds = recoverableIds,
            NextAfterTransferId = hasMore ? candidates[^1].TransferId : null
        };
    }

    public async Task<NpcTransferRecoveryPageV1> GetPendingSourceTransfersAsync(string sourceInstanceId,
        Guid? afterTransferId, int limit, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || string.IsNullOrWhiteSpace(sourceInstanceId) || sourceInstanceId.Length > 96)
            return new NpcTransferRecoveryPageV1();
        if (limit is < 1 or > 128)
            throw new ArgumentOutOfRangeException(nameof(limit));

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT transfer_id
            FROM npc_transfer_journal
            WHERE source_instance_id=@source AND state IN (@sourceFrozen, @targetAccepted, @committed)
              AND (@after IS NULL OR transfer_id > @after)
            ORDER BY transfer_id
            LIMIT @take
            """;
        command.Parameters.AddWithValue("@source", sourceInstanceId);
        command.Parameters.AddWithValue("@sourceFrozen", (byte)NpcTransferState.SourceFrozen);
        command.Parameters.AddWithValue("@targetAccepted", (byte)NpcTransferState.TargetAccepted);
        command.Parameters.AddWithValue("@committed", (byte)NpcTransferState.Committed);
        command.Parameters.AddWithValue("@after", afterTransferId?.ToString("D"));
        command.Parameters.AddWithValue("@take", limit + 1);

        var transferIds = new List<Guid>(limit + 1);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                transferIds.Add(ReadGuid(reader, 0));
        }
        var hasMore = transferIds.Count > limit;
        if (hasMore)
            transferIds.RemoveAt(transferIds.Count - 1);
        return new NpcTransferRecoveryPageV1
        {
            TransferIds = transferIds.ToArray(),
            NextAfterTransferId = hasMore ? transferIds[^1] : null
        };
    }

    private static async Task<bool> CommitOwnershipAsync(MySqlConnection connection, MySqlTransaction transaction,
        TransferRow row, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"UPDATE npc_ownership_leases SET instance_id=@target, system_id=@system, ownership_version=ownership_version+1, active_transfer_id=NULL, updated_at_utc=UTC_TIMESTAMP(6) WHERE active_transfer_id=@transfer AND instance_id=@source AND npc_id IN ({IdParameters(row.NpcIds.Length)})";
        command.Parameters.AddWithValue("@target", row.TargetInstanceId);
        command.Parameters.AddWithValue("@system", row.TargetSystemId);
        command.Parameters.AddWithValue("@transfer", row.TransferId.ToString("D"));
        command.Parameters.AddWithValue("@source", row.SourceInstanceId);
        AddIds(command, row.NpcIds);
        return await command.ExecuteNonQueryAsync(cancellationToken) == row.NpcIds.Length;
    }

    private static async Task<bool> SnapshotVersionsMatchAsync(MySqlConnection connection, MySqlTransaction transaction,
        NpcTransferSnapshot snapshot, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT npc_id, ownership_version FROM npc_ownership_leases WHERE active_transfer_id=@transfer AND npc_id IN ({IdParameters(snapshot.Npcs.Length)}) FOR UPDATE";
        command.Parameters.AddWithValue("@transfer", snapshot.TransferId.ToString("D"));
        AddIds(command, snapshot.Npcs.Select(x => x.NpcId).ToArray());
        var versions = new Dictionary<Guid, long>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            versions.Add(ReadGuid(reader, 0), reader.GetInt64(1));
        return versions.Count == snapshot.Npcs.Length && snapshot.Npcs.All(npc =>
            versions.TryGetValue(npc.NpcId, out var version) && version == npc.OwnershipVersion);
    }

    private static async Task ClearTransferLeasesAsync(MySqlConnection connection, MySqlTransaction transaction,
        Guid transferId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE npc_ownership_leases SET active_transfer_id=NULL, updated_at_utc=UTC_TIMESTAMP(6) WHERE active_transfer_id=@transfer";
        command.Parameters.AddWithValue("@transfer", transferId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task SetStateAsync(MySqlConnection connection, MySqlTransaction transaction,
        Guid transferId, NpcTransferState state, byte[]? snapshot, byte[]? snapshotHash,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE npc_transfer_journal SET state=@state, snapshot=COALESCE(@snapshot, snapshot), snapshot_sha256=COALESCE(@snapshot_hash, snapshot_sha256), updated_at_utc=UTC_TIMESTAMP(6) WHERE transfer_id=@transfer";
        command.Parameters.AddWithValue("@state", (byte)state);
        command.Parameters.AddWithValue("@snapshot", snapshot is null ? DBNull.Value : snapshot);
        command.Parameters.AddWithValue("@snapshot_hash", snapshotHash is null ? DBNull.Value : snapshotHash);
        command.Parameters.AddWithValue("@transfer", transferId.ToString("D"));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<TransferRow?> ReadTransferAsync(MySqlConnection connection, MySqlTransaction transaction,
        Guid transferId, string? idempotencyKey, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = idempotencyKey is null
            ? "SELECT transfer_id, source_instance_id, target_instance_id, target_system_id, npc_ids, expires_at_utc, state, snapshot, formation_id, mission_runtime_id, idempotency_key FROM npc_transfer_journal WHERE transfer_id=@transfer FOR UPDATE"
            : "SELECT transfer_id, source_instance_id, target_instance_id, target_system_id, npc_ids, expires_at_utc, state, snapshot, formation_id, mission_runtime_id, idempotency_key FROM npc_transfer_journal WHERE transfer_id=@transfer OR idempotency_key=@key FOR UPDATE";
        command.Parameters.AddWithValue("@transfer", transferId.ToString("D"));
        if (idempotencyKey is not null) command.Parameters.AddWithValue("@key", idempotencyKey);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new TransferRow(ReadGuid(reader, 0), reader.GetString(1), reader.GetString(2),
            reader.GetString(3), MessagePackSerializer.Deserialize<Guid[]>(reader.GetFieldValue<byte[]>(4)),
            DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc), (NpcTransferState)reader.GetByte(6),
            reader.IsDBNull(7) ? null : reader.GetFieldValue<byte[]>(7),
            reader.IsDBNull(8) ? null : ReadGuid(reader, 8),
            reader.IsDBNull(9) ? null : ReadGuid(reader, 9), reader.GetString(10));
    }

    private static Guid ReadGuid(MySqlDataReader reader, int ordinal) =>
        reader.GetValue(ordinal) is Guid value ? value : Guid.Parse(reader.GetString(ordinal));

    private static bool Matches(TransferRow row, NpcTransferPrepareRequest request, DateTime requestedExpiry) =>
        row.TransferId == request.TransferId && row.SourceInstanceId == request.SourceInstanceId &&
        row.TargetInstanceId == request.TargetInstanceId &&
        row.TargetSystemId.Equals(request.TargetSystemId, StringComparison.OrdinalIgnoreCase) &&
        row.NpcIds.Order().SequenceEqual(request.NpcIds.Order()) && row.FormationId == request.FormationId &&
        row.MissionRuntimeId == request.MissionRuntimeId && row.IdempotencyKey == request.IdempotencyKey &&
        row.ExpiresUtc == requestedExpiry;

    private static bool SnapshotMatches(byte[]? storedSnapshot, NpcTransferSnapshot? requestedSnapshot)
    {
        if (storedSnapshot is null || requestedSnapshot is null)
            return storedSnapshot is null && requestedSnapshot is null;
        var requestedBytes = MessagePackSerializer.Serialize(requestedSnapshot);
        return System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
            System.Security.Cryptography.SHA256.HashData(storedSnapshot),
            System.Security.Cryptography.SHA256.HashData(requestedBytes));
    }

    private static void AddTransferIdentity(MySqlCommand command, NpcTransferPrepareRequest request)
    {
        command.Parameters.AddWithValue("@transfer", request.TransferId.ToString("D"));
        command.Parameters.AddWithValue("@key", request.IdempotencyKey);
        command.Parameters.AddWithValue("@source", request.SourceInstanceId);
        command.Parameters.AddWithValue("@target", request.TargetInstanceId);
        command.Parameters.AddWithValue("@system", request.TargetSystemId.ToLowerInvariant());
        command.Parameters.AddWithValue("@formation", request.FormationId?.ToString("D") is { } f ? f : DBNull.Value);
        command.Parameters.AddWithValue("@mission", request.MissionRuntimeId?.ToString("D") is { } m ? m : DBNull.Value);
    }

    private static string IdParameters(int count) => string.Join(',', Enumerable.Range(0, count).Select(i => $"@id{i}"));
    private static void AddIds(MySqlCommand command, Guid[] ids)
    { for (var i = 0; i < ids.Length; i++) command.Parameters.AddWithValue($"@id{i}", ids[i].ToString("D")); }
    private static NpcTransferPrepared Prepared(Guid id, bool accepted, string reason, DateTime expiry) =>
        new() { TransferId = id, Accepted = accepted, ReasonCode = reason, ExpiresUtc = expiry };
    private static NpcTransferPrepared Rejected(Guid id, string reason) => Prepared(id, false, reason, DateTime.MinValue);

    private sealed record TransferRow(Guid TransferId, string SourceInstanceId, string TargetInstanceId,
        string TargetSystemId, Guid[] NpcIds, DateTime ExpiresUtc, NpcTransferState State, byte[]? Snapshot,
        Guid? FormationId, Guid? MissionRuntimeId, string IdempotencyKey);
}

public sealed record NpcTransferPhaseResult(bool Accepted, string ReasonCode, Guid TransferId, NpcTransferState State);
public sealed record NpcTransferRecoveryRecord(Guid TransferId, string SourceInstanceId, string TargetInstanceId,
    string TargetSystemId, Guid[] NpcIds, DateTime ExpiresUtc, NpcTransferState State, string? SnapshotSha256,
    NpcTransferSnapshot? Snapshot);
