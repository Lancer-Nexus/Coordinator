using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

public sealed partial class MySqlNpcOwnershipStore
{
    public async Task<NpcCheckpointWriteResponseV1> WriteCheckpointAsync(NpcCheckpointWriteRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        try { NpcCheckpointContractValidator.Validate(request); }
        catch (ProtocolViolationException) { return CheckpointRejected(request.RequestId, "invalid_npc_checkpoint_request"); }
        if (!IsEnabled) return CheckpointRejected(request.RequestId, "npc_ownership_unavailable");

        var payload = MessagePackSerializer.Serialize(request);
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = "INSERT INTO npc_checkpoint_writes (request_id, instance_id, request_payload, created_at_utc) VALUES (@request, @instance, @payload, UTC_TIMESTAMP(6))";
            insert.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            insert.Parameters.AddWithValue("@instance", request.InstanceId);
            insert.Parameters.AddWithValue("@payload", payload);
            try { await insert.ExecuteNonQueryAsync(cancellationToken); }
            catch (MySqlException exception) when (exception.Number == 1062)
            {
                await transaction.RollbackAsync(cancellationToken);
                var stored = await ReadCheckpointAsync(connection, null, request.RequestId, cancellationToken);
                if (stored is null) throw new InvalidDataException("NPC checkpoint has no committed result.");
                return stored.Value.Payload.AsSpan().SequenceEqual(payload) ? stored.Value.Recovery.Result :
                    CheckpointRejected(request.RequestId, "npc_checkpoint_request_id_conflict");
            }
        }

        // Character checkpoint authority requires its own durable Gateway arbitration.
        // Until that exists, no mission state may enter this ambient-only storage path.
        string? rejection = request.Mission is not null || request.Npcs.Any(npc =>
            MessagePackSerializer.Deserialize<NpcRuntimeStateV1>(npc.RuntimeState).MissionState.Length > 0)
            ? "npc_mission_checkpoint_authority_unavailable" : null;
        var rows = await LockCheckpointMembersAsync(connection, transaction,
            request.ExpectedRevisions.Select(entry => entry.NpcId).ToArray(), cancellationToken);
        if (rejection is null)
        {
            foreach (var expected in request.ExpectedRevisions)
            {
                if (!rows.TryGetValue(expected.NpcId, out var row) || row.InstanceId != request.InstanceId ||
                    row.Version != expected.OwnershipVersion || row.Retired ||
                    !string.Equals(row.SystemId, request.SystemId, StringComparison.OrdinalIgnoreCase))
                    rejection = "npc_ownership_conflict";
                else if (row.HasTransfer) rejection = "npc_transfer_in_progress";
                else if (row.Revision != expected.Revision) rejection = "npc_checkpoint_revision_conflict";
                if (rejection is not null) break;
            }
        }
        if (rejection is null && !await IncludesCheckpointGroupsAsync(connection, transaction, rows,
                request.ExpectedRevisions.Select(entry => entry.NpcId).ToHashSet(), cancellationToken))
            rejection = "npc_checkpoint_group_incomplete";

        NpcCheckpointWriteResponseV1 response;
        if (rejection is not null) response = CheckpointRejected(request.RequestId, rejection);
        else
        {
            var retired = request.Retirements.ToDictionary(entry => entry.NpcId);
            // One bulk update after sorted row locking; no per-NPC network round trips.
            await using var update = connection.CreateCommand();
            update.Transaction = transaction;
            var expected = request.ExpectedRevisions.OrderBy(entry => entry.NpcId.ToString("D"), StringComparer.Ordinal).ToArray();
            var ids = expected.Select((_, i) => $"@npc{i}").ToArray();
            var reasonCases = expected.Select((entry, i) => $"WHEN @npc{i} THEN @reason{i}");
            update.CommandText = $"UPDATE npc_ownership_leases SET checkpoint_revision=checkpoint_revision+1, ownership_version=ownership_version+CASE WHEN npc_id IN ({CheckpointRetirementIds(expected, retired)}) THEN 1 ELSE 0 END, checkpoint_id=CASE WHEN npc_id IN ({CheckpointRetirementIds(expected, retired)}) THEN NULL ELSE @request END, retired_at_utc=CASE WHEN npc_id IN ({CheckpointRetirementIds(expected, retired)}) THEN UTC_TIMESTAMP(6) ELSE NULL END, retirement_reason=CASE npc_id {string.Join(' ', reasonCases)} END, updated_at_utc=UTC_TIMESTAMP(6) WHERE npc_id IN ({string.Join(',', ids)})";
            update.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            for (var i = 0; i < expected.Length; i++)
            {
                update.Parameters.AddWithValue(ids[i], expected[i].NpcId.ToString("D"));
                update.Parameters.AddWithValue($"@reason{i}", retired.TryGetValue(expected[i].NpcId, out var terminal) ? (byte)terminal.Reason : DBNull.Value);
            }
            if (await update.ExecuteNonQueryAsync(cancellationToken) != expected.Length)
                throw new InvalidDataException("NPC checkpoint lost a locked member.");
            response = new()
            {
                RequestId = request.RequestId, Accepted = true, ReasonCode = "checkpointed",
                Revisions = request.ExpectedRevisions.Select(entry => entry with
                {
                    Revision = checked(entry.Revision + 1),
                    OwnershipVersion = checked(entry.OwnershipVersion + (retired.ContainsKey(entry.NpcId) ? 1 : 0))
                }).ToArray()
            };
        }
        await using (var save = connection.CreateCommand())
        {
            save.Transaction = transaction;
            save.CommandText = "UPDATE npc_checkpoint_writes SET response_payload=@response, accepted=@accepted WHERE request_id=@request";
            save.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            save.Parameters.AddWithValue("@response", MessagePackSerializer.Serialize(response));
            save.Parameters.AddWithValue("@accepted", response.Accepted);
            await save.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return response;
    }

    public async Task<NpcCheckpointRecoveryV1?> GetCheckpointAsync(Guid checkpointId, string instanceId,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || checkpointId == Guid.Empty || string.IsNullOrWhiteSpace(instanceId) || instanceId.Length > 96)
            return null;
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var stored = await ReadCheckpointAsync(connection, transaction, checkpointId, cancellationToken);
        if (stored is null || !stored.Value.Recovery.Result.Accepted ||
            stored.Value.Recovery.Snapshot.InstanceId != instanceId || stored.Value.Recovery.Snapshot.Mission is not null)
            return null;
        var recovery = stored.Value.Recovery;
        if (recovery.Snapshot.Npcs.Length == 0) return null; // No simulation remains to restore.
        var rows = await LockCheckpointMembersAsync(connection, transaction,
            recovery.Snapshot.Npcs.Select(npc => npc.NpcId).ToArray(), cancellationToken);
        var revisions = recovery.Result.Revisions.ToDictionary(entry => entry.NpcId);
        if (recovery.Snapshot.Npcs.Any(npc => !rows.TryGetValue(npc.NpcId, out var row) ||
                row.InstanceId != instanceId || row.Retired || row.HasTransfer || row.CheckpointId != checkpointId ||
                row.Version != npc.OwnershipVersion || row.Revision != revisions[npc.NpcId].Revision ||
                !string.Equals(row.SystemId, recovery.Snapshot.SystemId, StringComparison.OrdinalIgnoreCase)))
            return null;
        await transaction.CommitAsync(cancellationToken);
        return recovery;
    }

    public async Task<NpcCheckpointRecoveryPageV1> GetRecoverableCheckpointsAsync(string instanceId,
        Guid? afterCheckpointId, int limit, CancellationToken cancellationToken = default, string? systemId = null)
    {
        if (limit is < 1 or > 128) throw new ArgumentOutOfRangeException(nameof(limit));
        if (!IsEnabled || string.IsNullOrWhiteSpace(instanceId) || instanceId.Length > 96 ||
            systemId is not null && (string.IsNullOrWhiteSpace(systemId) || systemId.Length > 96)) return new();
        systemId = systemId?.ToLowerInvariant();
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        // Discover only current pointers, not all historical runtime blobs. Direct
        // recovery rechecks the complete group before handing out snapshot data.
        command.CommandText = "SELECT DISTINCT checkpoint_id FROM npc_ownership_leases WHERE instance_id=@instance AND (@system IS NULL OR system_id=@system) AND retired_at_utc IS NULL AND active_transfer_id IS NULL AND checkpoint_id IS NOT NULL AND (@after IS NULL OR checkpoint_id>@after) ORDER BY checkpoint_id LIMIT @take";
        command.Parameters.AddWithValue("@instance", instanceId);
        command.Parameters.AddWithValue("@system", (object?)systemId ?? DBNull.Value);
        command.Parameters.AddWithValue("@after", afterCheckpointId?.ToString("D"));
        command.Parameters.AddWithValue("@take", limit + 1);
        var ids = new List<Guid>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken)) ids.Add(ReadGuid(reader, 0));
        var more = ids.Count > limit;
        if (more) ids.RemoveAt(ids.Count - 1);
        return new() { CheckpointIds = ids.ToArray(), NextAfterCheckpointId = more ? ids[^1] : null };
    }

    internal static async Task<Dictionary<Guid, CheckpointLease>> LockCheckpointMembersAsync(MySqlConnection connection,
        MySqlTransaction transaction, Guid[] ids, CancellationToken cancellationToken)
    {
        if (ids.Length == 0) return [];
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        var sorted = ids.OrderBy(id => id.ToString("D"), StringComparer.Ordinal).ToArray();
        command.CommandText = $"SELECT npc_id, instance_id, system_id, ownership_version, active_transfer_id, retired_at_utc, checkpoint_revision, checkpoint_id FROM npc_ownership_leases WHERE npc_id IN ({string.Join(',', sorted.Select((_, i) => $"@npc{i}"))}) ORDER BY npc_id FOR UPDATE";
        for (var i = 0; i < sorted.Length; i++) command.Parameters.AddWithValue($"@npc{i}", sorted[i].ToString("D"));
        var rows = new Dictionary<Guid, CheckpointLease>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            rows.Add(ReadGuid(reader, 0), new(reader.GetString(1), reader.GetString(2), reader.GetInt64(3),
                !reader.IsDBNull(4), !reader.IsDBNull(5), reader.GetInt64(6), reader.IsDBNull(7) ? null : ReadGuid(reader, 7)));
        return rows;
    }

    internal static async Task<bool> IncludesCheckpointGroupsAsync(MySqlConnection connection, MySqlTransaction transaction,
        Dictionary<Guid, CheckpointLease> rows, HashSet<Guid> memberIds, CancellationToken cancellationToken)
    {
        var groups = rows.Values.Where(row => row.CheckpointId.HasValue).Select(row => row.CheckpointId!.Value).Distinct().Order().ToArray();
        if (groups.Length == 0) return true;
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT npc_id FROM npc_ownership_leases WHERE checkpoint_id IN ({string.Join(',', groups.Select((_, i) => $"@group{i}"))}) ORDER BY npc_id FOR UPDATE";
        for (var i = 0; i < groups.Length; i++) command.Parameters.AddWithValue($"@group{i}", groups[i].ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            if (!memberIds.Contains(ReadGuid(reader, 0))) return false;
        return true;
    }

    private static string CheckpointRetirementIds(NpcCheckpointRevisionV1[] entries,
        Dictionary<Guid, NpcRetirementEntryV1> retired)
    {
        var ids = entries.Select((entry, i) => retired.ContainsKey(entry.NpcId) ? $"@npc{i}" : null).Where(id => id is not null).ToArray();
        return ids.Length == 0 ? "NULL" : string.Join(',', ids);
    }

    private static async Task<(byte[] Payload, NpcCheckpointRecoveryV1 Recovery)?> ReadCheckpointAsync(
        MySqlConnection connection, MySqlTransaction? transaction, Guid checkpointId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT request_payload, response_payload FROM npc_checkpoint_writes WHERE request_id=@request" + (transaction is null ? "" : " FOR UPDATE");
        command.Parameters.AddWithValue("@request", checkpointId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        if (reader.IsDBNull(1)) throw new InvalidDataException("NPC checkpoint has no committed response.");
        var bytes = (byte[])reader.GetValue(0);
        return (bytes, new() { Snapshot = MessagePackSerializer.Deserialize<NpcCheckpointWriteRequestV1>(bytes),
            Result = MessagePackSerializer.Deserialize<NpcCheckpointWriteResponseV1>((byte[])reader.GetValue(1)) });
    }

    private static NpcCheckpointWriteResponseV1 CheckpointRejected(Guid requestId, string reason) => new()
    { RequestId = requestId, ReasonCode = reason };

    internal sealed record CheckpointLease(string InstanceId, string SystemId, long Version, bool HasTransfer,
        bool Retired, long Revision, Guid? CheckpointId);
}
