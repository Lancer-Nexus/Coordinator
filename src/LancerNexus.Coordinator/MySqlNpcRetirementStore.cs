using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

public sealed partial class MySqlNpcOwnershipStore
{
    public async Task<NpcRetirementResponseV1> RetireAsync(NpcRetirementRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        if (!request.IsValid())
            return new() { RequestId = request.RequestId, ReasonCode = "invalid_npc_retirement_request" };
        if (!IsEnabled)
            return new() { RequestId = request.RequestId, ReasonCode = "npc_ownership_unavailable" };

        var payload = MessagePackSerializer.Serialize(request);
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = "INSERT INTO npc_retirement_batches (request_id, request_payload, created_at_utc) VALUES (@request, @payload, UTC_TIMESTAMP(6))";
            insert.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            insert.Parameters.AddWithValue("@payload", payload);
            try { await insert.ExecuteNonQueryAsync(cancellationToken); }
            catch (MySqlException exception) when (exception.Number == 1062)
            {
                await transaction.RollbackAsync(cancellationToken);
                return await ReadRetirementBatchAsync(connection, request.RequestId, payload, cancellationToken);
            }
        }

        var rows = new Dictionary<Guid, RetirementLease>();
        var entries = request.Npcs.OrderBy(npc => npc.NpcId.ToString("D"), StringComparer.Ordinal).ToArray();
        await using (var select = connection.CreateCommand())
        {
            select.Transaction = transaction;
            var parameters = entries.Select((npc, i) => $"@npc{i}").ToArray();
            select.CommandText = $"SELECT npc_id, instance_id, ownership_version, active_transfer_id, retired_at_utc, retirement_reason, checkpoint_id FROM npc_ownership_leases WHERE npc_id IN ({string.Join(',', parameters)}) ORDER BY npc_id FOR UPDATE";
            for (var i = 0; i < entries.Length; i++)
                select.Parameters.AddWithValue(parameters[i], entries[i].NpcId.ToString("D"));
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                rows.Add(ReadGuid(reader, 0), new(reader.GetString(1), reader.GetInt64(2),
                    !reader.IsDBNull(3), !reader.IsDBNull(4), reader.IsDBNull(5) ? null : reader.GetByte(5), !reader.IsDBNull(6)));
        }

        var updates = new List<NpcRetirementEntryV1>();
        var results = request.Npcs.Select(npc =>
        {
            var code = "npc_not_found";
            var version = 0L;
            var accepted = false;
            if (rows.TryGetValue(npc.NpcId, out var row))
            {
                version = row.Version;
                if (row.InstanceId != request.InstanceId)
                    code = "npc_ownership_conflict";
                else if (row.Retired)
                {
                    accepted = row.Version == npc.OwnershipVersion + 1 && row.Reason == (byte)npc.Reason;
                    code = accepted ? "already_retired" : "npc_ownership_conflict";
                }
                else if (row.Version != npc.OwnershipVersion)
                    code = "npc_ownership_conflict";
                else if (row.HasTransfer)
                    code = "npc_transfer_in_progress";
                else if (row.HasCheckpoint)
                    code = "npc_checkpoint_required";
                else
                {
                    accepted = true;
                    version++;
                    code = "retired";
                    updates.Add(npc);
                }
            }
            return new NpcRetirementResultV1
            {
                NpcId = npc.NpcId,
                Accepted = accepted,
                ReasonCode = code,
                OwnershipVersion = version
            };
        }).ToArray();

        if (updates.Count > 0)
        {
            await using var update = connection.CreateCommand();
            update.Transaction = transaction;
            var ids = updates.Select((npc, i) => $"@npc{i}").ToArray();
            var cases = updates.Select((npc, i) => $"WHEN @npc{i} THEN @reason{i}");
            update.CommandText = $"UPDATE npc_ownership_leases SET ownership_version=ownership_version+1, retired_at_utc=UTC_TIMESTAMP(6), retirement_reason=CASE npc_id {string.Join(' ', cases)} END, updated_at_utc=UTC_TIMESTAMP(6) WHERE instance_id=@instance AND retired_at_utc IS NULL AND active_transfer_id IS NULL AND npc_id IN ({string.Join(',', ids)})";
            update.Parameters.AddWithValue("@instance", request.InstanceId);
            for (var i = 0; i < updates.Count; i++)
            {
                update.Parameters.AddWithValue(ids[i], updates[i].NpcId.ToString("D"));
                update.Parameters.AddWithValue($"@reason{i}", (byte)updates[i].Reason);
            }
            if (await update.ExecuteNonQueryAsync(cancellationToken) != updates.Count)
                throw new InvalidDataException("NPC retirement lost a locked ownership row.");
        }

        var response = new NpcRetirementResponseV1 { RequestId = request.RequestId, Npcs = results, ReasonCode = "processed" };
        await using (var save = connection.CreateCommand())
        {
            save.Transaction = transaction;
            save.CommandText = "UPDATE npc_retirement_batches SET response_payload=@response WHERE request_id=@request";
            save.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            save.Parameters.AddWithValue("@response", MessagePackSerializer.Serialize(response));
            await save.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return response;
    }

    private static async Task<NpcRetirementResponseV1> ReadRetirementBatchAsync(MySqlConnection connection,
        Guid requestId, byte[] payload, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT request_payload, response_payload FROM npc_retirement_batches WHERE request_id=@request";
        command.Parameters.AddWithValue("@request", requestId.ToString("D"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(1))
            throw new InvalidDataException("NPC retirement batch has no committed response.");
        if (!((byte[])reader.GetValue(0)).AsSpan().SequenceEqual(payload))
            return new() { RequestId = requestId, ReasonCode = "npc_retirement_request_id_conflict" };
        return MessagePackSerializer.Deserialize<NpcRetirementResponseV1>((byte[])reader.GetValue(1));
    }

    private sealed record RetirementLease(string InstanceId, long Version, bool HasTransfer, bool Retired, byte? Reason, bool HasCheckpoint);
}
