using LancerNexus.Protocol;
using MySqlConnector;
using System.Text;

namespace LancerNexus.Coordinator;

/// <summary>Authoritative, cross-process NPC identity and ownership registry.</summary>
public sealed class MySqlNpcOwnershipStore
{
    public const int MaximumBatchSize = 256;
    private readonly string? connectionString;

    public MySqlNpcOwnershipStore(string? connectionString) => this.connectionString =
        string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public bool IsEnabled => connectionString is not null;

    public async Task<NpcIdBatchAllocationResponse> AllocateAsync(
        NpcIdBatchAllocationRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!IsEnabled)
            return Rejected(request.RequestId, "npc_ownership_unavailable");
        if (request.RequestId == Guid.Empty || string.IsNullOrWhiteSpace(request.InstanceId) ||
            request.InstanceId.Length > 96 || string.IsNullOrWhiteSpace(request.SystemId) ||
            request.SystemId.Length > 96 || request.Count is 0 or > MaximumBatchSize)
            return Rejected(request.RequestId, "invalid_npc_allocation_request");

        var systemId = request.SystemId.ToLowerInvariant();
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var existing = await ReadBatchAsync(connection, transaction, request, cancellationToken);
        if (existing is not null)
        {
            await transaction.CommitAsync(cancellationToken);
            return existing;
        }

        try
        {
            await using (var batch = connection.CreateCommand())
            {
                batch.Transaction = transaction;
                batch.CommandText = """
                    INSERT INTO npc_id_allocation_batches
                        (request_id, instance_id, system_id, requested_count, created_at_utc)
                    VALUES (@request, @instance, @system, @count, UTC_TIMESTAMP(6))
                    """;
                batch.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
                batch.Parameters.AddWithValue("@instance", request.InstanceId);
                batch.Parameters.AddWithValue("@system", systemId);
                batch.Parameters.AddWithValue("@count", request.Count);
                await batch.ExecuteNonQueryAsync(cancellationToken);
            }

            var leases = new NpcOwnershipLease[request.Count];
            var values = new StringBuilder();
            await using (var insert = connection.CreateCommand())
            {
                insert.Transaction = transaction;
                for (var i = 0; i < leases.Length; i++)
                {
                    if (i > 0)
                        values.Append(',');
                    values.Append("(@npc").Append(i).Append(", @instance, @system, 1, NULL, @request, @ordinal")
                        .Append(i).Append(", UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))");
                    var npcId = Guid.CreateVersion7();
                    leases[i] = new NpcOwnershipLease
                    {
                        NpcId = npcId,
                        InstanceId = request.InstanceId,
                        OwnershipVersion = 1
                    };
                    insert.Parameters.AddWithValue($"@npc{i}", npcId.ToString("D"));
                    insert.Parameters.AddWithValue($"@ordinal{i}", i);
                }
                insert.CommandText = $"""
                    INSERT INTO npc_ownership_leases
                        (npc_id, instance_id, system_id, ownership_version, active_transfer_id,
                         allocation_request_id, allocation_ordinal, created_at_utc, updated_at_utc)
                    VALUES {values}
                    """;
                insert.Parameters.AddWithValue("@instance", request.InstanceId);
                insert.Parameters.AddWithValue("@system", systemId);
                insert.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
                await insert.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            return new()
            {
                RequestId = request.RequestId,
                Accepted = true,
                ReasonCode = "allocated",
                Npcs = leases
            };
        }
        catch (MySqlException exception) when (exception.Number == 1062)
        {
            await transaction.RollbackAsync(cancellationToken);
            return await ReadBatchOnNewConnectionAsync(request, cancellationToken);
        }
    }

    private async Task<NpcIdBatchAllocationResponse> ReadBatchOnNewConnectionAsync(
        NpcIdBatchAllocationRequest request,
        CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var result = await ReadBatchAsync(connection, null, request, cancellationToken);
        return result ?? Rejected(request.RequestId, "npc_allocation_conflict");
    }

    private static async Task<NpcIdBatchAllocationResponse?> ReadBatchAsync(
        MySqlConnection connection,
        MySqlTransaction? transaction,
        NpcIdBatchAllocationRequest request,
        CancellationToken cancellationToken)
    {
        string? instanceId;
        string? systemId;
        ushort requestedCount;
        await using (var batch = connection.CreateCommand())
        {
            batch.Transaction = transaction;
            batch.CommandText = """
                SELECT instance_id, system_id, requested_count
                FROM npc_id_allocation_batches
                WHERE request_id = @request
                """;
            batch.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return null;
            instanceId = reader.GetString(0);
            systemId = reader.GetString(1);
            requestedCount = reader.GetUInt16(2);
        }

        if (!string.Equals(instanceId, request.InstanceId, StringComparison.Ordinal) ||
            !string.Equals(systemId, request.SystemId, StringComparison.OrdinalIgnoreCase) ||
            requestedCount != request.Count)
            return Rejected(request.RequestId, "npc_allocation_request_id_conflict");

        var leases = new List<NpcOwnershipLease>(requestedCount);
        await using (var rows = connection.CreateCommand())
        {
            rows.Transaction = transaction;
            rows.CommandText = """
                SELECT npc_id, instance_id, ownership_version, active_transfer_id
                FROM npc_ownership_leases
                WHERE allocation_request_id = @request
                ORDER BY allocation_ordinal
                """;
            rows.Parameters.AddWithValue("@request", request.RequestId.ToString("D"));
            await using var reader = await rows.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                leases.Add(new NpcOwnershipLease
                {
                    NpcId = ReadGuid(reader, 0),
                    InstanceId = reader.GetString(1),
                    OwnershipVersion = reader.GetInt64(2),
                    ActiveTransferId = reader.IsDBNull(3) ? null : ReadGuid(reader, 3)
                });
            }
        }

        if (leases.Count != requestedCount)
            throw new InvalidDataException("NPC allocation batch exists without all of its ownership rows.");
        return new()
        {
            RequestId = request.RequestId,
            Accepted = true,
            Duplicate = true,
            ReasonCode = "duplicate_allocation_request",
            Npcs = leases.ToArray()
        };
    }

    private static NpcIdBatchAllocationResponse Rejected(Guid requestId, string reasonCode) => new()
    {
        RequestId = requestId,
        ReasonCode = reasonCode,
        Npcs = []
    };

    private static Guid ReadGuid(MySqlDataReader reader, int ordinal) =>
        reader.GetValue(ordinal) is Guid value ? value : Guid.Parse(reader.GetString(ordinal));
}
