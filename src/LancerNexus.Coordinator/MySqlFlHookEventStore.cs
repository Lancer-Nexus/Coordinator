using System.Security.Cryptography;
using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Durable idempotent storage for instance lifecycle events.</summary>
public sealed class MySqlFlHookEventStore(string? connectionString) : IFlHookEventStore
{
    public const int MaximumEventBytes = 64 * 1024;
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public bool IsEnabled => connectionString is not null;

    public async Task<FlHookEventIngestStatusV1> IngestAsync(FlHookEventV1 value,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (!IsEnabled)
            return FlHookEventIngestStatusV1.Unavailable;
        if (!IsClusterEventValid(value))
            return FlHookEventIngestStatusV1.Rejected;

        var payload = MessagePackSerializer.Serialize(value);
        if (payload.Length > MaximumEventBytes)
            return FlHookEventIngestStatusV1.Rejected;
        var digest = SHA256.HashData(payload);

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            int inserted;
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = """
                    INSERT IGNORE INTO flhook_server_events
                        (instance_id, producer_epoch, sequence_number, occurred_utc, payload_sha256, event_payload)
                    VALUES (@instance, @epoch, @sequence, @occurred, @digest, @payload)
                    """;
                command.Parameters.AddWithValue("@instance", value.InstanceId);
                command.Parameters.AddWithValue("@epoch", value.ProducerEpoch.ToString("D"));
                command.Parameters.AddWithValue("@sequence", value.Sequence);
                command.Parameters.AddWithValue("@occurred", value.OccurredUtc.UtcDateTime);
                command.Parameters.AddWithValue("@digest", digest);
                command.Parameters.AddWithValue("@payload", payload);
                inserted = await command.ExecuteNonQueryAsync(cancellationToken);
            }

            if (inserted == 1)
                return FlHookEventIngestStatusV1.Stored;

            byte[]? existingDigest;
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = """
                    SELECT payload_sha256 FROM flhook_server_events
                    WHERE instance_id = @instance AND producer_epoch = @epoch AND sequence_number = @sequence
                    FOR UPDATE
                    """;
                command.Parameters.AddWithValue("@instance", value.InstanceId);
                command.Parameters.AddWithValue("@epoch", value.ProducerEpoch.ToString("D"));
                command.Parameters.AddWithValue("@sequence", value.Sequence);
                existingDigest = await command.ExecuteScalarAsync(cancellationToken) as byte[];
            }
            return existingDigest is not null && CryptographicOperations.FixedTimeEquals(existingDigest, digest)
                ? FlHookEventIngestStatusV1.Duplicate
                : FlHookEventIngestStatusV1.Conflict;
        }
        catch (MySqlException)
        {
            return FlHookEventIngestStatusV1.Unavailable;
        }
    }

    private static bool IsClusterEventValid(FlHookEventV1 value) => value.IsValid() &&
        value.ProducerEpoch != Guid.Empty;
}
