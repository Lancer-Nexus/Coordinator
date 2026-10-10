using System.Security.Cryptography;
using System.Globalization;
using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Durable idempotent storage for instance lifecycle events.</summary>
public sealed class MySqlFlHookEventStore(string? connectionString) : IFlHookEventStore
{
    public const int MaximumEventBytes = 64 * 1024;
    private const int MaximumReplayPayloadBytes = checked((int)ClusterEnvelopeValidator.MaxPayloadLength);
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

    public async Task<FlHookEventReplayResultV1> ReplayAsync(FlHookEventReplayRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            throw new ProtocolViolationException("FLHook replay request is invalid.");
        if (!IsEnabled)
            throw new InvalidOperationException("FLHook event replay storage is not configured.");

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            string? epochText;
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = """
                    SELECT producer_epoch FROM flhook_server_events
                    WHERE instance_id = @instance
                    ORDER BY received_utc DESC, producer_epoch DESC, sequence_number DESC
                    LIMIT 1
                    """;
                command.Parameters.AddWithValue("@instance", request.InstanceId);
                var epochValue = await command.ExecuteScalarAsync(cancellationToken);
                epochText = epochValue is null or DBNull
                    ? null
                    : Convert.ToString(epochValue, CultureInfo.InvariantCulture);
            }

            if (epochText is null)
            {
                if (request.AfterSequence != 0 || request.ProducerEpoch is not null)
                    throw new ProtocolViolationException("FLHook replay cursor cannot be reconciled because the instance has no stored history.");
                return new FlHookEventReplayResultV1
                {
                    CorrelationId = request.CorrelationId,
                    InstanceId = request.InstanceId,
                    RequestedSequence = 0
                };
            }

            if (!Guid.TryParseExact(epochText, "D", out var currentEpoch) || currentEpoch == Guid.Empty)
                throw new ProtocolViolationException("Stored FLHook producer epoch is malformed.");
            var epochChanged = request.ProducerEpoch is Guid requestedEpoch && requestedEpoch != currentEpoch;
            long oldest;
            long latest;
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = """
                    SELECT MIN(sequence_number), MAX(sequence_number) FROM flhook_server_events
                    WHERE instance_id = @instance AND producer_epoch = @epoch
                    """;
                command.Parameters.AddWithValue("@instance", request.InstanceId);
                command.Parameters.AddWithValue("@epoch", currentEpoch.ToString("D"));
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0) || reader.IsDBNull(1))
                    throw new ProtocolViolationException("Selected FLHook producer epoch has no stored sequence range.");
                oldest = reader.GetInt64(0);
                latest = reader.GetInt64(1);
            }

            if (!epochChanged && request.AfterSequence > latest)
                throw new ProtocolViolationException("FLHook replay cursor is newer than the stored event history.");

            var startSequence = epochChanged ? 0 : request.AfterSequence;
            var events = new List<FlHookEventV1>(request.MaximumEvents);
            var payloadBytes = 0;
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = """
                    SELECT event_payload FROM flhook_server_events
                    WHERE instance_id = @instance AND producer_epoch = @epoch AND sequence_number > @after
                    ORDER BY sequence_number
                    LIMIT @maximum
                    """;
                command.Parameters.AddWithValue("@instance", request.InstanceId);
                command.Parameters.AddWithValue("@epoch", currentEpoch.ToString("D"));
                command.Parameters.AddWithValue("@after", startSequence);
                command.Parameters.AddWithValue("@maximum", request.MaximumEvents);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var payload = (byte[])reader[0];
                    if (payload.Length > MaximumEventBytes)
                        throw new ProtocolViolationException("Stored FLHook event exceeds the replay payload limit.");
                    var value = MessagePackSerializer.Deserialize<FlHookEventV1>(payload,
                        MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData));
                    if (!IsClusterEventValid(value) || value.InstanceId != request.InstanceId ||
                        value.ProducerEpoch != currentEpoch)
                        throw new ProtocolViolationException("Stored FLHook event does not match its database identity.");
                    // Leave room for the result fields and MessagePack array overhead.
                    if (payloadBytes + payload.Length + 256 > MaximumReplayPayloadBytes)
                    {
                        if (events.Count == 0)
                            throw new ProtocolViolationException("A stored FLHook event cannot fit in a replay response.");
                        break;
                    }
                    events.Add(value);
                    payloadBytes += payload.Length;
                }
            }

            var result = new FlHookEventReplayResultV1
            {
                CorrelationId = request.CorrelationId,
                InstanceId = request.InstanceId,
                OldestAvailableSequence = oldest,
                LatestSequence = latest,
                Gap = epochChanged || request.AfterSequence < oldest - 1,
                Events = events.ToArray(),
                RequestedSequence = request.AfterSequence,
                ProducerEpoch = currentEpoch,
                RequestedProducerEpoch = request.ProducerEpoch
            };
            if (!result.IsValid() || MessagePackSerializer.Serialize(result).Length > MaximumReplayPayloadBytes)
                throw new ProtocolViolationException("FLHook replay result cannot be represented within the cluster frame limit.");
            return result;
        }
        catch (MySqlException)
        {
            throw new IOException("FLHook event replay storage is unavailable.");
        }
    }

    private static bool IsClusterEventValid(FlHookEventV1 value) => value.IsValid() &&
        value.ProducerEpoch != Guid.Empty;
}
