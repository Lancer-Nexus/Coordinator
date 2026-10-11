using LancerNexus.Protocol;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Reads a revision-pinned market projection shared by all Coordinator replicas.</summary>
public sealed class MySqlMarketSnapshotStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public bool IsEnabled => connectionString is not null;

    public async Task<MarketSnapshotReadResponseV1> ReadAsync(MarketSnapshotReadRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            throw new ProtocolViolationException("Market snapshot request is invalid.");
        if (!IsEnabled)
            return Unavailable(request);

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            long systemRevision;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "SELECT system_revision FROM market_system_revisions WHERE system_id = @system";
                command.Parameters.AddWithValue("@system", request.SystemId);
                var revision = await command.ExecuteScalarAsync(cancellationToken);
                if (revision is null or DBNull)
                    return Unavailable(request);
                systemRevision = Convert.ToInt64(revision, System.Globalization.CultureInfo.InvariantCulture);
            }

            if (request.ExpectedSystemRevision is long expected && expected != systemRevision)
                return new MarketSnapshotReadResponseV1
                {
                    CorrelationId = request.CorrelationId,
                    SystemId = request.SystemId,
                    Status = MarketSnapshotReadStatusV1.RevisionExpired
                };

            var rows = new List<MarketGoodSnapshotV1>(request.PageSize + 1);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = request.AfterMarketId is null
                    ? """
                      SELECT market_id, system_id, good_id, base_unit_price, current_unit_price,
                             available_stock, target_stock, buy_enabled, sell_enabled, market_revision
                      FROM market_good_state
                      WHERE system_id = @system
                      ORDER BY market_id, good_id
                      LIMIT @limit
                      """
                    : """
                      SELECT market_id, system_id, good_id, base_unit_price, current_unit_price,
                             available_stock, target_stock, buy_enabled, sell_enabled, market_revision
                      FROM market_good_state
                      WHERE system_id = @system
                        AND (market_id > @afterMarket OR (market_id = @afterMarket AND good_id > @afterGood))
                      ORDER BY market_id, good_id
                      LIMIT @limit
                      """;
                command.Parameters.AddWithValue("@system", request.SystemId);
                command.Parameters.AddWithValue("@limit", request.PageSize + 1);
                if (request.AfterMarketId is not null)
                {
                    command.Parameters.AddWithValue("@afterMarket", request.AfterMarketId);
                    command.Parameters.AddWithValue("@afterGood", request.AfterGoodId);
                }
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add(new MarketGoodSnapshotV1
                    {
                        MarketId = reader.GetString(0),
                        SystemId = reader.GetString(1),
                        GoodId = reader.GetString(2),
                        BaseUnitPrice = reader.GetInt64(3),
                        CurrentUnitPrice = reader.GetInt64(4),
                        AvailableStock = reader.GetInt64(5),
                        TargetStock = reader.GetInt64(6),
                        BuyEnabled = reader.GetBoolean(7),
                        SellEnabled = reader.GetBoolean(8),
                        MarketRevision = reader.GetInt64(9)
                    });
                }
            }

            var hasMore = rows.Count > request.PageSize;
            if (hasMore)
                rows.RemoveAt(rows.Count - 1);
            var last = hasMore ? rows[^1] : null;
            var response = new MarketSnapshotReadResponseV1
            {
                CorrelationId = request.CorrelationId,
                SystemId = request.SystemId,
                Status = MarketSnapshotReadStatusV1.Ok,
                SystemRevision = systemRevision,
                Goods = [.. rows],
                NextMarketId = last?.MarketId,
                NextGoodId = last?.GoodId
            };
            if (!response.IsValidFor(request))
                throw new InvalidOperationException("Stored market projection violates the snapshot contract.");
            return response;
        }
        catch (MySqlException)
        {
            return Unavailable(request);
        }
    }

    private static MarketSnapshotReadResponseV1 Unavailable(MarketSnapshotReadRequestV1 request) => new()
    {
        CorrelationId = request.CorrelationId,
        SystemId = request.SystemId,
        Status = MarketSnapshotReadStatusV1.Unavailable
    };
}
