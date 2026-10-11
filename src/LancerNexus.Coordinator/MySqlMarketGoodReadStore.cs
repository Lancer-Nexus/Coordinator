using LancerNexus.Protocol;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Reads one exact market-good row from the cluster-shared MySQL projection.</summary>
public sealed class MySqlMarketGoodReadStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public async Task<MarketGoodReadResponseV1> ReadAsync(MarketGoodReadRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            throw new ProtocolViolationException("Market-good read request is invalid.");
        if (connectionString is null)
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

            if (request.ExpectedSystemRevision is long expectedSystem && expectedSystem != systemRevision)
                return RevisionMismatch(request, systemRevision);

            MarketGoodSnapshotV1 good;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    SELECT market_id, system_id, good_id, base_unit_price, current_unit_price,
                           available_stock - reserved_buy_stock, target_stock, buy_enabled, sell_enabled, market_revision
                    FROM market_good_state
                    WHERE market_id = @market AND system_id = @system AND good_id = @good
                    """;
                command.Parameters.AddWithValue("@market", request.MarketId);
                command.Parameters.AddWithValue("@system", request.SystemId);
                command.Parameters.AddWithValue("@good", request.GoodId);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                    return new MarketGoodReadResponseV1
                    {
                        CorrelationId = request.CorrelationId,
                        SystemId = request.SystemId,
                        MarketId = request.MarketId,
                        GoodId = request.GoodId,
                        Status = MarketGoodReadStatusV1.NotFound,
                        SystemRevision = systemRevision
                    };

                good = new MarketGoodSnapshotV1
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
                };
            }

            if (request.ExpectedMarketRevision is long expectedMarket && expectedMarket != good.MarketRevision)
                return RevisionMismatch(request, systemRevision, good.MarketRevision);

            var response = new MarketGoodReadResponseV1
            {
                CorrelationId = request.CorrelationId,
                SystemId = request.SystemId,
                MarketId = request.MarketId,
                GoodId = request.GoodId,
                Status = MarketGoodReadStatusV1.Found,
                SystemRevision = systemRevision,
                MarketRevision = good.MarketRevision,
                Good = good
            };
            if (!response.IsValidFor(request))
                throw new InvalidOperationException("Stored market projection violates the market-good read contract.");
            return response;
        }
        catch (MySqlException)
        {
            return Unavailable(request);
        }
    }

    private static MarketGoodReadResponseV1 RevisionMismatch(MarketGoodReadRequestV1 request,
        long systemRevision, long marketRevision = 0) => new()
    {
        CorrelationId = request.CorrelationId,
        SystemId = request.SystemId,
        MarketId = request.MarketId,
        GoodId = request.GoodId,
        Status = MarketGoodReadStatusV1.RevisionMismatch,
        SystemRevision = systemRevision,
        MarketRevision = marketRevision
    };

    private static MarketGoodReadResponseV1 Unavailable(MarketGoodReadRequestV1 request) => new()
    {
        CorrelationId = request.CorrelationId,
        SystemId = request.SystemId,
        MarketId = request.MarketId,
        GoodId = request.GoodId,
        Status = MarketGoodReadStatusV1.Unavailable
    };
}
