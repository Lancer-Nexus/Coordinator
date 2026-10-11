using LancerNexus.Protocol;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Reads the full seeded goods catalog, including goods with no market offer.</summary>
public sealed class MySqlMarketCatalogReadStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public async Task<MarketCatalogReadResponseV1> ReadAsync(MarketCatalogReadRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            throw new ProtocolViolationException("Market catalog request is invalid.");
        if (connectionString is null)
            return Unavailable(request);

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            byte[]? hash;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "SELECT seed_sha256 FROM market_catalog_seed WHERE seed_slot = 1";
                var value = await command.ExecuteScalarAsync(cancellationToken);
                hash = value is byte[] bytes ? bytes : null;
            }
            if (hash is null || hash.Length != 32)
                return Unavailable(request);
            var revision = Convert.ToHexString(hash).ToLowerInvariant();
            if (request.ExpectedCatalogRevision is { } expected && expected != revision)
                return new MarketCatalogReadResponseV1
                {
                    CorrelationId = request.CorrelationId,
                    Status = MarketCatalogReadStatusV1.RevisionExpired
                };

            var goods = new List<MarketCatalogGoodV1>(request.PageSize + 1);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = request.AfterGoodId is null
                    ? "SELECT good_id, category, base_unit_price FROM market_catalog_goods ORDER BY good_id LIMIT @limit"
                    : "SELECT good_id, category, base_unit_price FROM market_catalog_goods WHERE good_id > @after ORDER BY good_id LIMIT @limit";
                if (request.AfterGoodId is not null)
                    command.Parameters.AddWithValue("@after", request.AfterGoodId);
                command.Parameters.AddWithValue("@limit", request.PageSize + 1);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    goods.Add(new MarketCatalogGoodV1
                    {
                        GoodId = reader.GetString(0),
                        Category = reader.GetString(1),
                        BaseUnitPrice = reader.IsDBNull(2) ? null : reader.GetInt64(2)
                    });
            }
            var hasMore = goods.Count > request.PageSize;
            if (hasMore)
                goods.RemoveAt(goods.Count - 1);
            var response = new MarketCatalogReadResponseV1
            {
                CorrelationId = request.CorrelationId,
                Status = MarketCatalogReadStatusV1.Ok,
                CatalogRevision = revision,
                Goods = [.. goods],
                NextGoodId = hasMore ? goods[^1].GoodId : null
            };
            if (!response.IsValidFor(request))
                throw new InvalidOperationException("Stored market catalog violates the read contract.");
            return response;
        }
        catch (MySqlException)
        {
            return Unavailable(request);
        }
    }

    private static MarketCatalogReadResponseV1 Unavailable(MarketCatalogReadRequestV1 request) => new()
    {
        CorrelationId = request.CorrelationId,
        Status = MarketCatalogReadStatusV1.Unavailable
    };
}
