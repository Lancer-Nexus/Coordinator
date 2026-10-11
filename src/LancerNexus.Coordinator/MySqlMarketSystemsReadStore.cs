using LancerNexus.Protocol;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Reads the seeded cluster system directory, including systems without shop offers.</summary>
public sealed class MySqlMarketSystemsReadStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public async Task<MarketSystemsReadResponseV1> ReadAsync(MarketSystemsReadRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            throw new ProtocolViolationException("Market systems request is invalid.");
        if (connectionString is null)
            return Unavailable(request);

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            byte[]? seedHash;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "SELECT seed_sha256 FROM market_catalog_seed WHERE seed_slot = 1";
                var value = await command.ExecuteScalarAsync(cancellationToken);
                seedHash = value is byte[] bytes && bytes.Length == 32 ? bytes : null;
            }
            if (seedHash is null)
                return Unavailable(request);
            var revision = Convert.ToHexString(seedHash).ToLowerInvariant();
            if (request.ExpectedCatalogRevision is not null && request.ExpectedCatalogRevision != revision)
                return new MarketSystemsReadResponseV1
                {
                    CorrelationId = request.CorrelationId,
                    Status = MarketCatalogReadStatusV1.RevisionExpired
                };

            var systems = new List<MarketSystemSummaryV1>(request.PageSize + 1);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = request.AfterSystemId is null
                    ? """
                      SELECT revision.system_id,
                             COUNT(DISTINCT location.market_id),
                             COUNT(DISTINCT offer.market_id),
                             COUNT(offer.good_id)
                      FROM market_system_revisions AS revision
                      LEFT JOIN market_locations AS location ON location.system_id = revision.system_id
                      LEFT JOIN market_static_offers AS offer ON offer.market_id = location.market_id
                      GROUP BY revision.system_id
                      ORDER BY revision.system_id
                      LIMIT @limit
                      """
                    : """
                      SELECT revision.system_id,
                             COUNT(DISTINCT location.market_id),
                             COUNT(DISTINCT offer.market_id),
                             COUNT(offer.good_id)
                      FROM market_system_revisions AS revision
                      LEFT JOIN market_locations AS location ON location.system_id = revision.system_id
                      LEFT JOIN market_static_offers AS offer ON offer.market_id = location.market_id
                      WHERE revision.system_id > @afterSystem
                      GROUP BY revision.system_id
                      ORDER BY revision.system_id
                      LIMIT @limit
                      """;
                command.Parameters.AddWithValue("@limit", request.PageSize + 1);
                if (request.AfterSystemId is not null)
                    command.Parameters.AddWithValue("@afterSystem", request.AfterSystemId);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    systems.Add(new MarketSystemSummaryV1
                    {
                        SystemId = reader.GetString(0),
                        MarketCount = reader.GetInt64(1),
                        OfferingMarketCount = reader.GetInt64(2),
                        StaticOfferCount = reader.GetInt64(3)
                    });
                }
            }

            var hasMore = systems.Count > request.PageSize;
            if (hasMore)
                systems.RemoveAt(systems.Count - 1);
            var response = new MarketSystemsReadResponseV1
            {
                CorrelationId = request.CorrelationId,
                Status = MarketCatalogReadStatusV1.Ok,
                CatalogRevision = revision,
                Systems = [.. systems],
                NextSystemId = hasMore ? systems[^1].SystemId : null
            };
            if (!response.IsValidFor(request))
                throw new InvalidOperationException("Stored market system directory violates its contract.");
            return response;
        }
        catch (MySqlException)
        {
            return Unavailable(request);
        }
    }

    private static MarketSystemsReadResponseV1 Unavailable(MarketSystemsReadRequestV1 request) => new()
    {
        CorrelationId = request.CorrelationId,
        Status = MarketCatalogReadStatusV1.Unavailable
    };
}
