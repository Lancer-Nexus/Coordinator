using System.Data;
using LancerNexus.Protocol;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Creates a non-reserving, reputation-aware preview from one shared market row.</summary>
public sealed class MySqlMarketQuoteStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public async Task<MarketQuoteResponseV1> ReadAsync(MarketQuoteRequestV1 request,
        MarketFactionReputationReadResponseV1 reputation, DateTimeOffset nowUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(reputation);
        if (!request.IsValid())
            throw new ProtocolViolationException("Market quote request is invalid.");
        if (connectionString is null)
            return Response(request, MarketQuoteStatusV1.Unavailable);

        var reputationRequest = new MarketFactionReputationReadRequestV1
        {
            CorrelationId = request.CorrelationId,
            CharacterId = request.CharacterId,
            ExpectedReputationRevision = request.ExpectedReputationRevision
        };
        if (!reputation.IsValidFor(reputationRequest))
            return Response(request, MarketQuoteStatusV1.Unavailable);
        if (reputation.Status == MarketFactionReputationStatusV1.RevisionMismatch)
            return Response(request, MarketQuoteStatusV1.ReputationRevisionMismatch,
                reputationRevision: reputation.ReputationRevision);
        if (reputation.Status == MarketFactionReputationStatusV1.NotFound)
            return Response(request, MarketQuoteStatusV1.NotFound);
        if (reputation.Status != MarketFactionReputationStatusV1.Found)
            return Response(request, MarketQuoteStatusV1.Unavailable);

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted,
                cancellationToken);
            long marketRevision;
            long basePrice;
            long availableStock;
            long targetStock;
            bool enabled;
            string marketFactionId;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    SELECT state.market_revision, state.base_unit_price,
                           state.available_stock - state.reserved_buy_stock, state.target_stock,
                           CASE WHEN @side = 1 THEN state.buy_enabled ELSE state.sell_enabled END,
                           location.local_faction_id
                    FROM market_good_state AS state
                    INNER JOIN market_locations AS location
                      ON location.market_id = state.market_id AND location.system_id = state.system_id
                    WHERE state.system_id = @system AND state.market_id = @market AND state.good_id = @good
                    """;
                command.Parameters.AddWithValue("@side", (byte)request.Side);
                command.Parameters.AddWithValue("@system", request.SystemId);
                command.Parameters.AddWithValue("@market", request.MarketId);
                command.Parameters.AddWithValue("@good", request.GoodId);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                    return Response(request, MarketQuoteStatusV1.NotFound);
                marketRevision = reader.GetInt64(0);
                basePrice = reader.GetInt64(1);
                availableStock = reader.GetInt64(2);
                targetStock = reader.GetInt64(3);
                enabled = reader.GetBoolean(4);
                marketFactionId = reader.GetString(5);
            }

            if (request.ExpectedMarketRevision is long expected && expected != marketRevision)
                return Response(request, MarketQuoteStatusV1.MarketRevisionMismatch,
                    marketRevision: marketRevision);
            if (!enabled || targetStock <= 0)
                return Response(request, MarketQuoteStatusV1.Rejected);

            var faction = reputation.Factions.FirstOrDefault(value => value.FactionId == marketFactionId);
            var appliedReputation = faction?.Value ?? 0f;
            if (!MarketReputationPricePolicy.TryGetFactor(request.Side, appliedReputation,
                    out var priceFactor, out var factorPartsPerMillion) ||
                !MarketPriceCalculator.TryCalculateTradeTotal(basePrice, targetStock, availableStock,
                    request.Side, request.Quantity, out var unitPrice, out var totalPrice, priceFactor))
                return Response(request, MarketQuoteStatusV1.Rejected);

            return new MarketQuoteResponseV1
            {
                CorrelationId = request.CorrelationId,
                CharacterId = request.CharacterId,
                SystemId = request.SystemId,
                MarketId = request.MarketId,
                GoodId = request.GoodId,
                Side = request.Side,
                Quantity = request.Quantity,
                Status = MarketQuoteStatusV1.Quoted,
                MarketRevision = marketRevision,
                ReputationRevision = reputation.ReputationRevision,
                MarketFactionId = marketFactionId,
                AppliedReputation = appliedReputation,
                ReputationFactorPartsPerMillion = factorPartsPerMillion,
                QuoteUnitPrice = unitPrice,
                TotalPrice = totalPrice,
                ExpiresUtc = nowUtc.ToUniversalTime().AddSeconds(15),
                PricingPolicyVersion = MarketReputationPricePolicy.Version
            };
        }
        catch (MySqlException)
        {
            return Response(request, MarketQuoteStatusV1.Unavailable);
        }
    }

    private static MarketQuoteResponseV1 Response(MarketQuoteRequestV1 request, MarketQuoteStatusV1 status,
        long marketRevision = 0, long reputationRevision = 0) => new()
    {
        CorrelationId = request.CorrelationId,
        CharacterId = request.CharacterId,
        SystemId = request.SystemId,
        MarketId = request.MarketId,
        GoodId = request.GoodId,
        Side = request.Side,
        Quantity = request.Quantity,
        Status = status,
        MarketRevision = marketRevision,
        ReputationRevision = reputationRevision
    };
}
