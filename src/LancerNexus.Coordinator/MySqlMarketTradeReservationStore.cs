using System.Data;
using System.Security.Cryptography;
using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Durable Gateway trade reservations over the cluster-shared market projection.</summary>
public sealed class MySqlMarketTradeReservationStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;
    public bool IsEnabled => connectionString is not null;

    public async Task<MarketTradeReservationResponseV1> ReserveAsync(MarketTradeReservationRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            return Response(request, MarketTradeReservationStatusV1.Rejected);
        if (connectionString is null)
            return Response(request, MarketTradeReservationStatusV1.Unavailable);

        var digest = SHA256.HashData(MessagePackSerializer.Serialize(new ReservationIdentity(
            request.TradeId, request.MarketId, request.SystemId, request.GoodId,
            request.Side, request.Quantity, request.ExpectedMarketRevision)));
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

            var prior = await ReadReservationAsync(connection, transaction, request.TradeId, forUpdate: true,
                cancellationToken);
            if (prior is not null)
            {
                await transaction.RollbackAsync(cancellationToken);
                return CryptographicOperations.FixedTimeEquals(prior.Digest, digest)
                    ? Response(request, prior.State, prior.UnitPrice, prior.TotalPrice,
                        prior.MarketRevision, prior.SystemRevision)
                    : Response(request, MarketTradeReservationStatusV1.Conflict);
            }

            var systemRevision = await LockSystemRevisionAsync(connection, transaction, request.SystemId,
                cancellationToken);
            if (systemRevision is null)
                return Response(request, MarketTradeReservationStatusV1.Rejected);

            var market = await LockMarketAsync(connection, transaction, request.MarketId, request.GoodId,
                request.SystemId, request.Side, cancellationToken);
            if (market is null || market.MarketRevision != request.ExpectedMarketRevision ||
                market.TargetStock <= 0 || !market.Enabled || market.MarketRevision == long.MaxValue ||
                (request.Side == MarketTradeSideV1.Buy &&
                 market.Stock - market.ReservedBuyStock < request.Quantity) ||
                (request.Side == MarketTradeSideV1.Sell &&
                 request.Quantity > market.MaxStock - market.Stock - market.ReservedSellStock))
                return Response(request, MarketTradeReservationStatusV1.Rejected);

            if (!MarketPriceCalculator.TryCalculateTradeTotal(market.BasePrice, market.TargetStock,
                    market.Stock - market.ReservedBuyStock, request.Side, request.Quantity,
                    out var unitPrice, out var totalPrice, minUnitPrice: market.MinUnitPrice,
                    maxUnitPrice: market.MaxUnitPrice,
                    sellCapacity: market.MaxStock - market.Stock - market.ReservedSellStock))
                return Response(request, MarketTradeReservationStatusV1.Rejected);
            var reservedBuyStock = market.ReservedBuyStock +
                (request.Side == MarketTradeSideV1.Buy ? request.Quantity : 0);
            var reservedSellStock = market.ReservedSellStock +
                (request.Side == MarketTradeSideV1.Sell ? request.Quantity : 0);
            var nextMarketRevision = market.MarketRevision + 1;
            var nextSystemRevision = checked(systemRevision.Value + 1);
            var newPrice = MarketPriceCalculator.Calculate(market.BasePrice, market.TargetStock,
                market.Stock - reservedBuyStock, market.MinUnitPrice, market.MaxUnitPrice);

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    UPDATE market_good_state
                    SET reserved_buy_stock = @reservedBuy, reserved_sell_stock = @reservedSell,
                        current_unit_price = @price, market_revision = @revision
                    WHERE market_id = @market AND good_id = @good AND system_id = @system
                    """;
                command.Parameters.AddWithValue("@reservedBuy", reservedBuyStock);
                command.Parameters.AddWithValue("@reservedSell", reservedSellStock);
                command.Parameters.AddWithValue("@price", newPrice);
                command.Parameters.AddWithValue("@revision", nextMarketRevision);
                command.Parameters.AddWithValue("@market", request.MarketId);
                command.Parameters.AddWithValue("@good", request.GoodId);
                command.Parameters.AddWithValue("@system", request.SystemId);
                if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Locked market row disappeared during reservation.");
            }
            await SetSystemRevisionAsync(connection, transaction, request.SystemId, nextSystemRevision,
                cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    INSERT INTO market_trade_reservations
                        (trade_id, request_sha256, market_id, system_id, good_id, trade_side, quantity,
                         quote_unit_price, total_price, state, market_revision, system_revision)
                    VALUES (@trade, @digest, @market, @system, @good, @side, @quantity,
                            @unit, @total, @state, @marketRevision, @systemRevision)
                    """;
                command.Parameters.AddWithValue("@trade", request.TradeId.ToByteArray());
                command.Parameters.AddWithValue("@digest", digest);
                command.Parameters.AddWithValue("@market", request.MarketId);
                command.Parameters.AddWithValue("@system", request.SystemId);
                command.Parameters.AddWithValue("@good", request.GoodId);
                command.Parameters.AddWithValue("@side", (byte)request.Side);
                command.Parameters.AddWithValue("@quantity", request.Quantity);
                command.Parameters.AddWithValue("@unit", unitPrice);
                command.Parameters.AddWithValue("@total", totalPrice);
                command.Parameters.AddWithValue("@state", (byte)MarketTradeReservationStatusV1.Reserved);
                command.Parameters.AddWithValue("@marketRevision", nextMarketRevision);
                command.Parameters.AddWithValue("@systemRevision", nextSystemRevision);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            return Response(request, MarketTradeReservationStatusV1.Reserved, unitPrice, totalPrice,
                nextMarketRevision, nextSystemRevision);
        }
        catch (MySqlException)
        {
            return Response(request, MarketTradeReservationStatusV1.Unavailable);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(digest);
        }
    }

    public async Task<MarketTradeReservationResponseV1> ResolveAsync(MarketTradeResolutionRequestV1 request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!request.IsValid())
            return Response(request, MarketTradeReservationStatusV1.Rejected);
        if (connectionString is null)
            return Response(request, MarketTradeReservationStatusV1.Unavailable);

        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
            var reservation = await ReadReservationAsync(connection, transaction, request.TradeId, forUpdate: true,
                cancellationToken);
            if (reservation is null)
                return Response(request, MarketTradeReservationStatusV1.Rejected);

            var desiredState = request.Resolution == MarketTradeResolutionV1.Commit
                ? MarketTradeReservationStatusV1.Committed
                : MarketTradeReservationStatusV1.Aborted;
            if (reservation.State != MarketTradeReservationStatusV1.Reserved)
            {
                await transaction.RollbackAsync(cancellationToken);
                return reservation.State == desiredState
                    ? Response(request, reservation.State, reservation.UnitPrice, reservation.TotalPrice,
                        reservation.MarketRevision, reservation.SystemRevision)
                    : Response(request, MarketTradeReservationStatusV1.Conflict);
            }

            var systemRevision = await LockSystemRevisionAsync(connection, transaction, reservation.SystemId,
                cancellationToken);
            if (systemRevision is null || systemRevision == long.MaxValue)
                return Response(request, MarketTradeReservationStatusV1.Unavailable);
            var market = await LockMarketAsync(connection, transaction, reservation.MarketId, reservation.GoodId,
                reservation.SystemId, reservation.Side, cancellationToken);
            if (market is null || market.MarketRevision == long.MaxValue)
                return Response(request, MarketTradeReservationStatusV1.Unavailable);

            var stock = market.Stock;
            var reservedBuyStock = market.ReservedBuyStock;
            var reservedSellStock = market.ReservedSellStock;
            if (reservation.Side == MarketTradeSideV1.Buy)
            {
                if (reservedBuyStock < reservation.Quantity ||
                    request.Resolution == MarketTradeResolutionV1.Commit && stock < reservation.Quantity)
                    return Response(request, MarketTradeReservationStatusV1.Unavailable);
                reservedBuyStock -= reservation.Quantity;
                if (request.Resolution == MarketTradeResolutionV1.Commit)
                    stock -= reservation.Quantity;
            }
            else
            {
                if (reservedSellStock < reservation.Quantity)
                    return Response(request, MarketTradeReservationStatusV1.Unavailable);
                reservedSellStock -= reservation.Quantity;
                if (request.Resolution == MarketTradeResolutionV1.Commit)
                {
                    if (reservation.Quantity > market.MaxStock - stock - reservedSellStock)
                        return Response(request, MarketTradeReservationStatusV1.Unavailable);
                    stock += reservation.Quantity;
                }
            }

            var nextMarketRevision = market.MarketRevision + 1;
            var nextSystemRevision = systemRevision.Value + 1;
            var newPrice = MarketPriceCalculator.Calculate(market.BasePrice, market.TargetStock,
                stock - reservedBuyStock, market.MinUnitPrice, market.MaxUnitPrice);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    UPDATE market_good_state
                    SET available_stock = @stock, reserved_buy_stock = @reservedBuy,
                        reserved_sell_stock = @reservedSell,
                        current_unit_price = @price, market_revision = @revision
                    WHERE market_id = @market AND good_id = @good AND system_id = @system
                    """;
                command.Parameters.AddWithValue("@stock", stock);
                command.Parameters.AddWithValue("@reservedBuy", reservedBuyStock);
                command.Parameters.AddWithValue("@reservedSell", reservedSellStock);
                command.Parameters.AddWithValue("@price", newPrice);
                command.Parameters.AddWithValue("@revision", nextMarketRevision);
                command.Parameters.AddWithValue("@market", reservation.MarketId);
                command.Parameters.AddWithValue("@good", reservation.GoodId);
                command.Parameters.AddWithValue("@system", reservation.SystemId);
                if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Locked market row disappeared during reservation resolution.");
            }
            await SetSystemRevisionAsync(connection, transaction, reservation.SystemId, nextSystemRevision,
                cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    UPDATE market_trade_reservations
                    SET state = @state, market_revision = @marketRevision,
                        system_revision = @systemRevision, updated_utc = UTC_TIMESTAMP(6)
                    WHERE trade_id = @trade AND state = @reservedState
                    """;
                command.Parameters.AddWithValue("@state", (byte)desiredState);
                command.Parameters.AddWithValue("@marketRevision", nextMarketRevision);
                command.Parameters.AddWithValue("@systemRevision", nextSystemRevision);
                command.Parameters.AddWithValue("@trade", request.TradeId.ToByteArray());
                command.Parameters.AddWithValue("@reservedState", (byte)MarketTradeReservationStatusV1.Reserved);
                if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Trade reservation changed while it was being resolved.");
            }

            await transaction.CommitAsync(cancellationToken);
            return Response(request, desiredState, reservation.UnitPrice, reservation.TotalPrice,
                nextMarketRevision, nextSystemRevision);
        }
        catch (MySqlException)
        {
            return Response(request, MarketTradeReservationStatusV1.Unavailable);
        }
    }

    private static async Task<long?> LockSystemRevisionAsync(MySqlConnection connection,
        MySqlTransaction transaction, string systemId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT system_revision FROM market_system_revisions WHERE system_id = @system FOR UPDATE";
        command.Parameters.AddWithValue("@system", systemId);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull ? null : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<MarketRow?> LockMarketAsync(MySqlConnection connection, MySqlTransaction transaction,
        string marketId, string goodId, string systemId, MarketTradeSideV1 side, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT base_unit_price, current_unit_price, available_stock, reserved_buy_stock,
                   reserved_sell_stock, target_stock, max_stock, min_unit_price, max_unit_price,
                   market_revision,
                   CASE WHEN @side = 1 THEN buy_enabled ELSE sell_enabled END
            FROM market_good_state
            WHERE market_id = @market AND good_id = @good AND system_id = @system
            FOR UPDATE
            """;
        command.Parameters.AddWithValue("@side", (byte)side);
        command.Parameters.AddWithValue("@market", marketId);
        command.Parameters.AddWithValue("@good", goodId);
        command.Parameters.AddWithValue("@system", systemId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;
        return new MarketRow(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
            reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6), reader.GetInt64(7), reader.GetInt64(8),
            reader.GetInt64(9), reader.GetBoolean(10));
    }

    private static async Task<StoredReservation?> ReadReservationAsync(MySqlConnection connection,
        MySqlTransaction transaction, Guid tradeId, bool forUpdate, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            SELECT request_sha256, market_id, system_id, good_id, trade_side, quantity,
                   quote_unit_price, total_price, state, market_revision, system_revision
            FROM market_trade_reservations WHERE trade_id = @trade {(forUpdate ? "FOR UPDATE" : "")}
            """;
        command.Parameters.AddWithValue("@trade", tradeId.ToByteArray());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;
        return new StoredReservation((byte[])reader[0], reader.GetString(1), reader.GetString(2), reader.GetString(3),
            (MarketTradeSideV1)reader.GetByte(4), reader.GetInt64(5), reader.GetInt64(6), reader.GetInt64(7),
            (MarketTradeReservationStatusV1)reader.GetByte(8), reader.GetInt64(9), reader.GetInt64(10));
    }

    private static async Task SetSystemRevisionAsync(MySqlConnection connection, MySqlTransaction transaction,
        string systemId, long revision, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "UPDATE market_system_revisions SET system_revision = @revision WHERE system_id = @system";
        command.Parameters.AddWithValue("@revision", revision);
        command.Parameters.AddWithValue("@system", systemId);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new InvalidOperationException("Locked market system revision disappeared during reservation.");
    }

    private static MarketTradeReservationResponseV1 Response(MarketTradeReservationRequestV1 request,
        MarketTradeReservationStatusV1 status, long unitPrice = 0, long totalPrice = 0,
        long marketRevision = 0, long systemRevision = 0) => new()
    {
        CorrelationId = request.CorrelationId,
        TradeId = request.TradeId,
        Status = status,
        QuoteUnitPrice = unitPrice,
        TotalPrice = totalPrice,
        MarketRevision = marketRevision,
        SystemRevision = systemRevision
    };

    private static MarketTradeReservationResponseV1 Response(MarketTradeResolutionRequestV1 request,
        MarketTradeReservationStatusV1 status, long unitPrice = 0, long totalPrice = 0,
        long marketRevision = 0, long systemRevision = 0) => new()
    {
        CorrelationId = request.CorrelationId,
        TradeId = request.TradeId,
        Status = status,
        QuoteUnitPrice = unitPrice,
        TotalPrice = totalPrice,
        MarketRevision = marketRevision,
        SystemRevision = systemRevision
    };

    private sealed record MarketRow(long BasePrice, long CurrentPrice, long Stock, long ReservedBuyStock,
        long ReservedSellStock, long TargetStock, long MaxStock, long MinUnitPrice, long MaxUnitPrice,
        long MarketRevision, bool Enabled);

    private sealed record StoredReservation(byte[] Digest, string MarketId, string SystemId, string GoodId,
        MarketTradeSideV1 Side, long Quantity, long UnitPrice, long TotalPrice,
        MarketTradeReservationStatusV1 State, long MarketRevision, long SystemRevision);

    [MessagePackObject(AllowPrivate = true)]
    internal sealed record ReservationIdentity
    {
        public ReservationIdentity(Guid tradeId, string marketId, string systemId, string goodId,
            MarketTradeSideV1 side, long quantity, long expectedMarketRevision)
        {
            TradeId = tradeId;
            MarketId = marketId;
            SystemId = systemId;
            GoodId = goodId;
            Side = side;
            Quantity = quantity;
            ExpectedMarketRevision = expectedMarketRevision;
        }

        [Key(0)] public Guid TradeId { get; }
        [Key(1)] public string MarketId { get; }
        [Key(2)] public string SystemId { get; }
        [Key(3)] public string GoodId { get; }
        [Key(4)] public MarketTradeSideV1 Side { get; }
        [Key(5)] public long Quantity { get; }
        [Key(6)] public long ExpectedMarketRevision { get; }
    }
}
