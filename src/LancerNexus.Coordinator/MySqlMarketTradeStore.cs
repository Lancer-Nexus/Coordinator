using System.Data;
using System.Security.Cryptography;
using LancerNexus.Protocol;
using MessagePack;
using MySqlConnector;

namespace LancerNexus.Coordinator;

/// <summary>Applies confirmed trade effects and their replay fence in one shared-MySQL transaction.</summary>
public sealed class MySqlMarketTradeStore(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;
    public bool IsEnabled => connectionString is not null;

    public async Task<MarketTradeEventAcknowledgementV1> ApplyAsync(MarketTradeEventV1 trade,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(trade);
        if (!trade.IsValid())
            return Acknowledgement(trade, MarketTradeIngestStatusV1.Rejected);
        if (connectionString is null)
            return Acknowledgement(trade, MarketTradeIngestStatusV1.Unavailable);

        var payload = MessagePackSerializer.Serialize(trade);
        var digest = SHA256.HashData(payload);
        try
        {
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

            var prior = await ReadPriorEventAsync(connection, transaction, trade, cancellationToken);
            if (prior.Found)
            {
                await transaction.RollbackAsync(cancellationToken);
                return prior.Digest is not null && CryptographicOperations.FixedTimeEquals(prior.Digest, digest)
                    ? Acknowledgement(trade, MarketTradeIngestStatusV1.Duplicate, prior.MarketRevision)
                    : Acknowledgement(trade, MarketTradeIngestStatusV1.Conflict);
            }

            // Serialize all market mutations within one system and pin the parent revision row.
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "SELECT system_revision FROM market_system_revisions WHERE system_id = @system FOR UPDATE";
                command.Parameters.AddWithValue("@system", trade.SystemId);
                if (await command.ExecuteScalarAsync(cancellationToken) is null)
                    return Acknowledgement(trade, MarketTradeIngestStatusV1.Rejected);
            }

            long basePrice;
            long stock;
            long reservedBuyStock;
            long reservedSellStock;
            long targetStock;
            long maxStock;
            long minUnitPrice;
            long maxUnitPrice;
            long marketRevision;
            bool enabled;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    SELECT base_unit_price, available_stock, reserved_buy_stock, reserved_sell_stock,
                           target_stock, max_stock, min_unit_price, max_unit_price, market_revision,
                           CASE WHEN @side = 1 THEN buy_enabled ELSE sell_enabled END
                    FROM market_good_state
                    WHERE market_id = @market AND good_id = @good AND system_id = @system
                    FOR UPDATE
                    """;
                command.Parameters.AddWithValue("@side", (byte)trade.Side);
                command.Parameters.AddWithValue("@market", trade.MarketId);
                command.Parameters.AddWithValue("@good", trade.GoodId);
                command.Parameters.AddWithValue("@system", trade.SystemId);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                    return Acknowledgement(trade, MarketTradeIngestStatusV1.Rejected);
                basePrice = reader.GetInt64(0);
                stock = reader.GetInt64(1);
                reservedBuyStock = reader.GetInt64(2);
                reservedSellStock = reader.GetInt64(3);
                targetStock = reader.GetInt64(4);
                maxStock = reader.GetInt64(5);
                minUnitPrice = reader.GetInt64(6);
                maxUnitPrice = reader.GetInt64(7);
                marketRevision = reader.GetInt64(8);
                enabled = reader.GetBoolean(9);
            }

            if (!enabled || targetStock <= 0 ||
                (trade.Side == MarketTradeSideV1.Buy && stock - reservedBuyStock < trade.Quantity) ||
                (trade.Side == MarketTradeSideV1.Sell &&
                 trade.Quantity > maxStock - stock - reservedSellStock) ||
                marketRevision == long.MaxValue)
                return Acknowledgement(trade, MarketTradeIngestStatusV1.Rejected);

            // INSERT IGNORE makes a concurrent retry converge on the first durable result.
            int inserted;
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    INSERT IGNORE INTO market_trade_events
                        (event_id, instance_id, producer_epoch, sequence_number, system_id,
                         market_id, good_id, payload_sha256, resulting_market_revision)
                    VALUES (@event, @instance, @epoch, @sequence, @system,
                            @market, @good, @digest, 1)
                    """;
                command.Parameters.AddWithValue("@event", trade.EventId.ToByteArray());
                command.Parameters.AddWithValue("@instance", trade.InstanceId);
                command.Parameters.AddWithValue("@epoch", trade.ProducerEpoch.ToByteArray());
                command.Parameters.AddWithValue("@sequence", trade.Sequence);
                command.Parameters.AddWithValue("@system", trade.SystemId);
                command.Parameters.AddWithValue("@market", trade.MarketId);
                command.Parameters.AddWithValue("@good", trade.GoodId);
                command.Parameters.AddWithValue("@digest", digest);
                inserted = await command.ExecuteNonQueryAsync(cancellationToken);
            }

            if (inserted == 0)
            {
                prior = await ReadPriorEventAsync(connection, transaction, trade, cancellationToken);
                await transaction.RollbackAsync(cancellationToken);
                return prior.Found && prior.Digest is not null && CryptographicOperations.FixedTimeEquals(prior.Digest, digest)
                    ? Acknowledgement(trade, MarketTradeIngestStatusV1.Duplicate, prior.MarketRevision)
                    : Acknowledgement(trade, MarketTradeIngestStatusV1.Conflict);
            }

            var newStock = trade.Side == MarketTradeSideV1.Buy
                ? stock - trade.Quantity
                : stock + trade.Quantity;
            var newMarketRevision = marketRevision + 1;
            var newPrice = MarketPriceCalculator.Calculate(basePrice, targetStock, newStock - reservedBuyStock,
                minUnitPrice, maxUnitPrice);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    UPDATE market_good_state
                    SET available_stock = @stock, current_unit_price = @price, market_revision = @revision
                    WHERE market_id = @market AND good_id = @good AND system_id = @system
                    """;
                command.Parameters.AddWithValue("@stock", newStock);
                command.Parameters.AddWithValue("@price", newPrice);
                command.Parameters.AddWithValue("@revision", newMarketRevision);
                command.Parameters.AddWithValue("@market", trade.MarketId);
                command.Parameters.AddWithValue("@good", trade.GoodId);
                command.Parameters.AddWithValue("@system", trade.SystemId);
                if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Locked market row disappeared during trade application.");
            }
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "UPDATE market_system_revisions SET system_revision = system_revision + 1 WHERE system_id = @system";
                command.Parameters.AddWithValue("@system", trade.SystemId);
                if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Locked market system revision disappeared during trade application.");
            }
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "UPDATE market_trade_events SET resulting_market_revision = @revision WHERE event_id = @event";
                command.Parameters.AddWithValue("@revision", newMarketRevision);
                command.Parameters.AddWithValue("@event", trade.EventId.ToByteArray());
                if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                    throw new InvalidOperationException("Market trade idempotency record disappeared during application.");
            }
            await transaction.CommitAsync(cancellationToken);
            return Acknowledgement(trade, MarketTradeIngestStatusV1.Applied, newMarketRevision);
        }
        catch (MySqlException)
        {
            return Acknowledgement(trade, MarketTradeIngestStatusV1.Unavailable);
        }
    }

    private static async Task<(bool Found, byte[]? Digest, long MarketRevision)> ReadPriorEventAsync(
        MySqlConnection connection, MySqlTransaction transaction, MarketTradeEventV1 trade,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT payload_sha256, resulting_market_revision FROM market_trade_events
            WHERE event_id = @event OR
                  (instance_id = @instance AND producer_epoch = @epoch AND sequence_number = @sequence)
            LIMIT 1 FOR UPDATE
            """;
        command.Parameters.AddWithValue("@event", trade.EventId.ToByteArray());
        command.Parameters.AddWithValue("@instance", trade.InstanceId);
        command.Parameters.AddWithValue("@epoch", trade.ProducerEpoch.ToByteArray());
        command.Parameters.AddWithValue("@sequence", trade.Sequence);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? (true, (byte[])reader[0], reader.GetInt64(1))
            : (false, null, 0);
    }

    private static MarketTradeEventAcknowledgementV1 Acknowledgement(MarketTradeEventV1 trade,
        MarketTradeIngestStatusV1 status, long marketRevision = 0) => new()
    {
        EventId = trade.EventId,
        InstanceId = trade.InstanceId,
        ProducerEpoch = trade.ProducerEpoch,
        Sequence = trade.Sequence,
        Status = status,
        MarketRevision = marketRevision
    };
}
