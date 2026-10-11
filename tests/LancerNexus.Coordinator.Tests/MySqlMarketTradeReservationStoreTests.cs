using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlMarketTradeReservationStoreTests
{
    [MySqlFact]
    public async Task BuyReservationHoldsStockAndCommitReplayDoesNotApplyTwice()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeReservationStore(connectionString);
            var request = Reserve(MarketTradeSideV1.Buy, quantity: 5);

            var reserved = await store.ReserveAsync(request);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, reserved.Status);
            Assert.Equal(100, reserved.QuoteUnitPrice);
            Assert.Equal(500, reserved.TotalPrice);
            Assert.Equal(2, reserved.MarketRevision);
            Assert.Equal(2, reserved.SystemRevision);

            var snapshotRequest = new MarketSnapshotReadRequestV1
            {
                CorrelationId = Guid.NewGuid(),
                SystemId = "test_system"
            };
            var snapshot = await new MySqlMarketSnapshotStore(connectionString).ReadAsync(snapshotRequest);
            Assert.True(snapshot.IsValidFor(snapshotRequest));
            Assert.Equal(15, Assert.Single(snapshot.Goods).AvailableStock);
            Assert.Equal(133, snapshot.Goods[0].CurrentUnitPrice);

            var repeated = await store.ReserveAsync(request with { CorrelationId = Guid.NewGuid() });
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, repeated.Status);
            Assert.Equal(2, repeated.MarketRevision);
            Assert.NotEqual(request.CorrelationId, repeated.CorrelationId);
            Assert.Equal(MarketTradeReservationStatusV1.Conflict,
                (await store.ReserveAsync(request with { Quantity = 6 })).Status);

            var commit = Resolve(request.TradeId, MarketTradeResolutionV1.Commit);
            var committed = await store.ResolveAsync(commit);
            Assert.Equal(MarketTradeReservationStatusV1.Committed, committed.Status);
            Assert.Equal(3, committed.MarketRevision);
            var replayedCommit = await store.ResolveAsync(commit with { CorrelationId = Guid.NewGuid() });
            Assert.Equal(MarketTradeReservationStatusV1.Committed, replayedCommit.Status);
            Assert.Equal(committed.MarketRevision, replayedCommit.MarketRevision);
            Assert.Equal(MarketTradeReservationStatusV1.Conflict,
                (await store.ResolveAsync(Resolve(request.TradeId, MarketTradeResolutionV1.Abort))).Status);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT available_stock, reserved_buy_stock, current_unit_price, market_revision
                FROM market_good_state WHERE market_id = 'test_base' AND good_id = 'commodity_food'
                """;
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(15, reader.GetInt64(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(133, reader.GetInt64(2));
            Assert.Equal(3, reader.GetInt64(3));
        });
    }

    [MySqlFact]
    public async Task AbortReleasesReservedAvailabilityAndIsIdempotent()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeReservationStore(connectionString);
            var request = Reserve(MarketTradeSideV1.Buy, quantity: 5);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, (await store.ReserveAsync(request)).Status);

            var abort = Resolve(request.TradeId, MarketTradeResolutionV1.Abort);
            var aborted = await store.ResolveAsync(abort);
            Assert.Equal(MarketTradeReservationStatusV1.Aborted, aborted.Status);
            Assert.Equal(3, aborted.MarketRevision);
            var replayed = await store.ResolveAsync(abort with { CorrelationId = Guid.NewGuid() });
            Assert.Equal(MarketTradeReservationStatusV1.Aborted, replayed.Status);
            Assert.Equal(aborted.MarketRevision, replayed.MarketRevision);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT available_stock, reserved_buy_stock, current_unit_price FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(20, reader.GetInt64(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(100, reader.GetInt64(2));
        });
    }

    [MySqlFact]
    public async Task ConcurrentReservationsAgainstOneRevisionCannotOverbookStock()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeReservationStore(connectionString);
            var first = Reserve(MarketTradeSideV1.Buy, quantity: 12);
            var second = Reserve(MarketTradeSideV1.Buy, quantity: 12);
            var results = await Task.WhenAll(store.ReserveAsync(first), store.ReserveAsync(second));
            Assert.Single(results, result => result.Status == MarketTradeReservationStatusV1.Reserved);
            Assert.Single(results, result => result.Status == MarketTradeReservationStatusV1.Rejected);

            var competingTrade = new MarketTradeEventV1
            {
                EventId = Guid.NewGuid(),
                InstanceId = "instance_a",
                ProducerEpoch = Guid.NewGuid(),
                Sequence = 1,
                OccurredUtc = DateTimeOffset.UtcNow,
                MarketId = "test_base",
                SystemId = "test_system",
                GoodId = "commodity_food",
                Side = MarketTradeSideV1.Buy,
                Quantity = 9
            };
            Assert.Equal(MarketTradeIngestStatusV1.Rejected,
                (await new MySqlMarketTradeStore(connectionString).ApplyAsync(competingTrade)).Status);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT available_stock, reserved_buy_stock FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(20, reader.GetInt64(0));
            Assert.Equal(12, reader.GetInt64(1));
        });
    }

    [MySqlFact]
    public async Task SaleReservationAddsStockOnlyOnCommit()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "UPDATE market_good_state SET sell_enabled = TRUE WHERE market_id='test_base'";
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlMarketTradeReservationStore(connectionString);
            var request = Reserve(MarketTradeSideV1.Sell, quantity: 5);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, (await store.ReserveAsync(request)).Status);
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT available_stock FROM market_good_state WHERE market_id='test_base'";
                Assert.Equal(20L, Convert.ToInt64(await command.ExecuteScalarAsync()));
            }

            Assert.Equal(MarketTradeReservationStatusV1.Committed,
                (await store.ResolveAsync(Resolve(request.TradeId, MarketTradeResolutionV1.Commit))).Status);
            await using var verify = new MySqlConnection(connectionString);
            await verify.OpenAsync();
            await using var query = verify.CreateCommand();
            query.CommandText = "SELECT available_stock, current_unit_price FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await query.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(25, reader.GetInt64(0));
            Assert.Equal(80, reader.GetInt64(1));
        });
    }

    private static MarketTradeReservationRequestV1 Reserve(MarketTradeSideV1 side, long quantity) => new()
    {
        CorrelationId = Guid.NewGuid(),
        TradeId = Guid.NewGuid(),
        MarketId = "test_base",
        SystemId = "test_system",
        GoodId = "commodity_food",
        Side = side,
        Quantity = quantity,
        ExpectedMarketRevision = 1
    };

    private static MarketTradeResolutionRequestV1 Resolve(Guid tradeId, MarketTradeResolutionV1 resolution) => new()
    {
        CorrelationId = Guid.NewGuid(),
        TradeId = tradeId,
        Resolution = resolution
    };

    private static async Task SeedMarketAsync(string connectionString)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        var statements = new[]
        {
            "INSERT INTO market_system_revisions (system_id, system_revision) VALUES ('test_system', 1)",
            "INSERT INTO market_catalog_goods (good_id, category, base_unit_price) VALUES ('commodity_food', 'commodity', 100)",
            "INSERT INTO market_locations (market_id, system_id, local_faction_id) VALUES ('test_base', 'test_system', 'test_faction')",
            "INSERT INTO market_static_offers (market_id, good_id, baseline_unit_price, buy_enabled, rank_requirement, min_amount, max_amount, reputation_threshold, source_file) VALUES ('test_base', 'commodity_food', 100, TRUE, 0, 0, 1, -1, 'test.ini')",
            "INSERT INTO market_good_state (market_id, good_id, system_id, base_unit_price, current_unit_price, available_stock, target_stock, buy_enabled, sell_enabled, market_revision) VALUES ('test_base', 'commodity_food', 'test_system', 100, 100, 20, 20, TRUE, FALSE, 1)"
        };
        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task ApplySchemaAsync(string connectionString)
    {
        foreach (var filename in new[]
                 {
                     "006_dynamic_market_projection.sql",
                     "007_market_catalog_seed.sql",
                     "008_market_trade_events.sql",
                     "009_market_trade_reservations.sql"
                 })
        {
            var path = Path.Combine(AppContext.BaseDirectory, "db", "migrations", filename);
            var statements = (await File.ReadAllTextAsync(path))
                .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            foreach (var statement in statements)
            {
                await using var command = connection.CreateCommand();
                command.CommandText = statement;
                await command.ExecuteNonQueryAsync();
            }
        }
    }

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var databaseName = "ln_market_reserve_" + Guid.NewGuid().ToString("N");
        var admin = new MySqlConnectionStringBuilder(configured) { Database = "information_schema" };
        await using (var connection = new MySqlConnection(admin.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
            await command.ExecuteNonQueryAsync();
        }
        var isolated = new MySqlConnectionStringBuilder(configured) { Database = databaseName };
        try { await action(isolated.ConnectionString); }
        finally
        {
            await using var connection = new MySqlConnection(admin.ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP DATABASE `{databaseName}`";
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }
}
