using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlMarketTradeStoreTests
{
    [Fact]
    public async Task MissingConfigurationAndInvalidEventsFailClosed()
    {
        var trade = Trade();
        Assert.Equal(MarketTradeIngestStatusV1.Unavailable,
            (await new MySqlMarketTradeStore(null).ApplyAsync(trade)).Status);
        Assert.Equal(MarketTradeIngestStatusV1.Rejected,
            (await new MySqlMarketTradeStore("configured").ApplyAsync(trade with { Quantity = 0 })).Status);
    }

    [MySqlFact]
    public async Task AppliesTradeAtomicallyDeduplicatesRetriesAndRejectsIdentityConflicts()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var trade = Trade();
            var store = new MySqlMarketTradeStore(connectionString);

            var concurrent = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => store.ApplyAsync(trade)));
            Assert.Single(concurrent, result => result.Status == MarketTradeIngestStatusV1.Applied);
            Assert.Equal(7, concurrent.Count(result => result.Status == MarketTradeIngestStatusV1.Duplicate));
            Assert.All(concurrent, result => Assert.True(result.Matches(trade)));
            Assert.All(concurrent, result => Assert.Equal(2, result.MarketRevision));

            Assert.Equal(MarketTradeIngestStatusV1.Conflict,
                (await store.ApplyAsync(trade with { Quantity = 6 })).Status);
            Assert.Equal(MarketTradeIngestStatusV1.Conflict,
                (await store.ApplyAsync(trade with { EventId = Guid.NewGuid() })).Status);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT s.available_stock, s.current_unit_price, s.market_revision, r.system_revision,
                       (SELECT COUNT(*) FROM market_trade_events)
                FROM market_good_state s JOIN market_system_revisions r ON r.system_id = s.system_id
                WHERE s.market_id = 'test_base' AND s.good_id = 'commodity_food'
                """;
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(15, reader.GetInt64(0));
            Assert.Equal(133, reader.GetInt64(1));
            Assert.Equal(2, reader.GetInt64(2));
            Assert.Equal(2, reader.GetInt64(3));
            Assert.Equal(1, reader.GetInt64(4));
        });
    }

    [MySqlFact]
    public async Task RejectsDisabledOffersAndPurchasesLargerThanStockWithoutWritingEvents()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeStore(connectionString);
            Assert.Equal(MarketTradeIngestStatusV1.Rejected,
                (await store.ApplyAsync(Trade() with { Quantity = 21 })).Status);
            Assert.Equal(MarketTradeIngestStatusV1.Rejected,
                (await store.ApplyAsync(Trade() with { Side = MarketTradeSideV1.Sell })).Status);
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT (SELECT COUNT(*) FROM market_trade_events), available_stock FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0, reader.GetInt64(0));
            Assert.Equal(20, reader.GetInt64(1));
        });
    }

    [MySqlFact]
    public async Task RetryAfterAValidPurchaseDepletedTheMarketReturnsDuplicate()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeStore(connectionString);
            var trade = Trade() with { Quantity = 20 };
            Assert.Equal(MarketTradeIngestStatusV1.Applied, (await store.ApplyAsync(trade)).Status);
            var retry = await store.ApplyAsync(trade);
            Assert.Equal(MarketTradeIngestStatusV1.Duplicate, retry.Status);
            Assert.Equal(2, retry.MarketRevision);
            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT available_stock, current_unit_price, market_revision FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0, reader.GetInt64(0));
            Assert.Equal(400, reader.GetInt64(1));
            Assert.Equal(2, reader.GetInt64(2));
        });
    }

    [MySqlFact]
    public async Task ConfirmedSaleAddsStockAndLowersTheSharedMarketPrice()
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
            var trade = Trade() with { Side = MarketTradeSideV1.Sell, Quantity = 5 };
            var result = await new MySqlMarketTradeStore(connectionString).ApplyAsync(trade);
            Assert.Equal(MarketTradeIngestStatusV1.Applied, result.Status);
            Assert.Equal(2, result.MarketRevision);
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

    private static async Task SeedMarketAsync(string connectionString)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO market_system_revisions (system_id, system_revision) VALUES ('test_system', 1);
            INSERT INTO market_catalog_goods (good_id, category, base_unit_price) VALUES ('commodity_food', 'commodity', 100);
            INSERT INTO market_locations (market_id, system_id, local_faction_id) VALUES ('test_base', 'test_system', 'test_faction');
            INSERT INTO market_static_offers (market_id, good_id, baseline_unit_price, buy_enabled, rank_requirement, min_amount, max_amount, reputation_threshold, source_file)
            VALUES ('test_base', 'commodity_food', 100, TRUE, 0, 0, 1, -1, 'test.ini');
            INSERT INTO market_good_state (market_id, good_id, system_id, base_unit_price, current_unit_price, available_stock, target_stock, buy_enabled, sell_enabled, market_revision)
            VALUES ('test_base', 'commodity_food', 'test_system', 100, 100, 20, 20, TRUE, FALSE, 1)
            """;
        var statements = command.CommandText.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var statement in statements)
        {
            await using var seedCommand = connection.CreateCommand();
            seedCommand.CommandText = statement;
            await seedCommand.ExecuteNonQueryAsync();
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
        var databaseName = "ln_market_trade_" + Guid.NewGuid().ToString("N");
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

    private static MarketTradeEventV1 Trade() => new()
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
        Quantity = 5
    };

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }
}
