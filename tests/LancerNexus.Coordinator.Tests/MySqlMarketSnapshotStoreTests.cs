using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlMarketSnapshotStoreTests
{
    [Fact]
    public async Task MissingConfigurationAndUnknownSystemsAreUnavailable()
    {
        var request = Request();
        Assert.Equal(MarketSnapshotReadStatusV1.Unavailable,
            (await new MySqlMarketSnapshotStore(null).ReadAsync(request)).Status);
        Assert.True((await new MySqlMarketSnapshotStore(null).ReadAsync(request)).IsValidFor(request));
    }

    [Fact]
    public async Task MarketGoodReadWithoutConfigurationIsUnavailable()
    {
        var request = new MarketGoodReadRequestV1
        {
            CorrelationId = Guid.NewGuid(), SystemId = "li-01", MarketId = "li01_base",
            GoodId = "commodity_food"
        };
        var response = await new MySqlMarketGoodReadStore(null).ReadAsync(request);
        Assert.Equal(MarketGoodReadStatusV1.Unavailable, response.Status);
        Assert.True(response.IsValidFor(request));
    }

    [MySqlFact]
    public async Task MarketGoodReadReturnsExactRowAndReportsRevisionChanges()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "INSERT INTO market_system_revisions (system_id, system_revision) VALUES ('li-01', 7)";
                await command.ExecuteNonQueryAsync();
                command.CommandText = """
                    INSERT INTO market_good_state (market_id, good_id, system_id, base_unit_price,
                        current_unit_price, available_stock, target_stock, buy_enabled, sell_enabled, market_revision)
                    VALUES ('li01_base', 'commodity_food', 'li-01', 100, 110, 6, 10, TRUE, FALSE, 3)
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlMarketGoodReadStore(connectionString);
            var request = new MarketGoodReadRequestV1
            {
                CorrelationId = Guid.NewGuid(), SystemId = "li-01", MarketId = "li01_base",
                GoodId = "commodity_food", ExpectedSystemRevision = 7, ExpectedMarketRevision = 3
            };
            var found = await store.ReadAsync(request);
            Assert.True(found.IsValidFor(request));
            Assert.Equal(MarketGoodReadStatusV1.Found, found.Status);
            Assert.Equal(6, found.Good!.AvailableStock);
            Assert.False(found.Good.SellEnabled);

            var stale = request with { CorrelationId = Guid.NewGuid(), ExpectedMarketRevision = 4 };
            var mismatch = await store.ReadAsync(stale);
            Assert.True(mismatch.IsValidFor(stale));
            Assert.Equal(MarketGoodReadStatusV1.RevisionMismatch, mismatch.Status);
            Assert.Equal(3, mismatch.MarketRevision);

            var missing = request with { CorrelationId = Guid.NewGuid(), GoodId = "commodity_missing" };
            var notFound = await store.ReadAsync(missing);
            Assert.True(notFound.IsValidFor(missing));
            Assert.Equal(MarketGoodReadStatusV1.NotFound, notFound.Status);
        });
    }

    [MySqlFact]
    public async Task ReadsStableKeysetPagesAndExpiresOldSystemRevision()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "INSERT INTO market_system_revisions (system_id, system_revision) VALUES ('li-01', 7)";
                await command.ExecuteNonQueryAsync();
                command.CommandText = """
                    INSERT INTO market_good_state (market_id, good_id, system_id, base_unit_price,
                        current_unit_price, available_stock, target_stock, buy_enabled, sell_enabled, market_revision)
                    VALUES ('br01_01_base', 'commodity_a', 'li-01', 100, 110, 3, 10, TRUE, TRUE, 7),
                           ('br01_01_base', 'commodity_b', 'li-01', 200, 220, 4, 10, TRUE, FALSE, 7),
                           ('li01_01_base', 'commodity_a', 'li-01', 150, 150, 8, 10, TRUE, TRUE, 7)
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlMarketSnapshotStore(connectionString);
            var firstRequest = Request() with { PageSize = 2 };
            var first = await store.ReadAsync(firstRequest);
            Assert.True(first.IsValidFor(firstRequest));
            Assert.Equal(7, first.SystemRevision);
            Assert.Equal(["br01_01_base/commodity_a", "br01_01_base/commodity_b"],
                first.Goods.Select(row => $"{row.MarketId}/{row.GoodId}"));
            Assert.Equal("br01_01_base", first.NextMarketId);
            Assert.Equal("commodity_b", first.NextGoodId);

            var nextRequest = firstRequest with
            {
                CorrelationId = Guid.NewGuid(),
                ExpectedSystemRevision = first.SystemRevision,
                AfterMarketId = first.NextMarketId,
                AfterGoodId = first.NextGoodId
            };
            var next = await store.ReadAsync(nextRequest);
            Assert.True(next.IsValidFor(nextRequest));
            Assert.Single(next.Goods);
            Assert.Equal("li01_01_base", next.Goods[0].MarketId);
            Assert.Null(next.NextMarketId);

            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "UPDATE market_system_revisions SET system_revision = 8 WHERE system_id = 'li-01'";
                await command.ExecuteNonQueryAsync();
            }
            var expired = await store.ReadAsync(nextRequest);
            Assert.Equal(MarketSnapshotReadStatusV1.RevisionExpired, expired.Status);
            Assert.True(expired.IsValidFor(nextRequest));
        });
    }

    [MySqlFact]
    public async Task ReadsCompleteCatalogIncludingGoodsWithoutOffersAndPinsSeedRevision()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            var seedHash = Enumerable.Range(0, 32).Select(value => (byte)value).ToArray();
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "UPDATE market_catalog_seed SET seed_sha256 = @hash, good_count = 3 WHERE seed_slot = 1";
                command.Parameters.AddWithValue("@hash", seedHash);
                await command.ExecuteNonQueryAsync();
                command.CommandText = "INSERT INTO market_catalog_goods (good_id, category, base_unit_price) VALUES ('commodity_a', 'commodity', 10), ('commodity_b', 'commodity', NULL), ('ship_fighter', 'ship', 1000)";
                await command.ExecuteNonQueryAsync();
                // Only one good has an offer; the public catalog must still return all three goods.
                command.CommandText = "INSERT INTO market_system_revisions (system_id, system_revision) VALUES ('li-01', 1)";
                await command.ExecuteNonQueryAsync();
                command.CommandText = "INSERT INTO market_locations (market_id, system_id, local_faction_id) VALUES ('li01_base', 'li-01', 'li_grp')";
                await command.ExecuteNonQueryAsync();
                command.CommandText = "INSERT INTO market_static_offers (market_id, good_id, baseline_unit_price, buy_enabled, rank_requirement, min_amount, max_amount, reputation_threshold, source_file) VALUES ('li01_base', 'commodity_a', 10, TRUE, 0, 0, 100, 0, 'market.ini')";
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlMarketCatalogReadStore(connectionString);
            var firstRequest = new MarketCatalogReadRequestV1 { CorrelationId = Guid.NewGuid(), PageSize = 2 };
            var first = await store.ReadAsync(firstRequest);
            Assert.True(first.IsValidFor(firstRequest));
            Assert.Equal(Convert.ToHexString(seedHash).ToLowerInvariant(), first.CatalogRevision);
            Assert.Equal(["commodity_a", "commodity_b"], first.Goods.Select(good => good.GoodId));
            Assert.Null(first.Goods[1].BaseUnitPrice);
            Assert.Equal("commodity_b", first.NextGoodId);

            var nextRequest = firstRequest with
            {
                CorrelationId = Guid.NewGuid(),
                AfterGoodId = first.NextGoodId,
                ExpectedCatalogRevision = first.CatalogRevision
            };
            var next = await store.ReadAsync(nextRequest);
            Assert.True(next.IsValidFor(nextRequest));
            Assert.Equal("ship_fighter", Assert.Single(next.Goods).GoodId);
            Assert.Null(next.NextGoodId);

            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "UPDATE market_catalog_seed SET seed_sha256 = @hash WHERE seed_slot = 1";
                command.Parameters.AddWithValue("@hash", Enumerable.Repeat((byte)255, 32).ToArray());
                await command.ExecuteNonQueryAsync();
            }
            var expired = await store.ReadAsync(nextRequest);
            Assert.Equal(MarketCatalogReadStatusV1.RevisionExpired, expired.Status);
            Assert.True(expired.IsValidFor(nextRequest));
        });
    }

    private static MarketSnapshotReadRequestV1 Request() => new()
    {
        CorrelationId = Guid.NewGuid(),
        SystemId = "li-01"
    };

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var databaseName = "ln_market_" + Guid.NewGuid().ToString("N");
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

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }
}
