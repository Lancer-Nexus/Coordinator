using LancerNexus.Coordinator;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MarketStockPolicyImporterTests
{
    [MySqlFact]
    public async Task InstallsPolicyAndLiveStateAtomicallyAndAcceptsOnlyExactRetry()
    {
        var prefix = Path.Combine(Path.GetTempPath(), "market-policy-import-" + Guid.NewGuid().ToString("N"));
        var reportPath = prefix + ".report.json";
        var policyPath = prefix + ".policy.json";
        await File.WriteAllTextAsync(reportPath, MinimalReport);
        try
        {
            await WithDatabaseAsync(async connectionString =>
            {
                foreach (var migration in new[]
                         {
                             "006_dynamic_market_projection.sql", "007_market_catalog_seed.sql",
                             "008_market_trade_events.sql", "009_market_trade_reservations.sql",
                             "010_market_stock_policy.sql", "011_market_quote_bound_reservations.sql"
                         })
                    await ApplyMigrationAsync(connectionString, migration);

                var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
                await new MarketCatalogSeedImporter(connectionString).ImportAsync(seed);
                await File.WriteAllTextAsync(policyPath, Policy(seed));
                var policy = await MarketStockPolicyDocument.LoadAsync(policyPath, seed);
                var importer = new MarketStockPolicyImporter(connectionString);

                Assert.Equal(MarketStockPolicyImportResult.Imported, await importer.ImportAsync(seed, policy));
                Assert.Equal(MarketStockPolicyImportResult.AlreadyImported, await importer.ImportAsync(seed, policy));

                await using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = """
                    SELECT state.base_unit_price, state.current_unit_price, state.available_stock,
                           state.reserved_buy_stock, state.reserved_sell_stock, state.target_stock,
                           state.max_stock, state.min_unit_price, state.max_unit_price,
                           state.buy_enabled, state.sell_enabled, state.market_revision,
                           state.policy_revision, state.market_faction_override_id,
                           revision.system_revision, installation.policy_sha256,
                           (SELECT COUNT(*) FROM market_stock_policy_entries)
                    FROM market_good_state AS state
                    INNER JOIN market_system_revisions AS revision ON revision.system_id = state.system_id
                    INNER JOIN market_stock_policy_installation AS installation ON installation.policy_slot = 1
                    WHERE state.market_id = 'li01_base' AND state.good_id = 'commodity_food'
                    """;
                await using var reader = await command.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal(250, reader.GetInt64(0));
                Assert.Equal(500, reader.GetInt64(1));
                Assert.Equal(100, reader.GetInt64(2));
                Assert.Equal(0, reader.GetInt64(3));
                Assert.Equal(0, reader.GetInt64(4));
                Assert.Equal(200, reader.GetInt64(5));
                Assert.Equal(500, reader.GetInt64(6));
                Assert.Equal(50, reader.GetInt64(7));
                Assert.Equal(1000, reader.GetInt64(8));
                Assert.True(reader.GetBoolean(9));
                Assert.True(reader.GetBoolean(10));
                Assert.Equal(1, reader.GetInt64(11));
                Assert.Equal(1, reader.GetInt64(12));
                Assert.Equal("li_grp", reader.GetString(13));
                Assert.Equal(2, reader.GetInt64(14));
                Assert.Equal(policy.PolicySha256.ToArray(), (byte[])reader[15]);
                Assert.Equal(1, reader.GetInt32(16));

                await File.AppendAllTextAsync(policyPath, "\n");
                var changedPolicy = await MarketStockPolicyDocument.LoadAsync(policyPath, seed);
                await Assert.ThrowsAsync<InvalidOperationException>(() => importer.ImportAsync(seed, changedPolicy));
            });
        }
        finally
        {
            File.Delete(reportPath);
            File.Delete(policyPath);
        }
    }

    private static string Policy(MarketCatalogSeedDocument seed) => $$"""
        {
          "schemaVersion": 1,
          "policyVersion": "dynamic_market_stock_v1",
          "policyRevision": 1,
          "catalogSeedSha256": "{{Convert.ToHexString(seed.SourceSha256)}}",
          "entries": [{"marketId":"li01_base","goodId":"commodity_food","baseUnitPrice":250,
            "buyEnabled":true,"sellEnabled":true,"initialStock":100,"targetStock":200,
            "maxStock":500,"minUnitPrice":50,"maxUnitPrice":1000,"pricingPolicyVersion":1,
            "marketFactionOverrideId":"li_grp"}]
        }
        """;

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var databaseName = "ln_market_policy_" + Guid.NewGuid().ToString("N");
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

    private static async Task ApplyMigrationAsync(string connectionString, string filename)
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

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to run isolated MariaDB market policy integration tests.";
        }
    }

    private const string MinimalReport = """
        {
          "schemaVersion": 1,
          "summary": {"goodDefinitionCount": 1, "nexusSystemCount": 1, "marketEntryCount": 1},
          "unknownMarketGoodIds": [], "duplicateGoodIds": [], "duplicateMBaseIds": [],
          "marketsMissingMBase": [], "marketsMissingLocalFaction": [], "unpricedShipPackages": [],
          "malformedMarketEntries": [], "unmappedMarketIds": [], "marketsOutsideNexusSystems": [],
          "missingBaseDefinitions": [], "unassignedUniverseSystemIds": [],
          "goods": [{"goodId":"commodity_food", "category":"commodity", "price":"250", "priceDeclared":true}],
          "systems": [{"systemId":"li-01"}],
          "markets": [{"marketId":"li01_base", "localFactionId":"li_grp", "systemIds":["li-01"],
            "entries": [{"goodId":"commodity_food","forSale": true,"rank":0,"min":1,"max":1,
              "reputationThreshold":-1,"source":"market.ini","multiplierRaw":"1","legacyRuntimeUnitPrice":250}]}]
        }
        """;
}
