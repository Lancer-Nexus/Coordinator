using LancerNexus.Coordinator;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MarketCatalogSeedDocumentTests
{
    [Fact]
    public async Task LoadsShipPackagesAndNoReputationThresholdSentinel()
    {
        var path = Path.Combine(Path.GetTempPath(), "market-seed-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            await File.WriteAllTextAsync(path, MinimalReport);
            var seed = await MarketCatalogSeedDocument.LoadAsync(path);
            Assert.Single(seed.Goods);
            Assert.Null(seed.Goods[0].BaseUnitPrice);
            Assert.Single(seed.Offers);
            Assert.Equal(250L, seed.Offers[0].BaselineUnitPrice);
            Assert.Equal(-1m, seed.Offers[0].ReputationThreshold);
            Assert.True(seed.Offers[0].BuyEnabled);
            Assert.Equal(32, seed.SourceSha256.Length);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task RejectsCoverageErrorsBeforeImport()
    {
        var path = Path.Combine(Path.GetTempPath(), "market-seed-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            await File.WriteAllTextAsync(path, MinimalReport.Replace("\"unknownMarketGoodIds\": []",
                "\"unknownMarketGoodIds\": [\"missing_good\"]", StringComparison.Ordinal));
            await Assert.ThrowsAsync<InvalidDataException>(() => MarketCatalogSeedDocument.LoadAsync(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [MySqlFact]
    public async Task ImportsSeedAtomicallyAndAcceptsOnlyExactRetries()
    {
        var reportPath = Path.Combine(Path.GetTempPath(), "market-seed-" + Guid.NewGuid().ToString("N") + ".json");
        await File.WriteAllTextAsync(reportPath, MinimalReport);
        try
        {
            await WithDatabaseAsync(async connectionString =>
            {
                await ApplyMigrationAsync(connectionString, "006_dynamic_market_projection.sql");
                await ApplyMigrationAsync(connectionString, "007_market_catalog_seed.sql");
                var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
                var importer = new MarketCatalogSeedImporter(connectionString);
                Assert.Equal(MarketCatalogSeedImportResult.Imported, await importer.ImportAsync(seed));
                Assert.Equal(MarketCatalogSeedImportResult.AlreadyImported, await importer.ImportAsync(seed));

                await using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = """
                    SELECT (SELECT COUNT(*) FROM market_catalog_goods),
                           (SELECT COUNT(*) FROM market_system_revisions),
                           (SELECT COUNT(*) FROM market_locations),
                           (SELECT COUNT(*) FROM market_static_offers)
                    """;
                await using var reader = await command.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());
                Assert.Equal([1L, 1L, 1L, 1L],
                    Enumerable.Range(0, 4).Select(index => reader.GetInt64(index)));

                var changedHash = (byte[])seed.SourceSha256.Clone();
                changedHash[0] ^= 0xff;
                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    importer.ImportAsync(seed with { SourceSha256 = changedHash }));
            });
        }
        finally
        {
            File.Delete(reportPath);
        }
    }

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var databaseName = "ln_market_seed_" + Guid.NewGuid().ToString("N");
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
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }

    private const string MinimalReport = """
        {
          "schemaVersion": 1,
          "summary": { "goodDefinitionCount": 1, "nexusSystemCount": 1, "marketEntryCount": 1 },
          "unknownMarketGoodIds": [], "duplicateGoodIds": [], "duplicateMBaseIds": [],
          "marketsMissingMBase": [], "marketsMissingLocalFaction": [], "unpricedShipPackages": [],
          "malformedMarketEntries": [], "unmappedMarketIds": [], "marketsOutsideNexusSystems": [],
          "missingBaseDefinitions": [], "unassignedUniverseSystemIds": [],
          "goods": [{ "goodId": "test_package", "category": "ship", "price": "", "priceDeclared": false }],
          "systems": [{ "systemId": "test_system" }],
          "markets": [{
            "marketId": "test_base", "localFactionId": "test_faction", "systemIds": ["test_system"],
            "entries": [{ "goodId": "test_package", "rank": 0, "reputationThreshold": -1.0,
              "min": 0, "max": 1, "forSale": true, "source": "market_ships.ini",
              "legacyRuntimeUnitPrice": null, "legacyPackagePrice": 250 }]
          }]
        }
        """;
}
