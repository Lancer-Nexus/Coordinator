using LancerNexus.Coordinator;
using System.Text.Json.Nodes;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MarketStockPolicyDocumentTests
{
    [Fact]
    public async Task LoadsReportPinnedAllowlistEntry()
    {
        await WithDocumentsAsync(async (reportPath, policyPath) =>
        {
            var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
            await File.WriteAllTextAsync(policyPath, Policy(seed));

            var policy = await MarketStockPolicyDocument.LoadAsync(policyPath, seed);

            var entry = Assert.Single(policy.Entries);
            Assert.Equal("li01_base", entry.MarketId);
            Assert.Equal("commodity_food", entry.GoodId);
            Assert.Equal(250, entry.BaseUnitPrice);
            Assert.True(entry.BuyEnabled);
            Assert.True(entry.SellEnabled);
            Assert.Equal(100, entry.InitialStock);
            Assert.Equal(200, entry.TargetStock);
            Assert.Equal(500, entry.MaxStock);
            Assert.Equal(1, entry.PricingPolicyVersion);
            Assert.Equal("li_grp", entry.MarketFactionOverrideId);
            Assert.Equal(32, policy.PolicySha256.Length);
            Assert.Equal(seed.SourceSha256, policy.CatalogSeedSha256);
        });
    }

    [Fact]
    public async Task RejectsPolicyForDifferentCoverageReport()
    {
        await WithDocumentsAsync(async (reportPath, policyPath) =>
        {
            var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
            var policy = Policy(seed).Replace(Convert.ToHexString(seed.SourceSha256), new string('f', 64),
                StringComparison.Ordinal);
            await File.WriteAllTextAsync(policyPath, policy);

            var error = await Assert.ThrowsAsync<InvalidDataException>(() =>
                MarketStockPolicyDocument.LoadAsync(policyPath, seed));
            Assert.Contains("different catalog report hash", error.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task RejectsWrongJsonTypeAsValidationError()
    {
        await WithDocumentsAsync(async (reportPath, policyPath) =>
        {
            var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
            var policy = Policy(seed).Replace("\"initialStock\":100", "\"initialStock\":\"100\"",
                StringComparison.Ordinal);
            await File.WriteAllTextAsync(policyPath, policy);

            var error = await Assert.ThrowsAsync<InvalidDataException>(() =>
                MarketStockPolicyDocument.LoadAsync(policyPath, seed));
            Assert.Contains("Invalid initialStock", error.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task RejectsDuplicatePairAndInvalidCapacity()
    {
        await WithDocumentsAsync(async (reportPath, policyPath) =>
        {
            var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
            var entry = Entry;
            await File.WriteAllTextAsync(policyPath, Policy(seed, $"{entry},{entry}"));
            await Assert.ThrowsAsync<InvalidDataException>(() => MarketStockPolicyDocument.LoadAsync(policyPath, seed));

            await File.WriteAllTextAsync(policyPath, Policy(seed, Entry.Replace("\"maxStock\":500", "\"maxStock\":99",
                StringComparison.Ordinal)));
            var error = await Assert.ThrowsAsync<InvalidDataException>(() =>
                MarketStockPolicyDocument.LoadAsync(policyPath, seed));
            Assert.Contains("exceed maxStock", error.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task RequiresReviewForBuyEnablementOutsideStaticOfferPolicy()
    {
        await WithDocumentsAsync(async (reportPath, policyPath) =>
        {
            var report = MinimalReport.Replace("\"forSale\": true", "\"forSale\": false", StringComparison.Ordinal);
            await File.WriteAllTextAsync(reportPath, report);
            var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
            await File.WriteAllTextAsync(policyPath, Policy(seed));

            var error = await Assert.ThrowsAsync<InvalidDataException>(() =>
                MarketStockPolicyDocument.LoadAsync(policyPath, seed));
            Assert.Contains("requires reviewReason", error.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task RequiresReviewForNewNonStaticPair()
    {
        await WithDocumentsAsync(async (reportPath, policyPath) =>
        {
            var report = JsonNode.Parse(MinimalReport)!.AsObject();
            report["summary"]!["marketEntryCount"] = 0;
            report["markets"]![0]!["entries"] = new JsonArray();
            await File.WriteAllTextAsync(reportPath, report.ToJsonString());
            var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
            await File.WriteAllTextAsync(policyPath, Policy(seed));

            var error = await Assert.ThrowsAsync<InvalidDataException>(() =>
                MarketStockPolicyDocument.LoadAsync(policyPath, seed));
            Assert.Contains("requires reviewReason", error.Message, StringComparison.Ordinal);
        });
    }

    private static string Policy(MarketCatalogSeedDocument seed, string? entries = null) => $$"""
        {
          "schemaVersion": 1,
          "policyVersion": "dynamic_market_stock_v1",
          "policyRevision": 1,
          "catalogSeedSha256": "{{Convert.ToHexString(seed.SourceSha256)}}",
          "entries": [{{entries ?? Entry}}]
        }
        """;

    private const string Entry = """
        {"marketId":"li01_base","goodId":"commodity_food","baseUnitPrice":250,"buyEnabled":true,"sellEnabled":true,"initialStock":100,"targetStock":200,"maxStock":500,"minUnitPrice":50,"maxUnitPrice":1000,"pricingPolicyVersion":1,"marketFactionOverrideId":"li_grp"}
        """;

    private static async Task WithDocumentsAsync(Func<string, string, Task> action)
    {
        var prefix = Path.Combine(Path.GetTempPath(), "market-stock-policy-" + Guid.NewGuid().ToString("N"));
        var reportPath = prefix + ".report.json";
        var policyPath = prefix + ".policy.json";
        try
        {
            await File.WriteAllTextAsync(reportPath, MinimalReport);
            await action(reportPath, policyPath);
        }
        finally
        {
            File.Delete(reportPath);
            File.Delete(policyPath);
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
