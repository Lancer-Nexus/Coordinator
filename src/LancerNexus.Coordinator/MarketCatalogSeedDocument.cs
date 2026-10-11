using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record MarketCatalogGoodSeed(string GoodId, string Category, long? BaseUnitPrice);
public sealed record MarketSystemSeed(string SystemId);
public sealed record MarketLocationSeed(string MarketId, string SystemId, string LocalFactionId);
public sealed record MarketStaticOfferSeed(string MarketId, string GoodId, long BaselineUnitPrice,
    bool BuyEnabled, int RankRequirement, int MinAmount, int MaxAmount, decimal ReputationThreshold, string SourceFile);

/// <summary>A validated import view over analyze-market-catalog.py schema version 1 output.</summary>
public sealed record MarketCatalogSeedDocument(
    byte[] SourceSha256,
    MarketCatalogGoodSeed[] Goods,
    MarketSystemSeed[] Systems,
    MarketLocationSeed[] Markets,
    MarketStaticOfferSeed[] Offers)
{
    public static async Task<MarketCatalogSeedDocument> LoadAsync(string path, CancellationToken cancellationToken = default)
    {
        var source = await File.ReadAllBytesAsync(path, cancellationToken);
        using var json = JsonDocument.Parse(source);
        var root = json.RootElement;
        if (root.GetProperty("schemaVersion").GetInt32() != 1)
            throw new InvalidDataException("Unsupported market catalog report schema.");
        ValidateReportHasNoCoverageErrors(root);

        var goods = root.GetProperty("goods").EnumerateArray().Select(item =>
        {
            var goodId = Id(item, "goodId");
            var category = item.GetProperty("category").GetString() ?? "";
            if (!MarketContractValidation.ValidId(category) || category.Length > 64)
                throw new InvalidDataException($"Invalid category for market good '{goodId}'.");
            var rawPrice = item.GetProperty("price").GetString();
            long? price = string.IsNullOrWhiteSpace(rawPrice)
                ? item.GetProperty("priceDeclared").GetBoolean()
                    ? throw new InvalidDataException($"Missing declared base price for '{goodId}'.")
                    : null
                : ToNonnegativeLong(decimal.Truncate(Decimal(item.GetProperty("price"), $"price for {goodId}")), $"price for {goodId}");
            return new MarketCatalogGoodSeed(goodId, category, price);
        }).ToArray();
        EnsureUnique(goods.Select(good => good.GoodId), "good ID");
        var goodIds = goods.Select(good => good.GoodId).ToHashSet(StringComparer.Ordinal);

        var systems = root.GetProperty("systems").EnumerateArray()
            .Select(item => new MarketSystemSeed(Id(item, "systemId"))).ToArray();
        EnsureUnique(systems.Select(system => system.SystemId), "system ID");

        var markets = new List<MarketLocationSeed>();
        var offers = new List<MarketStaticOfferSeed>();
        var offerKeys = new HashSet<(string MarketId, string GoodId)>();
        foreach (var market in root.GetProperty("markets").EnumerateArray())
        {
            var marketId = Id(market, "marketId");
            var faction = Id(market, "localFactionId");
            var mappedSystems = market.GetProperty("systemIds").EnumerateArray()
                .Select(value => RequiredId(value.GetString(), "market system ID")).ToArray();
            if (mappedSystems.Length != 1 || !systems.Any(system => system.SystemId == mappedSystems[0]))
                throw new InvalidDataException($"Market '{marketId}' must map to exactly one catalog system.");
            markets.Add(new MarketLocationSeed(marketId, mappedSystems[0], faction));

            foreach (var entry in market.GetProperty("entries").EnumerateArray())
            {
                var goodId = Id(entry, "goodId");
                if (!goodIds.Contains(goodId))
                    throw new InvalidDataException($"Market '{marketId}' references unknown good '{goodId}'.");
                if (!offerKeys.Add((marketId, goodId)))
                    throw new InvalidDataException($"Duplicate static offer '{marketId}/{goodId}'.");

                var runtimePrice = entry.TryGetProperty("legacyRuntimeUnitPrice", out var runtimeValue)
                    ? runtimeValue : default;
                var packagePrice = entry.TryGetProperty("legacyPackagePrice", out var packageValue)
                    ? packageValue : default;
                var priceValue = runtimePrice.ValueKind == JsonValueKind.Number ? runtimePrice : packagePrice;
                if (priceValue.ValueKind != JsonValueKind.Number)
                    throw new InvalidDataException($"No resolved legacy baseline price for '{marketId}/{goodId}'.");
                var price = ToNonnegativeLong(priceValue.GetInt64(), $"baseline price for {marketId}/{goodId}");
                var sourceFile = RequiredId(entry.GetProperty("source").GetString(), "source file");
                if (sourceFile.Length > 96)
                    throw new InvalidDataException($"Source file name is too long for '{marketId}/{goodId}'.");
                offers.Add(new MarketStaticOfferSeed(marketId, goodId, price,
                    entry.GetProperty("forSale").GetBoolean(), entry.GetProperty("rank").GetInt32(),
                    entry.GetProperty("min").GetInt32(), entry.GetProperty("max").GetInt32(),
                    Decimal(entry.GetProperty("reputationThreshold"), $"reputation threshold for {marketId}/{goodId}", allowNegative: true),
                    sourceFile));
            }
        }
        EnsureUnique(markets.Select(market => market.MarketId), "market ID");

        var summary = root.GetProperty("summary");
        if (goods.Length != summary.GetProperty("goodDefinitionCount").GetInt32() ||
            systems.Length != summary.GetProperty("nexusSystemCount").GetInt32() ||
            offers.Count != summary.GetProperty("marketEntryCount").GetInt32())
            throw new InvalidDataException("Market catalog report summary does not match its rows.");

        return new MarketCatalogSeedDocument(SHA256.HashData(source), goods, systems, [.. markets], [.. offers]);
    }

    private static void ValidateReportHasNoCoverageErrors(JsonElement root)
    {
        foreach (var name in new[] { "unknownMarketGoodIds", "duplicateGoodIds", "duplicateMBaseIds",
                     "marketsMissingMBase", "marketsMissingLocalFaction", "unpricedShipPackages",
                     "malformedMarketEntries", "unmappedMarketIds", "marketsOutsideNexusSystems",
                     "missingBaseDefinitions", "unassignedUniverseSystemIds" })
            if (root.GetProperty(name).GetArrayLength() != 0)
                throw new InvalidDataException($"Market catalog report contains coverage errors in '{name}'.");
    }

    private static string Id(JsonElement element, string name) =>
        RequiredId(element.GetProperty(name).GetString(), name);

    private static string RequiredId(string? value, string name)
    {
        if (!MarketContractValidation.ValidId(value))
            throw new InvalidDataException($"Invalid {name} in market catalog report.");
        return value!;
    }

    private static decimal Decimal(JsonElement element, string description, bool allowNegative = false)
    {
        var value = element.ValueKind == JsonValueKind.String ? element.GetString() : element.GetRawText();
        if (!decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var result) ||
            (!allowNegative && result < 0) ||
            (allowNegative && result is < -9999.999999m or > 9999.999999m))
            throw new InvalidDataException($"Invalid {description} in market catalog report.");
        return result;
    }

    private static long ToNonnegativeLong(decimal value, string description)
    {
        if (value is < 0 or > long.MaxValue)
            throw new InvalidDataException($"Out-of-range {description} in market catalog report.");
        return decimal.ToInt64(value);
    }

    private static void EnsureUnique(IEnumerable<string> values, string description)
    {
        if (values.Distinct(StringComparer.Ordinal).Count() != values.Count())
            throw new InvalidDataException($"Duplicate {description} in market catalog report.");
    }
}
