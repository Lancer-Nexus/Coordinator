using System.Security.Cryptography;
using System.Text.Json;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record MarketStockPolicyEntry(
    string MarketId,
    string GoodId,
    long BaseUnitPrice,
    bool BuyEnabled,
    bool SellEnabled,
    long InitialStock,
    long TargetStock,
    long MaxStock,
    long MinUnitPrice,
    long MaxUnitPrice,
    int PricingPolicyVersion,
    string? MarketFactionOverrideId,
    string? ReviewReason);

/// <summary>A fail-closed, report-pinned allowlist for later live-market initialization.</summary>
public sealed class MarketStockPolicyDocument
{
    public ReadOnlyMemory<byte> PolicySha256 { get; }
    public ReadOnlyMemory<byte> CatalogSeedSha256 { get; }
    public long PolicyRevision { get; }
    public string PolicyVersion { get; }
    public IReadOnlyList<MarketStockPolicyEntry> Entries { get; }

    private MarketStockPolicyDocument(byte[] policySha256, byte[] catalogSeedSha256,
        long policyRevision, string policyVersion, MarketStockPolicyEntry[] entries)
    {
        PolicySha256 = policySha256;
        CatalogSeedSha256 = catalogSeedSha256;
        PolicyRevision = policyRevision;
        PolicyVersion = policyVersion;
        Entries = Array.AsReadOnly(entries);
    }

    public static async Task<MarketStockPolicyDocument> LoadAsync(string path,
        MarketCatalogSeedDocument seed, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(seed);
        var source = await File.ReadAllBytesAsync(path, cancellationToken);
        using var json = JsonDocument.Parse(source);
        var root = json.RootElement;
        if (RequiredInt32(root, "schemaVersion") != 1)
            throw new InvalidDataException("Unsupported market stock policy schema.");

        var reportHashText = RequiredString(root, "catalogSeedSha256");
        if (reportHashText.Length != 64)
            throw new InvalidDataException("Invalid catalogSeedSha256 in market stock policy.");
        byte[] reportHash;
        try { reportHash = Convert.FromHexString(reportHashText); }
        catch (FormatException exception)
        {
            throw new InvalidDataException("Invalid catalogSeedSha256 in market stock policy.", exception);
        }
        if (!CryptographicOperations.FixedTimeEquals(reportHash, seed.SourceSha256))
            throw new InvalidDataException("Market stock policy targets a different catalog report hash.");

        var policyVersion = RequiredString(root, "policyVersion");
        if (!MarketContractValidation.ValidId(policyVersion) || policyVersion.Length > 64)
            throw new InvalidDataException("Invalid policyVersion in market stock policy.");
        var revision = RequiredInt64(root, "policyRevision");
        if (revision <= 0)
            throw new InvalidDataException("Market stock policy revision must be positive.");

        var markets = seed.Markets.Select(market => market.MarketId).ToHashSet(StringComparer.Ordinal);
        var goods = seed.Goods.Select(good => good.GoodId).ToHashSet(StringComparer.Ordinal);
        var offers = seed.Offers.ToDictionary(offer => (offer.MarketId, offer.GoodId));
        var keys = new HashSet<(string MarketId, string GoodId)>();
        var entries = new List<MarketStockPolicyEntry>();
        foreach (var item in root.GetProperty("entries").EnumerateArray())
        {
            var marketId = RequiredId(item, "marketId");
            var goodId = RequiredId(item, "goodId");
            if (!markets.Contains(marketId) || !goods.Contains(goodId))
                throw new InvalidDataException($"Unknown market/good pair '{marketId}/{goodId}' in market stock policy.");
            if (!keys.Add((marketId, goodId)))
                throw new InvalidDataException($"Duplicate market stock policy entry '{marketId}/{goodId}'.");

            var basePrice = Nonnegative(item, "baseUnitPrice");
            var buyEnabled = RequiredBoolean(item, "buyEnabled");
            var sellEnabled = RequiredBoolean(item, "sellEnabled");
            var initialStock = Nonnegative(item, "initialStock");
            var targetStock = Positive(item, "targetStock");
            var maxStock = Positive(item, "maxStock");
            var minPrice = Nonnegative(item, "minUnitPrice");
            var maxPrice = Nonnegative(item, "maxUnitPrice");
            var pricingVersion = RequiredInt32(item, "pricingPolicyVersion");
            var factionOverride = OptionalId(item, "marketFactionOverrideId");
            var reviewReason = OptionalString(item, "reviewReason");

            if (!buyEnabled && !sellEnabled)
                throw new InvalidDataException($"Disabled market stock policy entry '{marketId}/{goodId}' is redundant.");
            if (initialStock > maxStock || targetStock > maxStock)
                throw new InvalidDataException($"Stock values exceed maxStock for '{marketId}/{goodId}'.");
            if (buyEnabled && initialStock == 0)
                throw new InvalidDataException($"Buy-enabled market '{marketId}/{goodId}' needs positive initialStock.");
            if (sellEnabled && initialStock >= maxStock)
                throw new InvalidDataException($"Sell-enabled market '{marketId}/{goodId}' needs room below maxStock.");
            if (minPrice > basePrice || basePrice > maxPrice)
                throw new InvalidDataException($"baseUnitPrice must be within the price bounds for '{marketId}/{goodId}'.");
            if (pricingVersion != 1)
                throw new InvalidDataException($"Unsupported pricingPolicyVersion for '{marketId}/{goodId}'.");

            if (offers.TryGetValue((marketId, goodId), out var offer))
            {
                if (offer.BaselineUnitPrice != basePrice)
                    throw new InvalidDataException($"Policy baseUnitPrice differs from the static offer for '{marketId}/{goodId}'.");
                if (buyEnabled && !offer.BuyEnabled && string.IsNullOrWhiteSpace(reviewReason))
                    throw new InvalidDataException($"Enabling static buy-disabled offer '{marketId}/{goodId}' requires reviewReason.");
            }
            else if (string.IsNullOrWhiteSpace(reviewReason))
                throw new InvalidDataException($"Adding non-static market offer '{marketId}/{goodId}' requires reviewReason.");

            entries.Add(new MarketStockPolicyEntry(marketId, goodId, basePrice,
                buyEnabled, sellEnabled, initialStock, targetStock, maxStock,
                minPrice, maxPrice, pricingVersion, factionOverride, reviewReason));
        }

        if (entries.Count == 0)
            throw new InvalidDataException("Market stock policy must contain at least one enabled entry.");

        return new MarketStockPolicyDocument(SHA256.HashData(source), reportHash, revision,
            policyVersion, [.. entries]);
    }

    private static string RequiredId(JsonElement parent, string name)
    {
        var value = RequiredString(parent, name);
        if (!MarketContractValidation.ValidId(value))
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        return value;
    }

    private static string RequiredString(JsonElement parent, string name)
    {
        var property = RequiredProperty(parent, name);
        if (property.ValueKind != JsonValueKind.String)
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        var value = property.GetString();
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidDataException($"Missing {name} in market stock policy.");
        return value;
    }

    private static string? OptionalString(JsonElement parent, string name)
    {
        if (!parent.TryGetProperty(name, out var value)) return null;
        if (value.ValueKind != JsonValueKind.String)
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        var result = value.GetString();
        if (result?.Length > 512)
            throw new InvalidDataException($"{name} is too long in market stock policy.");
        return result;
    }

    private static string? OptionalId(JsonElement parent, string name)
    {
        var value = OptionalString(parent, name);
        if (value is null) return null;
        if (!MarketContractValidation.ValidId(value) || value.Length > 96)
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        return value;
    }

    private static long Nonnegative(JsonElement parent, string name)
    {
        var value = RequiredInt64(parent, name);
        if (value < 0)
            throw new InvalidDataException($"{name} must be nonnegative in market stock policy.");
        return value;
    }

    private static long Positive(JsonElement parent, string name)
    {
        var value = Nonnegative(parent, name);
        if (value == 0)
            throw new InvalidDataException($"{name} must be positive in market stock policy.");
        return value;
    }

    private static JsonElement RequiredProperty(JsonElement parent, string name)
    {
        if (!parent.TryGetProperty(name, out var value))
            throw new InvalidDataException($"Missing {name} in market stock policy.");
        return value;
    }

    private static long RequiredInt64(JsonElement parent, string name)
    {
        if (RequiredProperty(parent, name).ValueKind != JsonValueKind.Number ||
            !RequiredProperty(parent, name).TryGetInt64(out var value))
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        return value;
    }

    private static int RequiredInt32(JsonElement parent, string name)
    {
        if (RequiredProperty(parent, name).ValueKind != JsonValueKind.Number ||
            !RequiredProperty(parent, name).TryGetInt32(out var value))
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        return value;
    }

    private static bool RequiredBoolean(JsonElement parent, string name)
    {
        if (RequiredProperty(parent, name).ValueKind is not (JsonValueKind.True or JsonValueKind.False))
            throw new InvalidDataException($"Invalid {name} in market stock policy.");
        return RequiredProperty(parent, name).GetBoolean();
    }
}
