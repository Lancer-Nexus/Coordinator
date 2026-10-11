using LancerNexus.Coordinator;
using System.Text.Json;

if (args.Length == 1 && args[0] is "--help" or "-h" || args.Length is < 1 or > 3)
{
    Console.Error.WriteLine("Usage: MarketCatalogSeeder [--validate] <analyze-market-catalog.json>");
    Console.Error.WriteLine("       MarketCatalogSeeder --validate-policy <analyze-market-catalog.json> <market-stock-policy.json>");
    Console.Error.WriteLine("       MarketCatalogSeeder --import-policy <analyze-market-catalog.json> <market-stock-policy.json>");
    return args.Length == 1 ? 0 : 2;
}

if (args.Length == 3 && args[0] == "--validate-policy")
{
    try
    {
        var seed = await MarketCatalogSeedDocument.LoadAsync(args[1]);
        var policy = await MarketStockPolicyDocument.LoadAsync(args[2], seed);
        Console.WriteLine($"Market stock policy valid: revision {policy.PolicyRevision}, " +
                          $"{policy.Entries.Count} enabled market/good entries, " +
                          $"catalog {Convert.ToHexString(policy.CatalogSeedSha256.Span)}.");
        Console.WriteLine("Validation only; no database rows were changed.");
        return 0;
    }
    catch (Exception exception) when (exception is IOException or InvalidDataException or JsonException or
                                       KeyNotFoundException or FormatException or OverflowException or InvalidOperationException)
    {
        Console.Error.WriteLine($"Market stock policy failed: {exception.Message}");
        return 1;
    }
}

if (args.Length == 3 && args[0] == "--import-policy")
{
    try
    {
        var seed = await MarketCatalogSeedDocument.LoadAsync(args[1]);
        var policy = await MarketStockPolicyDocument.LoadAsync(args[2], seed);
        var connectionString = Environment.GetEnvironmentVariable("ConnectionStrings__Market") ??
                               Environment.GetEnvironmentVariable("Coordinator__MarketConnectionString");
        var result = await new MarketStockPolicyImporter(connectionString).ImportAsync(seed, policy);
        Console.WriteLine($"Market stock policy {result}: revision {policy.PolicyRevision}, " +
                          $"{policy.Entries.Count} market/good entries.");
        return 0;
    }
    catch (Exception exception) when (exception is IOException or InvalidDataException or JsonException or
                                       KeyNotFoundException or FormatException or OverflowException or InvalidOperationException or
                                       MySqlConnector.MySqlException)
    {
        Console.Error.WriteLine($"Market stock policy import failed: {exception.Message}");
        return 1;
    }
}

var validateOnly = args.Length == 2 && args[0] == "--validate";
if (!validateOnly && args.Length != 1)
{
    Console.Error.WriteLine("Unsupported MarketCatalogSeeder arguments. Use --help for usage.");
    return 2;
}
var reportPath = validateOnly ? args[1] : args[^1];
var connectionString = validateOnly ? null : Environment.GetEnvironmentVariable("ConnectionStrings__Market") ??
                                             Environment.GetEnvironmentVariable("Coordinator__MarketConnectionString");
if (!validateOnly && string.IsNullOrWhiteSpace(connectionString))
{
    Console.Error.WriteLine("Configure ConnectionStrings__Market or Coordinator__MarketConnectionString.");
    return 2;
}

try
{
    var seed = await MarketCatalogSeedDocument.LoadAsync(reportPath);
    if (validateOnly)
    {
        Console.WriteLine($"Market seed valid: {seed.Goods.Length} goods, {seed.Systems.Length} systems, " +
                          $"{seed.Markets.Length} market locations, {seed.Offers.Length} static offers.");
        return 0;
    }
    var result = await new MarketCatalogSeedImporter(connectionString).ImportAsync(seed);
    Console.WriteLine($"Market seed {result}: {seed.Goods.Length} goods, {seed.Systems.Length} systems, " +
                      $"{seed.Markets.Length} market locations, {seed.Offers.Length} static offers.");
    Console.WriteLine("Live market stock and prices were not initialized from static INI amounts.");
    return 0;
}
catch (Exception exception) when (exception is IOException or InvalidDataException or JsonException or
                                   KeyNotFoundException or FormatException or OverflowException or
                                   MySqlConnector.MySqlException or InvalidOperationException)
{
    Console.Error.WriteLine($"Market seed failed: {exception.Message}");
    return 1;
}
