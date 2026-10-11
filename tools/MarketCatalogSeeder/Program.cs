using LancerNexus.Coordinator;
using System.Text.Json;

if (args.Length == 1 && args[0] is "--help" or "-h" || args.Length is < 1 or > 2)
{
    Console.Error.WriteLine("Usage: MarketCatalogSeeder [--validate] <analyze-market-catalog.json>");
    return args.Length == 1 ? 0 : 2;
}

var validateOnly = args.Length == 2 && args[0] == "--validate";
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
