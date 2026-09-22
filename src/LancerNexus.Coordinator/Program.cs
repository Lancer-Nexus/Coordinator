using System.Security.Cryptography;
using System.Text;
using LancerNexus.Coordinator;
using LancerNexus.Protocol;

var builder = WebApplication.CreateBuilder(args);
var quicSettings = CoordinatorQuicSettings.FromConfiguration(builder.Configuration);
var registryOptions = new CoordinatorRegistryOptions(
    ReadPositiveSeconds(builder.Configuration, "Coordinator:Registry:AgentHeartbeatTimeoutSeconds", 15),
    ReadPositiveSeconds(builder.Configuration, "Coordinator:Registry:InstanceHeartbeatTimeoutSeconds", 15),
    ReadPositiveSeconds(builder.Configuration, "Coordinator:Placement:GroupAffinityLifetimeSeconds", 30));
var placementPolicyOptions = new PlacementPolicyOptions(
    ReadPositiveSeconds(builder.Configuration, "Coordinator:Placement:MaximumHeartbeatAgeSeconds", 15),
    ReadPositiveSeconds(builder.Configuration, "Coordinator:Placement:ReservationLifetimeSeconds", 15));
builder.Services.AddHealthChecks();
builder.Services.AddSingleton(placementPolicyOptions);
builder.Services.AddSingleton(registryOptions);
builder.Services.AddSingleton<PlacementPolicy>();
var registryStateFile = builder.Configuration["Coordinator:StateFile"] ??
                        Path.Combine(builder.Environment.ContentRootPath, "data", "coordinator-state.json");
builder.Services.AddSingleton<ICoordinatorRegistryStore>(_ => new FileCoordinatorRegistryStore(registryStateFile));
builder.Services.AddSingleton<CoordinatorRegistry>();
builder.Services.AddSingleton(TimeProvider.System);
if (quicSettings is not null)
{
    if (!OperatingSystem.IsLinux())
        throw new PlatformNotSupportedException("The Coordinator QUIC listener is currently supported on Linux only.");
    builder.Services.AddSingleton(quicSettings);
    builder.Services.AddHostedService<CoordinatorQuicHandshakeService>();
}

var app = builder.Build();
var internalApiKey = app.Configuration["Coordinator:InternalApiKey"];

app.Use(async (context, next) =>
{
    var protectedRequest = context.Request.Path.StartsWithSegments("/internal") ||
                          context.Request.Path.StartsWithSegments("/api/v1/placement");
    if (!protectedRequest)
    {
        await next();
        return;
    }

    if (string.IsNullOrWhiteSpace(internalApiKey) || Encoding.UTF8.GetByteCount(internalApiKey) < 32)
    {
        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        await context.Response.WriteAsJsonAsync(new { error = "internal_auth_not_configured" });
        return;
    }

    var authorization = context.Request.Headers.Authorization.ToString();
    const string bearerPrefix = "Bearer ";
    var token = authorization.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase)
        ? authorization[bearerPrefix.Length..]
        : "";
    var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(internalApiKey));
    var suppliedHash = SHA256.HashData(Encoding.UTF8.GetBytes(token));
    if (!CryptographicOperations.FixedTimeEquals(expectedHash, suppliedHash))
    {
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        await context.Response.WriteAsJsonAsync(new { error = "unauthorized" });
        return;
    }

    await next();
});

app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = _ => false
});

app.MapHealthChecks("/health/ready");

app.MapGet("/api/v1/capabilities", () => Results.Ok(new
{
    service = "coordinator",
    protocolVersion = ProtocolConstants.ProtocolVersion,
    capabilities = new[] { "health_v1", "registry_v1", "placement_policy_v1", "placement_reservations_v1" }
        .Concat(quicSettings is null ? [] : ["quic_mtls_handshake_v1"])
}));

app.MapPost("/internal/v1/agents/heartbeat", (
    AgentHeartbeat heartbeat,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var result = registry.ApplyAgentHeartbeat(heartbeat, timeProvider.GetUtcNow());
    return result.Accepted ? Results.Ok(result) : Results.Conflict(result);
});

app.MapPost("/internal/v1/instances/heartbeat", (
    InstanceHeartbeat heartbeat,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var result = registry.ApplyInstanceHeartbeat(heartbeat, timeProvider.GetUtcNow());
    return result.Accepted ? Results.Ok(result) : Results.Conflict(result);
});

app.MapGet("/internal/v1/registry", (CoordinatorRegistry registry, TimeProvider timeProvider) =>
{
    var snapshot = registry.Snapshot(timeProvider.GetUtcNow());
    return Results.Ok(new { agents = snapshot.Agents, instances = snapshot.Instances });
});

app.MapPost("/api/v1/placement", (
    PlacementRequest request,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var outcome = registry.Place(request, timeProvider.GetUtcNow());
    return outcome.Decision.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
});

app.Run();

static TimeSpan ReadPositiveSeconds(IConfiguration configuration, string key, int defaultValue)
{
    var seconds = configuration.GetValue<int?>(key) ?? defaultValue;
    if (seconds <= 0)
        throw new InvalidOperationException($"Configuration value '{key}' must be a positive number of seconds.");
    return TimeSpan.FromSeconds(seconds);
}

public partial class Program;
