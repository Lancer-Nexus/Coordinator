using System.Security.Cryptography;
using System.Text;
using LancerNexus.Coordinator;
using LancerNexus.Protocol;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHealthChecks();
builder.Services.AddSingleton(PlacementPolicyOptions.Default);
builder.Services.AddSingleton(CoordinatorRegistryOptions.Default);
builder.Services.AddSingleton<PlacementPolicy>();
var registryStateFile = builder.Configuration["Coordinator:StateFile"] ??
                        Path.Combine(builder.Environment.ContentRootPath, "data", "coordinator-state.json");
builder.Services.AddSingleton<ICoordinatorRegistryStore>(_ => new FileCoordinatorRegistryStore(registryStateFile));
builder.Services.AddSingleton<CoordinatorRegistry>();
builder.Services.AddSingleton(TimeProvider.System);

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

public partial class Program;
