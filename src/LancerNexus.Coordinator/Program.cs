using LancerNexus.Coordinator;
using LancerNexus.Protocol;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHealthChecks();
builder.Services.AddSingleton(PlacementPolicyOptions.Default);
builder.Services.AddSingleton<PlacementPolicy>();

var app = builder.Build();

app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = _ => false
});

app.MapHealthChecks("/health/ready");

app.MapGet("/api/v1/capabilities", () => Results.Ok(new
{
    service = "coordinator",
    protocolVersion = ProtocolConstants.ProtocolVersion,
    capabilities = new[] { "health_v1", "placement_policy_v1" }
}));

app.Run();

public partial class Program;
