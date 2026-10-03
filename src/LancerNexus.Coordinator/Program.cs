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
var npcOwnershipConnectionString = builder.Configuration.GetConnectionString("NpcOwnership") ??
                                   builder.Configuration["Coordinator:NpcOwnershipConnectionString"];
if (string.IsNullOrWhiteSpace(npcOwnershipConnectionString))
    npcOwnershipConnectionString = null;
builder.Services.AddHealthChecks();
builder.Services.AddSingleton(placementPolicyOptions);
builder.Services.AddSingleton(registryOptions);
builder.Services.AddSingleton<PlacementPolicy>();
var registryStateFile = builder.Configuration["Coordinator:StateFile"] ??
                        Path.Combine(builder.Environment.ContentRootPath, "data", "coordinator-state.json");
builder.Services.AddSingleton<ICoordinatorRegistryStore>(_ => new FileCoordinatorRegistryStore(registryStateFile));
builder.Services.AddSingleton<CoordinatorRegistry>();
builder.Services.AddSingleton(new MySqlNpcOwnershipStore(npcOwnershipConnectionString));
builder.Services.AddHttpClient<INpcMissionAuthorityClient, NpcMissionAuthorityClient>(http =>
    http.Timeout = TimeSpan.FromSeconds(8)).ConfigurePrimaryHttpMessageHandler(() =>
    new HttpClientHandler { AllowAutoRedirect = false });
builder.Services.AddSingleton(services => new MySqlNpcTransferStore(npcOwnershipConnectionString,
    services.GetRequiredService<INpcMissionAuthorityClient>()));
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
var startupLogger = app.Logger;
var startupRegistry = app.Services.GetRequiredService<CoordinatorRegistry>();
var initialRegistry = startupRegistry.Snapshot(DateTimeOffset.UtcNow);
var liveAgents = initialRegistry.Agents.Count(agent => agent.IsAlive);
var readyInstances = initialRegistry.Instances.Count(instance =>
    instance.IsAlive && instance.AgentIsAlive && instance.IsReady && !instance.IsDraining);
startupLogger.LogInformation(
    "Coordinator registry loaded: {RegisteredAgents} agents ({LiveAgents} heartbeat-live), {RegisteredInstances} instances ({ReadyInstances} ready)",
    initialRegistry.Agents.Count,
    liveAgents,
    initialRegistry.Instances.Count,
    readyInstances);
startupLogger.LogInformation("Coordinator HTTP service is starting; QUIC mTLS control listener {QuicListenerStatus}.",
    quicSettings is null ? "disabled" : "enabled");

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
        .Concat(npcOwnershipConnectionString is null ? [] : [ClusterCapabilities.NpcOwnershipV1, ClusterCapabilities.NpcTransferV1, ClusterCapabilities.NpcRetirementV1])
        .Concat(quicSettings is null ? [] : ["quic_mtls_handshake_v1", "quic_agent_heartbeat_v1", "quic_instance_heartbeat_v1"])
}));

app.MapPost("/internal/v1/agents/heartbeat", (
    AgentHeartbeat heartbeat,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var now = timeProvider.GetUtcNow();
    var wasAlive = registry.Snapshot(now).Agents.Any(agent =>
        string.Equals(agent.AgentId, heartbeat.AgentId, StringComparison.Ordinal) && agent.IsAlive);
    var result = registry.ApplyAgentHeartbeat(heartbeat, now);
    if (!result.Accepted)
        startupLogger.LogWarning("Agent heartbeat rejected for {AgentId}: {ReasonCode}.", heartbeat.AgentId, result.ReasonCode);
    else if (!wasAlive)
        startupLogger.LogInformation("Agent {AgentId} registered and heartbeat is live.", heartbeat.AgentId);
    return result.Accepted ? Results.Ok(result) : Results.Conflict(result);
});

app.MapPost("/internal/v1/instances/heartbeat", (
    InstanceHeartbeat heartbeat,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var now = timeProvider.GetUtcNow();
    var previous = registry.Snapshot(now).Instances.FirstOrDefault(instance =>
        string.Equals(instance.InstanceId, heartbeat.InstanceId, StringComparison.Ordinal));
    var result = registry.ApplyInstanceHeartbeat(heartbeat, now);
    if (!result.Accepted)
        startupLogger.LogWarning("Instance heartbeat rejected for {InstanceId} from Agent {AgentId}: {ReasonCode}.",
            heartbeat.InstanceId, heartbeat.AgentId, result.ReasonCode);
    else if (previous is null || !previous.IsAlive || !previous.AgentIsAlive)
        startupLogger.LogInformation("Instance {InstanceId} registered on system {SystemId}; ready {IsReady}, draining {IsDraining}, players {CurrentPlayers}/{MaxPlayers}.",
            heartbeat.InstanceId, heartbeat.SystemId, heartbeat.IsReady, heartbeat.IsDraining, heartbeat.CurrentPlayers, heartbeat.MaxPlayers);
    else if (!previous.IsReady && heartbeat.IsReady)
        startupLogger.LogInformation("Instance {InstanceId} is now ready on system {SystemId}.", heartbeat.InstanceId, heartbeat.SystemId);
    else if (previous.IsReady && !heartbeat.IsReady)
        startupLogger.LogWarning("Instance {InstanceId} is no longer ready.", heartbeat.InstanceId);
    else if (previous.IsDraining != heartbeat.IsDraining)
        startupLogger.LogInformation("Instance {InstanceId} draining state changed to {IsDraining}.", heartbeat.InstanceId, heartbeat.IsDraining);
    return result.Accepted ? Results.Ok(result) : Results.Conflict(result);
});

app.MapPost("/internal/v1/npcs/allocate", async (
    NpcIdBatchAllocationRequest request,
    MySqlNpcOwnershipStore ownershipStore,
    CoordinatorRegistry registry,
    TimeProvider timeProvider,
    CancellationToken cancellationToken) =>
{
    if (!ownershipStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);

    var owner = registry.Snapshot(timeProvider.GetUtcNow()).Instances.FirstOrDefault(instance =>
        string.Equals(instance.InstanceId, request.InstanceId, StringComparison.Ordinal) &&
        instance.IsAlive && instance.AgentIsAlive && instance.IsReady && !instance.IsDraining);
    var ownedSystems = owner?.SystemIds is { Length: > 0 } systemIds ? systemIds :
        owner is null ? [] : [owner.SystemId];
    if (owner is null || !ownedSystems.Contains(request.SystemId, StringComparer.OrdinalIgnoreCase))
        return Results.Conflict(new NpcIdBatchAllocationResponse
            { RequestId = request.RequestId, ReasonCode = "instance_not_authorized_for_system" });

    var result = await ownershipStore.AllocateAsync(request, cancellationToken);
    return result.Accepted ? Results.Ok(result) : Results.Conflict(result);
});

app.MapPost("/internal/v1/npc-retirements", async (
    NpcRetirementRequestV1 request,
    MySqlNpcOwnershipStore ownershipStore,
    CancellationToken cancellationToken) =>
{
    if (!ownershipStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    if (!request.IsValid())
        return Results.BadRequest(new NpcRetirementResponseV1
            { RequestId = request.RequestId, ReasonCode = "invalid_npc_retirement_request" });
    // SQL owner/fence checks apply even to draining or temporarily unregistered instances.
    var result = await ownershipStore.RetireAsync(request, cancellationToken);
    return result.ReasonCode == "processed" ? Results.Ok(result) : Results.Conflict(result);
});

app.MapPost("/internal/v1/npc-transfers/prepare", async (
    NpcTransferPrepareRequest request,
    MySqlNpcTransferStore transferStore,
    CoordinatorRegistry registry,
    TimeProvider timeProvider,
    CancellationToken cancellationToken) =>
{
    if (!transferStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    NpcTransferContractValidator.Validate(request);
    var instances = registry.Snapshot(timeProvider.GetUtcNow()).Instances;
    var source = instances.FirstOrDefault(i => i.InstanceId == request.SourceInstanceId && i.IsAlive && i.AgentIsAlive && i.IsReady);
    var target = instances.FirstOrDefault(i => i.InstanceId == request.TargetInstanceId && i.IsAlive && i.AgentIsAlive && i.IsReady && !i.IsDraining);
    var targetSystems = target?.SystemIds is { Length: > 0 } ids ? ids : target is null ? [] : [target.SystemId];
    if (source is null || target is null || !targetSystems.Contains(request.TargetSystemId, StringComparer.OrdinalIgnoreCase) ||
        !source.Capabilities.Contains(ClusterCapabilities.NpcTransferV1, StringComparer.Ordinal) ||
        !target.Capabilities.Contains(ClusterCapabilities.NpcTransferV1, StringComparer.Ordinal))
        return Results.Conflict(new NpcTransferPrepared { TransferId = request.TransferId, ReasonCode = "transfer_instance_unavailable" });
    var result = await transferStore.PrepareAsync(request, timeProvider.GetUtcNow().UtcDateTime, cancellationToken);
    return result.Accepted
        ? Results.Ok(new NpcTransferPrepared
        {
            TransferId = result.TransferId,
            Accepted = true,
            TargetEndpoint = target.Endpoint,
            NpcTransferEndpoint = target.NpcTransferEndpoint,
            ExpiresUtc = result.ExpiresUtc,
            ReasonCode = result.ReasonCode
        })
        : Results.Conflict(result);
});

app.MapPost("/internal/v1/npc-transfers/target", (
    NpcTransferTargetResolveRequestV1 request,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    NpcTransferContractValidator.Validate(request);
    var target = NpcTransferTargetResolver.Resolve(
        registry.Snapshot(timeProvider.GetUtcNow()).Instances, request);
    return target is null
        ? Results.Ok(new NpcTransferTargetResolveResultV1 { ReasonCode = "target_system_unavailable" })
        : Results.Ok(new NpcTransferTargetResolveResultV1
        {
            Found = true,
            TargetInstanceId = target.InstanceId,
            TargetEndpoint = target.Endpoint,
            NpcTransferEndpoint = target.NpcTransferEndpoint,
            ReasonCode = "target_resolved"
        });
});

app.MapPost("/internal/v1/npc-transfers/{transferId:guid}/phase", async (
    Guid transferId,
    NpcTransferPhaseRequest request,
    MySqlNpcTransferStore transferStore,
    TimeProvider timeProvider,
    CancellationToken cancellationToken) =>
{
    if (!transferStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    if (request.TransferId != transferId)
        return Results.Conflict(new NpcTransferPhaseResult(false, "transfer_id_mismatch", transferId, request.State));
    var result = await transferStore.AdvanceAsync(request, timeProvider.GetUtcNow().UtcDateTime, cancellationToken);
    return result.Accepted ? Results.Ok(result) : Results.Conflict(result);
});

app.MapGet("/internal/v1/npc-transfers/{transferId:guid}/recovery", async (
    Guid transferId,
    string instanceId,
    MySqlNpcTransferStore transferStore,
    CancellationToken cancellationToken,
    bool includeSnapshot = false) =>
{
    if (!transferStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    if (string.IsNullOrWhiteSpace(instanceId) || instanceId.Length > 96)
        return Results.BadRequest(new { error = "invalid_instance_id" });
    var record = await transferStore.GetRecoveryRecordAsync(transferId, instanceId, includeSnapshot, cancellationToken);
    if (record is null || (record.SourceInstanceId != instanceId && record.TargetInstanceId != instanceId))
        return Results.NotFound();
    return Results.Ok(record);
});

app.MapGet("/internal/v1/npc-transfers/recovery", async (
    string instanceId,
    Guid? afterTransferId,
    int limit,
    MySqlNpcTransferStore transferStore,
    CancellationToken cancellationToken) =>
{
    if (!transferStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    if (string.IsNullOrWhiteSpace(instanceId) || instanceId.Length > 96 || limit is < 1 or > 128)
        return Results.BadRequest(new { error = "invalid_recovery_query" });
    var page = await transferStore.GetRecoverableTransfersAsync(instanceId, afterTransferId, limit, cancellationToken);
    return Results.Ok(page);
});

app.MapGet("/internal/v1/npc-transfers/source-recovery", async (
    string instanceId,
    Guid? afterTransferId,
    int limit,
    MySqlNpcTransferStore transferStore,
    CancellationToken cancellationToken) =>
{
    if (!transferStore.IsEnabled)
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    if (string.IsNullOrWhiteSpace(instanceId) || instanceId.Length > 96 || limit is < 1 or > 128)
        return Results.BadRequest(new { error = "invalid_recovery_query" });
    var page = await transferStore.GetPendingSourceTransfersAsync(instanceId, afterTransferId, limit, cancellationToken);
    return Results.Ok(page);
});

app.MapGet("/internal/v1/registry", (CoordinatorRegistry registry, TimeProvider timeProvider) =>
{
    var snapshot = registry.Snapshot(timeProvider.GetUtcNow());
    return Results.Ok(new { agents = snapshot.Agents, instances = snapshot.Instances });
});

app.MapGet("/internal/v1/transfers", (CoordinatorRegistry registry, TimeProvider timeProvider) =>
    Results.Ok(registry.TransferSnapshot(timeProvider.GetUtcNow())));

app.MapGet("/internal/v1/transfers/{transferId:guid}", (Guid transferId, CoordinatorRegistry registry, TimeProvider timeProvider) =>
{
    var transfer = registry.GetTransfer(transferId, timeProvider.GetUtcNow());
    return transfer is null ? Results.NotFound() : Results.Ok(transfer);
});

app.MapPost("/internal/v1/transfers/prepare", (
    TransferPrepareRequest request,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var outcome = registry.PrepareTransfer(request, timeProvider.GetUtcNow());
    return outcome.Decision.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
});

app.MapPost("/internal/v1/transfers/{transferId:guid}/source-frozen", (
    Guid transferId,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var outcome = registry.AdvanceTransfer(transferId, TransferState.SourceFrozen, timeProvider.GetUtcNow());
    return outcome.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
});

app.MapPost("/internal/v1/transfers/{transferId:guid}/target-accepted", (
    Guid transferId,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var outcome = registry.AdvanceTransfer(transferId, TransferState.TargetAccepted, timeProvider.GetUtcNow());
    return outcome.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
});

app.MapPost("/internal/v1/transfers/{transferId:guid}/commit/{leaseVersion:long}", (
    Guid transferId,
    long leaseVersion,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var transfer = registry.TransferSnapshot(timeProvider.GetUtcNow())
        .FirstOrDefault(entry => entry.TransferId == transferId);
    if (transfer is null)
        return Results.NotFound(new TransferOperationResult(false, "transfer_not_found", TransferState.Expired));
    var outcome = registry.CommitTransfer(transferId, transfer.Request.CharacterId, leaseVersion, timeProvider.GetUtcNow());
    return outcome.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
});

app.MapPost("/internal/v1/transfers/{transferId:guid}/source-released", (
    Guid transferId,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var outcome = registry.AdvanceTransfer(transferId, TransferState.SourceReleased, timeProvider.GetUtcNow());
    return outcome.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
});

app.MapPost("/internal/v1/transfers/abort", (
    TransferAbort request,
    CoordinatorRegistry registry,
    TimeProvider timeProvider) =>
{
    var outcome = registry.AbortTransfer(request, timeProvider.GetUtcNow());
    return outcome.Accepted ? Results.Ok(outcome) : Results.Conflict(outcome);
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
