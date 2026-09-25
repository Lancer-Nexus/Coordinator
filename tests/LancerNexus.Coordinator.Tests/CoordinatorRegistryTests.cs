using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class CoordinatorRegistryTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 22, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Heartbeats_RegisterOnlyFreshAgentsAndRejectReplayedSequences()
    {
        var registry = CreateRegistry();

        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 2), Now).Accepted);
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 2), Now.AddSeconds(1)).Duplicate);
        Assert.Equal("stale_sequence", registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).ReasonCode);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance(sequence: 2), Now).Accepted);
        Assert.Equal("stale_sequence", registry.ApplyInstanceHeartbeat(Instance(sequence: 1), Now).ReasonCode);
        Assert.Equal("agent_identity_changed", registry.ApplyAgentHeartbeat(Agent(nodeId: "other", sequence: 3), Now).ReasonCode);

        var snapshot = registry.Snapshot(Now.AddSeconds(16));
        Assert.False(Assert.Single(snapshot.Agents).IsAlive);
        Assert.False(Assert.Single(snapshot.Instances).IsAlive);
    }

    [Fact]
    public void InstanceHeartbeat_RequiresCertificateIdentityToMatchRegisteredAgent()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(nodeId: "node-1"), Now).Accepted);

        var mismatch = registry.ApplyInstanceHeartbeat(Instance(), Now, authenticatedNodeId: "node-2");
        var accepted = registry.ApplyInstanceHeartbeat(Instance(), Now, authenticatedNodeId: "node-1");

        Assert.False(mismatch.Accepted);
        Assert.Equal("agent_certificate_mismatch", mismatch.ReasonCode);
        Assert.True(accepted.Accepted);
    }

    [Fact]
    public void Place_IsIdempotentAndHonorsCapacityReservations()
    {
        var registry = CreateRegistry();
        Register(registry, maxPlayers: 1);
        var request = Request("first");

        var accepted = registry.Place(request, Now);
        var retry = registry.Place(new PlacementRequest
        {
            RequestId = Guid.NewGuid(),
            SessionId = request.SessionId,
            TargetSystem = request.TargetSystem,
            IdempotencyKey = request.IdempotencyKey
        }, Now.AddSeconds(1));
        var rejected = registry.Place(Request("second"), Now.AddSeconds(1));

        Assert.True(accepted.Decision.Accepted);
        Assert.True(retry.Duplicate);
        Assert.Equal(accepted.Decision.InstanceId, retry.Decision.InstanceId);
        Assert.False(rejected.Decision.Accepted);
        Assert.Equal("no_ready_capacity", rejected.Decision.ReasonCode);
    }

    [Fact]
    public void Place_RejectsInstanceWhenAgentReportsNotReady()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance(isReady: false), Now).Accepted);

        var outcome = registry.Place(Request("not-ready-instance"), Now);

        Assert.False(outcome.Decision.Accepted);
        Assert.Equal("no_ready_capacity", outcome.Decision.ReasonCode);
    }

    [Fact]
    public void Place_ConflictingIdempotencyKeyIsRejected()
    {
        var registry = CreateRegistry();
        Register(registry);
        Assert.True(registry.Place(Request("same-key"), Now).Decision.Accepted);

        var conflict = registry.Place(Request("same-key", targetSystem: "li02"), Now);

        Assert.False(conflict.Decision.Accepted);
        Assert.Equal("idempotency_conflict", conflict.Decision.ReasonCode);
    }

    [Fact]
    public void Place_ConcurrentAssignmentsCannotOverbookLastSlot()
    {
        var registry = CreateRegistry();
        Register(registry, maxPlayers: 1);
        var outcomes = new System.Collections.Concurrent.ConcurrentBag<PlacementOutcome>();

        Parallel.For(0, 16, index => outcomes.Add(registry.Place(Request($"parallel-{index}"), Now)));

        Assert.Single(outcomes, outcome => outcome.Decision.Accepted);
        Assert.Equal(15, outcomes.Count(outcome => !outcome.Decision.Accepted));
    }

    [Fact]
    public void Place_GroupAffinityDoesNotSilentlyMoveToAnotherInstance()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("one", sequence: 1, maxPlayers: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("two", sequence: 1, maxPlayers: 10), Now).Accepted);
        var first = Request("member-1", groupId: "group");
        Assert.Equal("one", registry.Place(first, Now).Decision.InstanceId);

        var nextMember = Request("member-2", groupId: "group");
        var result = registry.Place(nextMember, Now);

        Assert.False(result.Decision.Accepted);
        Assert.Equal("group_instance_unavailable", result.Decision.ReasonCode);
    }

    [Fact]
    public void FileStore_RestoresHeartbeatsReservationsAndIdempotencyAfterRestart()
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), $"lancer-nexus-registry-{Guid.NewGuid():N}");
        var statePath = Path.Combine(tempDirectory, "state.json");
        try
        {
            var firstProcess = new CoordinatorRegistry(new PlacementPolicy(), CoordinatorRegistryOptions.Default,
                new FileCoordinatorRegistryStore(statePath));
            Register(firstProcess, maxPlayers: 1);
            var request = Request("restart-safe-key");
            var accepted = firstProcess.Place(request, Now);

            var restartedProcess = new CoordinatorRegistry(new PlacementPolicy(), CoordinatorRegistryOptions.Default,
                new FileCoordinatorRegistryStore(statePath));
            var retry = restartedProcess.Place(request, Now.AddSeconds(1));
            var anotherSession = restartedProcess.Place(Request("another-session"), Now.AddSeconds(1));
            var snapshot = restartedProcess.Snapshot(Now.AddSeconds(1));

            Assert.True(accepted.Decision.Accepted);
            Assert.True(retry.Duplicate);
            Assert.Equal(accepted.Decision.InstanceId, retry.Decision.InstanceId);
            Assert.False(anotherSession.Decision.Accepted);
            Assert.True(Assert.Single(snapshot.Agents).IsAlive);
            Assert.Equal(1, Assert.Single(snapshot.Instances).ReservedPlayers);
        }
        finally
        {
            if (Directory.Exists(tempDirectory))
                Directory.Delete(tempDirectory, recursive: true);
        }
    }

    [Fact]
    public void PrepareTransfer_ReservesTheRequestedReadyTargetAndIsIdempotent()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("target", "li02", 2), Now).Accepted);
        var request = TransferRequest();

        var prepared = registry.PrepareTransfer(request, Now);
        var retry = registry.PrepareTransfer(request, Now.AddSeconds(1));
        var target = registry.Snapshot(Now.AddSeconds(1)).Instances.Single(x => x.InstanceId == "target");

        Assert.True(prepared.Decision.Accepted);
        Assert.Equal(TransferState.Prepared, prepared.State);
        Assert.Equal("quic://target:7443", prepared.TargetEndpoint);
        Assert.True(retry.Duplicate);
        Assert.Equal(request.TransferId, retry.Decision.TransferId);
        Assert.Equal(1, target.ReservedPlayers);
    }

    [Fact]
    public void TransferLifecycle_RejectsInvalidOrderAndRequiresLeaseVersionBeforeSourceRelease()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("target", "li02", 2), Now).Accepted);
        var request = TransferRequest();
        Assert.True(registry.PrepareTransfer(request, Now).Decision.Accepted);

        Assert.Equal("invalid_transfer_transition",
            registry.AdvanceTransfer(request.TransferId, TransferState.TargetAccepted, Now).ReasonCode);
        Assert.True(registry.AdvanceTransfer(request.TransferId, TransferState.SourceFrozen, Now).Accepted);
        var sourceFrozen = registry.GetTransfer(request.TransferId, Now);
        Assert.NotNull(sourceFrozen);
        Assert.Equal(TransferState.SourceFrozen, sourceFrozen.State);
        Assert.True(registry.AdvanceTransfer(request.TransferId, TransferState.TargetAccepted, Now).Accepted);
        Assert.Equal("invalid_lease_version",
            registry.AdvanceTransfer(request.TransferId, TransferState.Committed, Now).ReasonCode);
        Assert.True(registry.AdvanceTransfer(request.TransferId, TransferState.Committed, Now, leaseVersion: 7).Accepted);
        Assert.Equal("transfer_already_committed",
            registry.AbortTransfer(new TransferAbort { TransferId = request.TransferId, ReasonCode = "late_abort" }, Now).ReasonCode);
        Assert.True(registry.AdvanceTransfer(request.TransferId, TransferState.SourceReleased, Now).Accepted);
        Assert.Equal(0, registry.Snapshot(Now).Instances.Single(x => x.InstanceId == "target").ReservedPlayers);
        Assert.Equal(7, Assert.Single(registry.TransferSnapshot(Now)).LeaseVersion);
    }

    [Fact]
    public void PrepareTransfer_RejectsWrongSystemFullOrStaleTargets()
    {
        var wrongSystem = CreateRegistry();
        Assert.True(wrongSystem.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(wrongSystem.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
        Assert.True(wrongSystem.ApplyInstanceHeartbeat(Instance("target", "li01", 2), Now).Accepted);
        Assert.Equal("target_system_mismatch",
            wrongSystem.PrepareTransfer(TransferRequest(targetSystem: "li02"), Now).Decision.ReasonCode);

        var full = CreateRegistry();
        Assert.True(full.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(full.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
        Assert.True(full.ApplyInstanceHeartbeat(Instance("target", "li02", 2, maxPlayers: 1, currentPlayers: 1), Now).Accepted);
        Assert.Equal("target_instance_unavailable",
            full.PrepareTransfer(TransferRequest(), Now).Decision.ReasonCode);

        var stale = CreateRegistry();
        Assert.True(stale.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(stale.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
        Assert.True(stale.ApplyInstanceHeartbeat(Instance("target", "li02", 2), Now).Accepted);
        Assert.True(stale.ApplyAgentHeartbeat(Agent(sequence: 2), Now.AddSeconds(16)).Accepted);
        Assert.True(stale.ApplyInstanceHeartbeat(Instance("source", "li01", 2), Now.AddSeconds(16)).Accepted);
        Assert.Equal("target_instance_unavailable",
            stale.PrepareTransfer(TransferRequest(), Now.AddSeconds(16)).Decision.ReasonCode);
    }

    [Fact]
    public void FileStore_RestoresPreparedTransfersAndTheirCapacityReservation()
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), $"lancer-nexus-transfer-{Guid.NewGuid():N}");
        var statePath = Path.Combine(tempDirectory, "state.json");
        try
        {
            var first = new CoordinatorRegistry(new PlacementPolicy(), CoordinatorRegistryOptions.Default,
                new FileCoordinatorRegistryStore(statePath));
            Assert.True(first.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
            Assert.True(first.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
            Assert.True(first.ApplyInstanceHeartbeat(Instance("target", "li02", 2), Now).Accepted);
            var request = TransferRequest();
            Assert.True(first.PrepareTransfer(request, Now).Decision.Accepted);
            Assert.True(first.AdvanceTransfer(request.TransferId, TransferState.SourceFrozen, Now).Accepted);

            var restarted = new CoordinatorRegistry(new PlacementPolicy(), CoordinatorRegistryOptions.Default,
                new FileCoordinatorRegistryStore(statePath));
            var transfer = Assert.Single(restarted.TransferSnapshot(Now.AddSeconds(1)));
            var target = restarted.Snapshot(Now.AddSeconds(1)).Instances.Single(x => x.InstanceId == "target");

            Assert.Equal(TransferState.SourceFrozen, transfer.State);
            Assert.Equal(1, target.ReservedPlayers);
            Assert.True(restarted.PrepareTransfer(request, Now.AddSeconds(1)).Duplicate);
        }
        finally
        {
            if (Directory.Exists(tempDirectory))
                Directory.Delete(tempDirectory, recursive: true);
        }
    }

    [Fact]
    public void TransferAbortExpiryAndCommitReplayPreserveReservationRules()
    {
        var registry = CreateRegistry();
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("source", "li01", 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance("target", "li02", 2), Now).Accepted);

        var aborted = TransferRequest();
        Assert.True(registry.PrepareTransfer(aborted, Now).Decision.Accepted);
        var keyConflict = TransferRequest(idempotencyKey: aborted.IdempotencyKey);
        Assert.Equal("idempotency_conflict", registry.PrepareTransfer(keyConflict, Now).Decision.ReasonCode);
        Assert.True(registry.AbortTransfer(new TransferAbort
        {
            TransferId = aborted.TransferId,
            ReasonCode = "source_rejected",
            Retryable = true
        }, Now).Accepted);

        var expiring = TransferRequest();
        Assert.True(registry.PrepareTransfer(expiring, Now).Decision.Accepted);
        var expired = registry.TransferSnapshot(Now.AddSeconds(61)).Single(x => x.TransferId == expiring.TransferId);

        Assert.Equal(TransferState.Expired, expired.State);
        Assert.Equal(0, registry.Snapshot(Now.AddSeconds(61)).Instances.Single(x => x.InstanceId == "target").ReservedPlayers);
    }

    [Fact]
    public void FileStore_MigratesVersionOneStateWithoutLosingRegistryData()
    {
        var tempDirectory = Path.Combine(Path.GetTempPath(), $"lancer-nexus-registry-v1-{Guid.NewGuid():N}");
        var statePath = Path.Combine(tempDirectory, "state.json");
        Directory.CreateDirectory(tempDirectory);
        try
        {
            File.WriteAllText(statePath, """
                {"schemaVersion":1,"agents":[],"instances":[],"reservations":[],"groupAffinities":[]}
                """);

            var state = new FileCoordinatorRegistryStore(statePath).Load();

            Assert.Equal(CoordinatorRegistryState.CurrentSchemaVersion, state.SchemaVersion);
            Assert.Empty(state.Transfers);
        }
        finally
        {
            if (Directory.Exists(tempDirectory))
                Directory.Delete(tempDirectory, recursive: true);
        }
    }

    private static CoordinatorRegistry CreateRegistry() => new(new PlacementPolicy(), CoordinatorRegistryOptions.Default);

    private static void Register(CoordinatorRegistry registry, int maxPlayers = 20)
    {
        Assert.True(registry.ApplyAgentHeartbeat(Agent(sequence: 1), Now).Accepted);
        Assert.True(registry.ApplyInstanceHeartbeat(Instance(sequence: 1, maxPlayers: maxPlayers), Now).Accepted);
    }

    private static AgentHeartbeat Agent(string nodeId = "node-1", ulong sequence = 1) => new()
    {
        AgentId = "agent-1",
        NodeId = nodeId,
        BuildVersion = "test",
        Sequence = sequence
    };

    private static InstanceHeartbeat Instance(string id = "instance-1", string system = "li01", ulong sequence = 1, int maxPlayers = 20, bool isReady = true, int currentPlayers = 0) => new()
    {
        AgentId = "agent-1",
        InstanceId = id,
        SystemId = system,
        Sequence = sequence,
        IsReady = isReady,
        CurrentPlayers = currentPlayers,
        MaxPlayers = maxPlayers,
        Endpoint = $"quic://{id}:7443"
    };

    private static TransferPrepareRequest TransferRequest(string targetSystem = "li02", string? idempotencyKey = null) => new()
    {
        TransferId = Guid.NewGuid(),
        SessionId = Guid.NewGuid(),
        CharacterId = 42,
        SourceInstanceId = "source",
        TargetInstanceId = "target",
        TargetSystemId = targetSystem,
        ExpiresUtc = Now.AddMinutes(1).UtcDateTime,
        IdempotencyKey = idempotencyKey ?? Guid.NewGuid().ToString("N")
    };

    private static PlacementRequest Request(string key, string targetSystem = "li01", string? groupId = null) => new()
    {
        RequestId = Guid.NewGuid(),
        SessionId = Guid.NewGuid(),
        TargetSystem = targetSystem,
        GroupId = groupId,
        IdempotencyKey = key
    };
}
