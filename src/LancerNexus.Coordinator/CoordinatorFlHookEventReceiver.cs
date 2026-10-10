using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed class CoordinatorFlHookEventReceiver(
    CoordinatorRegistry registry,
    IFlHookEventStore store,
    TimeProvider timeProvider)
{
    public async Task<FlHookEventIngestResultV1> IngestAsync(Guid correlationId, ulong frameSequence,
        string certificateNodeId, FlHookEventV1 value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (correlationId == Guid.Empty || !AdminQuery.ValidIdentifier(value.InstanceId) ||
            value.ProducerEpoch == Guid.Empty || value.Sequence <= 0 || (ulong)value.Sequence != frameSequence)
            throw new ProtocolViolationException("FLHook event identity does not match its authenticated frame.");

        var now = timeProvider.GetUtcNow();
        var snapshot = registry.Snapshot(now);
        var instance = snapshot.Instances.FirstOrDefault(candidate =>
            candidate.InstanceId == value.InstanceId && candidate.IsAlive && candidate.AgentIsAlive);
        var certificateOwnsInstance = instance is not null && snapshot.Agents.Any(agent =>
            agent.IsAlive && agent.AgentId == instance.AgentId &&
            string.Equals(agent.NodeId, certificateNodeId, StringComparison.OrdinalIgnoreCase));

        var status = !certificateOwnsInstance || !value.IsValid()
            ? FlHookEventIngestStatusV1.Rejected
            : await store.IngestAsync(value, cancellationToken);
        return new FlHookEventIngestResultV1
        {
            CorrelationId = correlationId,
            InstanceId = value.InstanceId,
            ProducerEpoch = value.ProducerEpoch,
            Sequence = value.Sequence,
            Status = status
        };
    }
}
