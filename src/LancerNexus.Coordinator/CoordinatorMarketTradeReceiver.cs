using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed class CoordinatorMarketTradeReceiver(
    CoordinatorRegistry registry,
    MySqlMarketTradeStore store,
    TimeProvider timeProvider)
{
    public async Task<MarketTradeEventAcknowledgementV1> ApplyAsync(ulong frameSequence,
        string certificateNodeId, MarketTradeEventV1 trade, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(trade);
        if (!trade.IsValid() || (ulong)trade.Sequence != frameSequence)
            throw new ProtocolViolationException("Market trade identity does not match its authenticated frame.");

        var snapshot = registry.Snapshot(timeProvider.GetUtcNow());
        var instance = snapshot.Instances.FirstOrDefault(candidate =>
            candidate.InstanceId == trade.InstanceId && candidate.IsAlive && candidate.AgentIsAlive);
        var certificateOwnsInstance = instance is not null && snapshot.Agents.Any(agent =>
            agent.IsAlive && agent.AgentId == instance.AgentId &&
            string.Equals(agent.NodeId, certificateNodeId, StringComparison.OrdinalIgnoreCase));
        if (!certificateOwnsInstance)
            return new MarketTradeEventAcknowledgementV1
            {
                EventId = trade.EventId,
                InstanceId = trade.InstanceId,
                ProducerEpoch = trade.ProducerEpoch,
                Sequence = trade.Sequence,
                Status = MarketTradeIngestStatusV1.Rejected
            };

        var result = await store.ApplyAsync(trade, cancellationToken);
        if (!result.Matches(trade))
            throw new ProtocolViolationException("Market store returned an acknowledgement for another event.");
        return result;
    }
}
