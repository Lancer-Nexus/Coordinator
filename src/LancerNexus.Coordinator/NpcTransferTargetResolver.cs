using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public static class NpcTransferTargetResolver
{
    public static InstanceRegistryView? Resolve(IEnumerable<InstanceRegistryView> instances,
        NpcTransferTargetResolveRequestV1 request)
    {
        NpcTransferContractValidator.Validate(request);
        return instances
            .Where(instance => instance.InstanceId != request.SourceInstanceId && instance.IsAlive &&
                               instance.AgentIsAlive && instance.IsReady && !instance.IsDraining &&
                               instance.Capabilities is not null &&
                               instance.Capabilities.Contains(ClusterCapabilities.NpcTransferV1, StringComparer.Ordinal) &&
                               (instance.SystemIds is { Length: > 0 } ids
                                   ? ids.Contains(request.TargetSystemId, StringComparer.OrdinalIgnoreCase)
                                   : string.Equals(instance.SystemId, request.TargetSystemId, StringComparison.OrdinalIgnoreCase)))
            .OrderBy(instance => instance.MaxPlayers > 0
                ? (double)(instance.CurrentPlayers + instance.ReservedPlayers) / instance.MaxPlayers
                : 1d)
            .ThenBy(instance => instance.InstanceId, StringComparer.Ordinal)
            .FirstOrDefault();
    }
}
