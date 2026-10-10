using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public interface IFlHookEventStore
{
    bool IsEnabled { get; }
    Task<FlHookEventIngestStatusV1> IngestAsync(FlHookEventV1 value, CancellationToken cancellationToken = default);
}
