using System.Text.Json;
using LancerNexus.Protocol;

namespace LancerNexus.Coordinator;

public sealed record PersistedAgent(AgentHeartbeat Heartbeat, DateTimeOffset LastHeartbeatUtc);
public sealed record PersistedInstance(InstanceHeartbeat Heartbeat, DateTimeOffset LastHeartbeatUtc);
public sealed record PersistedReservation(
    Guid SessionId,
    string TargetSystem,
    string? GroupId,
    string InstanceId,
    PlacementDecision Decision,
    DateTimeOffset ExpiresUtc,
    string IdempotencyKey);
public sealed record PersistedGroupAffinity(string SystemId, string InstanceId, DateTimeOffset ExpiresUtc, string GroupId);

public sealed record CoordinatorRegistryState(
    int SchemaVersion,
    PersistedAgent[] Agents,
    PersistedInstance[] Instances,
    PersistedReservation[] Reservations,
    PersistedGroupAffinity[] GroupAffinities)
{
    public const int CurrentSchemaVersion = 1;

    public static CoordinatorRegistryState Empty { get; } = new(
        CurrentSchemaVersion, [], [], [], []);
}

/// <summary>
/// Snapshot persistence boundary. Implementations used by multiple Coordinator replicas
/// must provide transactional, cross-process coordination; the file implementation is single-writer.
/// </summary>
public interface ICoordinatorRegistryStore
{
    CoordinatorRegistryState Load();
    void Save(CoordinatorRegistryState state);
}

public sealed class InMemoryCoordinatorRegistryStore : ICoordinatorRegistryStore
{
    private CoordinatorRegistryState state = CoordinatorRegistryState.Empty;

    public CoordinatorRegistryState Load() => state;

    public void Save(CoordinatorRegistryState newState) => state = newState;
}

/// <summary>
/// Persists registry snapshots using an atomic same-filesystem rename. This supports recovery
/// after process restart, but intentionally does not claim multi-process or multi-replica safety.
/// </summary>
public sealed class FileCoordinatorRegistryStore : ICoordinatorRegistryStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly object sync = new();
    private readonly string filePath;

    public FileCoordinatorRegistryStore(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        this.filePath = Path.GetFullPath(filePath);
    }

    public CoordinatorRegistryState Load()
    {
        lock (sync)
        {
            if (!File.Exists(filePath))
                return CoordinatorRegistryState.Empty;

            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var state = JsonSerializer.Deserialize<CoordinatorRegistryState>(stream, JsonOptions)
                ?? throw new InvalidDataException("Coordinator registry state file is empty or invalid.");
            if (state.SchemaVersion != CoordinatorRegistryState.CurrentSchemaVersion)
                throw new InvalidDataException($"Unsupported Coordinator registry schema version {state.SchemaVersion}.");
            return state;
        }
    }

    public void Save(CoordinatorRegistryState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        if (state.SchemaVersion != CoordinatorRegistryState.CurrentSchemaVersion)
            throw new InvalidDataException($"Unsupported Coordinator registry schema version {state.SchemaVersion}.");

        lock (sync)
        {
            var directory = Path.GetDirectoryName(filePath)
                ?? throw new InvalidOperationException("Coordinator state path must have a parent directory.");
            Directory.CreateDirectory(directory);
            var temporaryPath = Path.Combine(directory, $".{Path.GetFileName(filePath)}.{Guid.NewGuid():N}.tmp");
            try
            {
                using (var stream = new FileStream(
                           temporaryPath,
                           FileMode.CreateNew,
                           FileAccess.Write,
                           FileShare.None,
                           16 * 1024,
                           FileOptions.WriteThrough))
                {
                    JsonSerializer.Serialize(stream, state, JsonOptions);
                    stream.Flush(flushToDisk: true);
                }

                File.Move(temporaryPath, filePath, overwrite: true);
            }
            finally
            {
                if (File.Exists(temporaryPath))
                    File.Delete(temporaryPath);
            }
        }
    }
}
