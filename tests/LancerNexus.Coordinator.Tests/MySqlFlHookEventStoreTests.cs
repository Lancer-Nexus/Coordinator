using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlFlHookEventStoreTests
{
    [Fact]
    public async Task MissingConfigurationIsUnavailableAndMalformedEventsAreRejected()
    {
        Assert.Equal(FlHookEventIngestStatusV1.Unavailable,
            await new MySqlFlHookEventStore(null).IngestAsync(Event()));
        Assert.Equal(FlHookEventIngestStatusV1.Rejected,
            await new MySqlFlHookEventStore("configured").IngestAsync(Event() with { ProducerEpoch = Guid.Empty }));
    }

    [MySqlFact]
    public async Task StoresIdenticalRetriesAndRejectsConflictingPayloadForSameIdentity()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            var store = new MySqlFlHookEventStore(connectionString);
            var value = Event();

            var concurrent = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ =>
                new MySqlFlHookEventStore(connectionString).IngestAsync(value)));
            Assert.Single(concurrent, status => status == FlHookEventIngestStatusV1.Stored);
            Assert.True(concurrent.Count(status => status == FlHookEventIngestStatusV1.Duplicate) == 7,
                $"Outcomes: {string.Join(',', concurrent)}");
            Assert.Equal(FlHookEventIngestStatusV1.Conflict,
                await store.IngestAsync(value with { PlayerId = "other-player" }));
        });
    }

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var databaseName = "ln_flhook_" + Guid.NewGuid().ToString("N");
        var admin = new MySqlConnectionStringBuilder(configured) { Database = "information_schema" };
        await using (var connection = new MySqlConnection(admin.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci";
            await command.ExecuteNonQueryAsync();
        }

        var isolated = new MySqlConnectionStringBuilder(configured) { Database = databaseName };
        try
        {
            await action(isolated.ConnectionString);
        }
        finally
        {
            await using var connection = new MySqlConnection(admin.ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP DATABASE `{databaseName}`";
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task ApplySchemaAsync(string connectionString)
    {
        var migration = Path.Combine(AppContext.BaseDirectory, "db", "migrations", "005_flhook_server_events.sql");
        var statements = (await File.ReadAllTextAsync(migration))
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static FlHookEventV1 Event() => new()
    {
        InstanceId = "li-01", ProducerEpoch = Guid.NewGuid(), Sequence = 17,
        OccurredUtc = DateTimeOffset.UtcNow, Kind = FlHookEventKind.PlayerConnected, PlayerId = "player-01"
    };

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }
}
