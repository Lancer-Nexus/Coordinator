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

    [MySqlFact]
    public async Task ReplaysOrderedEventsAndReportsGapsAndProducerEpochChanges()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            var store = new MySqlFlHookEventStore(connectionString);
            var epoch = Guid.NewGuid();
            var first = Event() with { ProducerEpoch = epoch, Sequence = 1 };
            var second = first with { Sequence = 2 };
            Assert.Equal(FlHookEventIngestStatusV1.Stored, await store.IngestAsync(first));
            Assert.Equal(FlHookEventIngestStatusV1.Stored, await store.IngestAsync(second));

            var page = await store.ReplayAsync(new FlHookEventReplayRequestV1
            {
                CorrelationId = Guid.NewGuid(),
                InstanceId = first.InstanceId,
                AfterSequence = 1,
                ProducerEpoch = epoch
            });
            Assert.True(page.IsValid());
            Assert.False(page.Gap);
            Assert.Equal([2L], page.Events.Select(x => x.Sequence));

            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM flhook_server_events WHERE instance_id = @instance AND producer_epoch = @epoch AND sequence_number = 1";
                command.Parameters.AddWithValue("@instance", first.InstanceId);
                command.Parameters.AddWithValue("@epoch", epoch.ToString("D"));
                await command.ExecuteNonQueryAsync();
            }
            var expiredPage = await store.ReplayAsync(new FlHookEventReplayRequestV1
            {
                CorrelationId = Guid.NewGuid(),
                InstanceId = first.InstanceId,
                AfterSequence = 0,
                ProducerEpoch = epoch
            });
            Assert.True(expiredPage.IsValid());
            Assert.True(expiredPage.Gap);
            Assert.Equal([2L], expiredPage.Events.Select(x => x.Sequence));

            var restartedEpoch = Guid.NewGuid();
            Assert.Equal(FlHookEventIngestStatusV1.Stored,
                await store.IngestAsync(Event() with { ProducerEpoch = restartedEpoch, Sequence = 1 }));
            var changedEpochPage = await store.ReplayAsync(new FlHookEventReplayRequestV1
            {
                CorrelationId = Guid.NewGuid(),
                InstanceId = first.InstanceId,
                AfterSequence = 2,
                ProducerEpoch = epoch
            });
            Assert.True(changedEpochPage.IsValid());
            Assert.True(changedEpochPage.Gap);
            Assert.Equal(restartedEpoch, changedEpochPage.ProducerEpoch);
            Assert.Equal([1L], changedEpochPage.Events.Select(x => x.Sequence));
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
            command.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
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
        InstanceId = "li-01",
        ProducerEpoch = Guid.NewGuid(),
        Sequence = 17,
        OccurredUtc = DateTimeOffset.UtcNow,
        Kind = FlHookEventKind.PlayerConnected,
        PlayerId = "player-01"
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
