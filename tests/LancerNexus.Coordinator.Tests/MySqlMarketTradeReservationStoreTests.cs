using LancerNexus.Coordinator;
using LancerNexus.Protocol;
using MySqlConnector;
using Xunit;

namespace LancerNexus.Coordinator.Tests;

public sealed class MySqlMarketTradeReservationStoreTests
{
    [MySqlFact]
    public async Task BuyReservationHoldsStockAndCommitReplayDoesNotApplyTwice()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeReservationStore(connectionString);
            var request = Reserve(MarketTradeSideV1.Buy, quantity: 5);

            var reserved = await store.ReserveAsync(request);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, reserved.Status);
            Assert.Equal(100, reserved.QuoteUnitPrice);
            Assert.Equal(559, reserved.TotalPrice);
            Assert.Equal(2, reserved.MarketRevision);
            Assert.Equal(2, reserved.SystemRevision);

            var snapshotRequest = new MarketSnapshotReadRequestV1
            {
                CorrelationId = Guid.NewGuid(),
                SystemId = "test_system"
            };
            var snapshot = await new MySqlMarketSnapshotStore(connectionString).ReadAsync(snapshotRequest);
            Assert.True(snapshot.IsValidFor(snapshotRequest));
            Assert.Equal(15, Assert.Single(snapshot.Goods).AvailableStock);
            Assert.Equal(133, snapshot.Goods[0].CurrentUnitPrice);

            var repeated = await store.ReserveAsync(request with { CorrelationId = Guid.NewGuid() });
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, repeated.Status);
            Assert.Equal(2, repeated.MarketRevision);
            Assert.NotEqual(request.CorrelationId, repeated.CorrelationId);
            Assert.Equal(MarketTradeReservationStatusV1.Conflict,
                (await store.ReserveAsync(request with { Quantity = 6 })).Status);

            var commit = Resolve(request.TradeId, MarketTradeResolutionV1.Commit);
            var committed = await store.ResolveAsync(commit);
            Assert.Equal(MarketTradeReservationStatusV1.Committed, committed.Status);
            Assert.Equal(3, committed.MarketRevision);
            var replayedCommit = await store.ResolveAsync(commit with { CorrelationId = Guid.NewGuid() });
            Assert.Equal(MarketTradeReservationStatusV1.Committed, replayedCommit.Status);
            Assert.Equal(committed.MarketRevision, replayedCommit.MarketRevision);
            Assert.Equal(MarketTradeReservationStatusV1.Conflict,
                (await store.ResolveAsync(Resolve(request.TradeId, MarketTradeResolutionV1.Abort))).Status);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT available_stock, reserved_buy_stock, current_unit_price, market_revision
                FROM market_good_state WHERE market_id = 'test_base' AND good_id = 'commodity_food'
                """;
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(15, reader.GetInt64(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(133, reader.GetInt64(2));
            Assert.Equal(3, reader.GetInt64(3));
        });
    }

    [MySqlFact]
    public async Task AbortReleasesReservedAvailabilityAndIsIdempotent()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var store = new MySqlMarketTradeReservationStore(connectionString);
            var request = Reserve(MarketTradeSideV1.Buy, quantity: 5);
            var reserved = await store.ReserveAsync(request);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, reserved.Status);
            Assert.Equal(100, reserved.QuoteUnitPrice);
            Assert.Equal(559, reserved.TotalPrice);

            var abort = Resolve(request.TradeId, MarketTradeResolutionV1.Abort);
            var aborted = await store.ResolveAsync(abort);
            Assert.Equal(MarketTradeReservationStatusV1.Aborted, aborted.Status);
            Assert.Equal(3, aborted.MarketRevision);
            var replayed = await store.ResolveAsync(abort with { CorrelationId = Guid.NewGuid() });
            Assert.Equal(MarketTradeReservationStatusV1.Aborted, replayed.Status);
            Assert.Equal(aborted.MarketRevision, replayed.MarketRevision);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT available_stock, reserved_buy_stock, current_unit_price FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(20, reader.GetInt64(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(100, reader.GetInt64(2));
        });
    }

    [MySqlFact]
    public async Task ConcurrentReservationsAcrossCoordinatorStoreInstancesCannotOverbookStock()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var firstCoordinatorStore = new MySqlMarketTradeReservationStore(connectionString);
            var secondCoordinatorStore = new MySqlMarketTradeReservationStore(connectionString);
            var first = Reserve(MarketTradeSideV1.Buy, quantity: 12);
            var second = Reserve(MarketTradeSideV1.Buy, quantity: 12);
            var results = await Task.WhenAll(
                firstCoordinatorStore.ReserveAsync(first),
                secondCoordinatorStore.ReserveAsync(second));
            Assert.Single(results, result => result.Status == MarketTradeReservationStatusV1.Reserved);
            Assert.Single(results, result => result.Status == MarketTradeReservationStatusV1.MarketRevisionMismatch);

            var snapshotRequest = new MarketSnapshotReadRequestV1
            {
                CorrelationId = Guid.NewGuid(),
                SystemId = "test_system"
            };
            var replicaReads = await Task.WhenAll(
                new MySqlMarketSnapshotStore(connectionString).ReadAsync(snapshotRequest),
                new MySqlMarketSnapshotStore(connectionString).ReadAsync(snapshotRequest));
            Assert.All(replicaReads, snapshot => Assert.True(snapshot.IsValidFor(snapshotRequest)));
            Assert.Equal(replicaReads[0].SystemRevision, replicaReads[1].SystemRevision);
            Assert.Equal(replicaReads[0].Goods, replicaReads[1].Goods);
            Assert.Equal(8, Assert.Single(replicaReads[0].Goods).AvailableStock);
            Assert.Equal(8, Assert.Single(replicaReads[1].Goods).AvailableStock);

            var competingTrade = new MarketTradeEventV1
            {
                EventId = Guid.NewGuid(),
                InstanceId = "instance_a",
                ProducerEpoch = Guid.NewGuid(),
                Sequence = 1,
                OccurredUtc = DateTimeOffset.UtcNow,
                MarketId = "test_base",
                SystemId = "test_system",
                GoodId = "commodity_food",
                Side = MarketTradeSideV1.Buy,
                Quantity = 9
            };
            Assert.Equal(MarketTradeIngestStatusV1.Rejected,
                (await new MySqlMarketTradeStore(connectionString).ApplyAsync(competingTrade)).Status);

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT available_stock, reserved_buy_stock FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(20, reader.GetInt64(0));
            Assert.Equal(12, reader.GetInt64(1));
        });
    }

    [MySqlFact]
    public async Task SaleReservationAddsStockOnlyOnCommit()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "UPDATE market_good_state SET sell_enabled = TRUE, max_stock = 25 WHERE market_id='test_base'";
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlMarketTradeReservationStore(connectionString);
            var request = Reserve(MarketTradeSideV1.Sell, quantity: 5);
            var reserved = await store.ReserveAsync(request);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, reserved.Status);
            Assert.Equal(100, reserved.QuoteUnitPrice);
            Assert.Equal(456, reserved.TotalPrice);
            var overCapacity = await store.ReserveAsync(Reserve(MarketTradeSideV1.Sell, quantity: 1) with
            {
                ExpectedMarketRevision = reserved.MarketRevision
            });
            Assert.Equal(MarketTradeReservationStatusV1.Rejected, overCapacity.Status);
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT available_stock, reserved_sell_stock FROM market_good_state WHERE market_id='test_base'";
                await using var stateReader = await command.ExecuteReaderAsync();
                Assert.True(await stateReader.ReadAsync());
                Assert.Equal(20, stateReader.GetInt64(0));
                Assert.Equal(5, stateReader.GetInt64(1));
            }

            Assert.Equal(MarketTradeReservationStatusV1.Committed,
                (await store.ResolveAsync(Resolve(request.TradeId, MarketTradeResolutionV1.Commit))).Status);
            await using var verify = new MySqlConnection(connectionString);
            await verify.OpenAsync();
            await using var query = verify.CreateCommand();
            query.CommandText = "SELECT available_stock, reserved_sell_stock, current_unit_price FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await query.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(25, reader.GetInt64(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(80, reader.GetInt64(2));
        });
    }

    [MySqlFact]
    public async Task PersonalQuoteAppliesMarketFactionReputationWithoutReservingStock()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var now = DateTimeOffset.UtcNow;
            var request = new MarketQuoteRequestV1
            {
                CorrelationId = Guid.NewGuid(), CharacterId = 77, SystemId = "test_system",
                MarketId = "test_base", GoodId = "commodity_food", Side = MarketTradeSideV1.Buy,
                Quantity = 5, ExpectedMarketRevision = 1, ExpectedReputationRevision = 3
            };
            var reputationRequest = new MarketFactionReputationReadRequestV1
            {
                CorrelationId = request.CorrelationId,
                CharacterId = request.CharacterId,
                ExpectedReputationRevision = request.ExpectedReputationRevision
            };
            var reputation = new MarketFactionReputationReadResponseV1
            {
                CorrelationId = request.CorrelationId,
                CharacterId = request.CharacterId,
                Status = MarketFactionReputationStatusV1.Found,
                ReputationRevision = 3,
                Factions = [new MarketFactionReputationV1 { FactionId = "test_faction", Value = 0.5f }]
            };
            Assert.True(reputation.IsValidFor(reputationRequest));

            var store = new MySqlMarketQuoteStore(connectionString);
            var quote = await store.ReadAsync(request, reputation, now);
            Assert.True(quote.IsValidFor(request));
            Assert.Equal(MarketQuoteStatusV1.Quoted, quote.Status);
            Assert.Equal("test_faction", quote.MarketFactionId);
            Assert.Equal(0.5f, quote.AppliedReputation);
            Assert.Equal(950_000, quote.ReputationFactorPartsPerMillion);
            Assert.Equal(95, quote.QuoteUnitPrice);
            Assert.Equal(531, quote.TotalPrice);
            Assert.Equal(1, quote.MarketRevision);
            Assert.Equal(3, quote.ReputationRevision);
            Assert.Equal(now.AddSeconds(15), quote.ExpiresUtc);

            var staleMarketRequest = request with { ExpectedMarketRevision = 2 };
            var staleMarket = await store.ReadAsync(staleMarketRequest, reputation, now);
            Assert.Equal(MarketQuoteStatusV1.MarketRevisionMismatch, staleMarket.Status);
            Assert.True(staleMarket.IsValidFor(staleMarketRequest));

            var staleReputation = reputation with
            {
                Status = MarketFactionReputationStatusV1.RevisionMismatch,
                ReputationRevision = 4,
                Factions = []
            };
            var staleReputationQuote = await store.ReadAsync(request, staleReputation, now);
            Assert.Equal(MarketQuoteStatusV1.ReputationRevisionMismatch, staleReputationQuote.Status);
            Assert.True(staleReputationQuote.IsValidFor(request));

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT available_stock, reserved_buy_stock, market_revision FROM market_good_state WHERE market_id='test_base'";
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(20, reader.GetInt64(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(1, reader.GetInt64(2));
        });
    }

    [MySqlFact]
    public async Task QuoteBoundReservationRecomputesPriceAndPinsReputationInputs()
    {
        await WithDatabaseAsync(async connectionString =>
        {
            await ApplySchemaAsync(connectionString);
            await SeedMarketAsync(connectionString);
            var correlationId = Guid.NewGuid();
            var quoteRequest = new MarketQuoteRequestV1
            {
                CorrelationId = correlationId, CharacterId = 77, SystemId = "test_system",
                MarketId = "test_base", GoodId = "commodity_food", Side = MarketTradeSideV1.Buy,
                Quantity = 3, ExpectedMarketRevision = 1, ExpectedReputationRevision = 8
            };
            var reputationRequest = new MarketFactionReputationReadRequestV1
            {
                CorrelationId = correlationId, CharacterId = 77, ExpectedReputationRevision = 8
            };
            var reputation = new MarketFactionReputationReadResponseV1
            {
                CorrelationId = correlationId, CharacterId = 77,
                Status = MarketFactionReputationStatusV1.Found, ReputationRevision = 8,
                Factions = [new MarketFactionReputationV1 { FactionId = "test_faction", Value = 0.5f }]
            };
            var quote = await new MySqlMarketQuoteStore(connectionString).ReadAsync(
                quoteRequest, reputation, DateTimeOffset.UtcNow);
            Assert.Equal(MarketQuoteStatusV1.Quoted, quote.Status);

            var request = new MarketTradeQuoteReservationRequestV1
            {
                CorrelationId = Guid.NewGuid(), TradeId = Guid.NewGuid(),
                MarketId = quoteRequest.MarketId, SystemId = quoteRequest.SystemId,
                GoodId = quoteRequest.GoodId, Side = quoteRequest.Side, Quantity = quoteRequest.Quantity,
                ExpectedMarketRevision = quote.MarketRevision,
                ExpectedReputationRevision = quote.ReputationRevision,
                ReputationPriceFactorPartsPerMillion = quote.ReputationFactorPartsPerMillion,
                ExpectedQuoteUnitPrice = quote.QuoteUnitPrice, ExpectedTotalPrice = quote.TotalPrice,
                CharacterId = quoteRequest.CharacterId, MarketFactionId = quote.MarketFactionId!
            };
            var reservationReputation = reputation with { CorrelationId = request.CorrelationId };
            var store = new MySqlMarketTradeReservationStore(connectionString);
            var mismatchedPrice = await store.ReserveQuoteAsync(request with
            {
                ExpectedTotalPrice = request.ExpectedTotalPrice + 1
            }, reservationReputation);
            Assert.Equal(MarketTradeReservationStatusV1.Rejected, mismatchedPrice.Status);
            Assert.True(mismatchedPrice.IsValid());
            Assert.True(mismatchedPrice.Matches(request with { ExpectedTotalPrice = request.ExpectedTotalPrice + 1 }));
            var wrongFaction = await store.ReserveQuoteAsync(request with
            {
                MarketFactionId = "other_faction",
                ReputationPriceFactorPartsPerMillion = 1_000_000
            }, reservationReputation);
            Assert.Equal(MarketTradeReservationStatusV1.Rejected, wrongFaction.Status);

            var reserved = await store.ReserveQuoteAsync(request, reservationReputation);
            Assert.Equal(MarketTradeReservationStatusV1.Reserved, reserved.Status);
            Assert.Equal(quote.QuoteUnitPrice, reserved.QuoteUnitPrice);
            Assert.Equal(quote.TotalPrice, reserved.TotalPrice);
            Assert.Equal(quote.ReputationRevision, reserved.ReputationRevision);
            Assert.Equal(quote.ReputationFactorPartsPerMillion, reserved.ReputationPriceFactorPartsPerMillion);
            Assert.True(reserved.Matches(request));
            Assert.Equal(MarketTradeReservationStatusV1.Conflict,
                (await store.ReserveQuoteAsync(request with
                {
                    ExpectedTotalPrice = request.ExpectedTotalPrice + 1
                }, reservationReputation)).Status);
            var retry = request with { CorrelationId = Guid.NewGuid() };
            Assert.Equal(MarketTradeReservationStatusV1.Reserved,
                (await store.ReserveQuoteAsync(retry,
                    reservationReputation with { CorrelationId = retry.CorrelationId })).Status);
            var staleRequest = request with { ExpectedReputationRevision = 9 };
            var staleReputation = new MarketFactionReputationReadResponseV1
            {
                CorrelationId = staleRequest.CorrelationId,
                CharacterId = staleRequest.CharacterId,
                Status = MarketFactionReputationStatusV1.RevisionMismatch,
                ReputationRevision = 10
            };
            var stale = await store.ReserveQuoteAsync(staleRequest, staleReputation);
            Assert.Equal(MarketTradeReservationStatusV1.ReputationRevisionMismatch, stale.Status);
            Assert.Equal(10, stale.ReputationRevision);
            Assert.True(stale.Matches(staleRequest));

            await using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT reputation_revision, reputation_price_factor_parts_per_million, market_faction_id FROM market_trade_reservations WHERE trade_id=@trade";
            command.Parameters.AddWithValue("@trade", request.TradeId.ToByteArray());
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(8, reader.GetInt64(0));
            Assert.Equal(quote.ReputationFactorPartsPerMillion, reader.GetInt64(1));
            Assert.Equal(quote.MarketFactionId, reader.GetString(2));
        });
    }

    private static MarketTradeReservationRequestV1 Reserve(MarketTradeSideV1 side, long quantity) => new()
    {
        CorrelationId = Guid.NewGuid(),
        TradeId = Guid.NewGuid(),
        MarketId = "test_base",
        SystemId = "test_system",
        GoodId = "commodity_food",
        Side = side,
        Quantity = quantity,
        ExpectedMarketRevision = 1
    };

    private static MarketTradeResolutionRequestV1 Resolve(Guid tradeId, MarketTradeResolutionV1 resolution) => new()
    {
        CorrelationId = Guid.NewGuid(),
        TradeId = tradeId,
        Resolution = resolution
    };

    private static async Task SeedMarketAsync(string connectionString)
    {
        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync();
        var statements = new[]
        {
            "INSERT INTO market_system_revisions (system_id, system_revision) VALUES ('test_system', 1)",
            "INSERT INTO market_catalog_goods (good_id, category, base_unit_price) VALUES ('commodity_food', 'commodity', 100)",
            "INSERT INTO market_locations (market_id, system_id, local_faction_id) VALUES ('test_base', 'test_system', 'test_faction')",
            "INSERT INTO market_static_offers (market_id, good_id, baseline_unit_price, buy_enabled, rank_requirement, min_amount, max_amount, reputation_threshold, source_file) VALUES ('test_base', 'commodity_food', 100, TRUE, 0, 0, 1, -1, 'test.ini')",
            "INSERT INTO market_good_state (market_id, good_id, system_id, base_unit_price, current_unit_price, available_stock, target_stock, buy_enabled, sell_enabled, market_revision) VALUES ('test_base', 'commodity_food', 'test_system', 100, 100, 20, 20, TRUE, FALSE, 1)"
        };
        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    private static async Task ApplySchemaAsync(string connectionString)
    {
        foreach (var filename in new[]
                 {
                     "006_dynamic_market_projection.sql",
                     "007_market_catalog_seed.sql",
                     "008_market_trade_events.sql",
                     "009_market_trade_reservations.sql",
                     "010_market_stock_policy.sql",
                     "011_market_quote_bound_reservations.sql",
                     "012_market_reservation_faction.sql"
                 })
        {
            var path = Path.Combine(AppContext.BaseDirectory, "db", "migrations", filename);
            var statements = (await File.ReadAllTextAsync(path))
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
    }

    private static async Task WithDatabaseAsync(Func<string, Task> action)
    {
        var configured = Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")!;
        var databaseName = "ln_market_reserve_" + Guid.NewGuid().ToString("N");
        var admin = new MySqlConnectionStringBuilder(configured) { Database = "information_schema" };
        await using (var connection = new MySqlConnection(admin.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"CREATE DATABASE `{databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
            await command.ExecuteNonQueryAsync();
        }
        var isolated = new MySqlConnectionStringBuilder(configured) { Database = databaseName };
        try { await action(isolated.ConnectionString); }
        finally
        {
            await using var connection = new MySqlConnection(admin.ConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP DATABASE `{databaseName}`";
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed class MySqlFactAttribute : FactAttribute
    {
        public MySqlFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("LANCER_NEXUS_COORDINATOR_TEST_MYSQL")))
                Skip = "Set LANCER_NEXUS_COORDINATOR_TEST_MYSQL to an isolated MySQL test server.";
        }
    }
}
