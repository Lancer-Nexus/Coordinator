using System.Data;
using System.Security.Cryptography;
using MySqlConnector;

namespace LancerNexus.Coordinator;

public enum MarketCatalogSeedImportResult
{
    Imported,
    AlreadyImported
}

/// <summary>Imports one immutable catalog/offers snapshot without inventing live inventory.</summary>
public sealed class MarketCatalogSeedImporter(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public async Task<MarketCatalogSeedImportResult> ImportAsync(MarketCatalogSeedDocument seed,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(seed);
        if (connectionString is null)
            throw new InvalidOperationException("Market database connection is not configured.");

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        byte[]? existingHash;
        int existingGoodCount;
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "SELECT seed_sha256, good_count FROM market_catalog_seed WHERE seed_slot = 1 FOR UPDATE";
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                throw new InvalidOperationException("Market seed marker is missing; apply migration 007 first.");
            existingHash = reader.IsDBNull(0) ? null : (byte[])reader[0];
            existingGoodCount = reader.GetInt32(1);
        }

        if (existingHash is not null)
        {
            if (!CryptographicOperations.FixedTimeEquals(existingHash, seed.SourceSha256))
                throw new InvalidOperationException("A different market catalog seed is already installed; catalog replacement needs an explicit migration.");
            await transaction.CommitAsync(cancellationToken);
            return MarketCatalogSeedImportResult.AlreadyImported;
        }

        if (existingGoodCount != 0 || await CountAsync(connection, transaction, "market_catalog_goods", cancellationToken) != 0 ||
            await CountAsync(connection, transaction, "market_locations", cancellationToken) != 0 ||
            await CountAsync(connection, transaction, "market_static_offers", cancellationToken) != 0 ||
            await CountAsync(connection, transaction, "market_system_revisions", cancellationToken) != 0)
            throw new InvalidOperationException("Market tables contain data without a seed marker; refusing to mix or replace market state.");

        await InsertBatchesAsync(connection, transaction, "market_system_revisions",
            ["system_id", "system_revision"], seed.Systems.Select(system => new object?[] { system.SystemId, 1L }),
            500, cancellationToken);
        await InsertBatchesAsync(connection, transaction, "market_catalog_goods",
            ["good_id", "category", "base_unit_price"],
            seed.Goods.Select(good => new object?[] { good.GoodId, good.Category, good.BaseUnitPrice }),
            300, cancellationToken);
        await InsertBatchesAsync(connection, transaction, "market_locations",
            ["market_id", "system_id", "local_faction_id"],
            seed.Markets.Select(market => new object?[] { market.MarketId, market.SystemId, market.LocalFactionId }),
            300, cancellationToken);
        await InsertBatchesAsync(connection, transaction, "market_static_offers",
            ["market_id", "good_id", "baseline_unit_price", "buy_enabled", "rank_requirement",
                "min_amount", "max_amount", "reputation_threshold", "source_file"],
            seed.Offers.Select(offer => new object?[] { offer.MarketId, offer.GoodId, offer.BaselineUnitPrice,
                offer.BuyEnabled, offer.RankRequirement, offer.MinAmount, offer.MaxAmount,
                offer.ReputationThreshold, offer.SourceFile }), 200, cancellationToken);

        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE market_catalog_seed
                SET seed_sha256 = @hash, imported_utc = UTC_TIMESTAMP(6), good_count = @goods,
                    system_count = @systems, market_count = @markets, offer_count = @offers
                WHERE seed_slot = 1 AND seed_sha256 IS NULL
                """;
            command.Parameters.AddWithValue("@hash", seed.SourceSha256);
            command.Parameters.AddWithValue("@goods", seed.Goods.Length);
            command.Parameters.AddWithValue("@systems", seed.Systems.Length);
            command.Parameters.AddWithValue("@markets", seed.Markets.Length);
            command.Parameters.AddWithValue("@offers", seed.Offers.Length);
            if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new InvalidOperationException("Market seed marker changed during import.");
        }

        await transaction.CommitAsync(cancellationToken);
        return MarketCatalogSeedImportResult.Imported;
    }

    private static async Task<long> CountAsync(MySqlConnection connection, MySqlTransaction transaction,
        string table, CancellationToken cancellationToken)
    {
        // Table names are fixed by this importer and are not caller input.
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task InsertBatchesAsync(MySqlConnection connection, MySqlTransaction transaction,
        string table, string[] columns, IEnumerable<object?[]> sourceRows, int batchSize,
        CancellationToken cancellationToken)
    {
        var batch = new List<object?[]>(batchSize);
        foreach (var row in sourceRows)
        {
            batch.Add(row);
            if (batch.Count == batchSize)
            {
                await InsertBatchAsync(connection, transaction, table, columns, batch, cancellationToken);
                batch.Clear();
            }
        }
        if (batch.Count > 0)
            await InsertBatchAsync(connection, transaction, table, columns, batch, cancellationToken);
    }

    private static async Task InsertBatchAsync(MySqlConnection connection, MySqlTransaction transaction,
        string table, string[] columns, IReadOnlyList<object?[]> rows, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        var values = new string[rows.Count];
        for (var rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            var placeholders = new string[columns.Length];
            for (var columnIndex = 0; columnIndex < columns.Length; columnIndex++)
            {
                var name = $"@p{rowIndex}_{columnIndex}";
                placeholders[columnIndex] = name;
                command.Parameters.AddWithValue(name, rows[rowIndex][columnIndex] ?? DBNull.Value);
            }
            values[rowIndex] = $"({string.Join(",", placeholders)})";
        }
        command.CommandText = $"INSERT INTO {table} ({string.Join(",", columns)}) VALUES {string.Join(",", values)}";
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
