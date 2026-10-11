using System.Data;
using System.Security.Cryptography;
using MySqlConnector;

namespace LancerNexus.Coordinator;

public enum MarketStockPolicyImportResult
{
    Imported,
    AlreadyImported
}

/// <summary>Installs a report-pinned market allowlist and initial state atomically.</summary>
public sealed class MarketStockPolicyImporter(string? connectionString)
{
    private readonly string? connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    public async Task<MarketStockPolicyImportResult> ImportAsync(MarketCatalogSeedDocument seed,
        MarketStockPolicyDocument policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(seed);
        ArgumentNullException.ThrowIfNull(policy);
        if (connectionString is null)
            throw new InvalidOperationException("Market database connection is not configured.");
        if (seed.SourceSha256.Length != 32 || policy.CatalogSeedSha256.Length != 32 ||
            !CryptographicOperations.FixedTimeEquals(seed.SourceSha256, policy.CatalogSeedSha256.Span) ||
            policy.PolicySha256.Length != 32 || policy.Entries.Count == 0)
            throw new InvalidDataException("Market stock policy is not bound to the supplied catalog seed.");

        var marketSystems = seed.Markets.ToDictionary(market => market.MarketId, market => market.SystemId,
            StringComparer.Ordinal);
        var knownGoods = seed.Goods.Select(good => good.GoodId).ToHashSet(StringComparer.Ordinal);
        var keys = new HashSet<(string MarketId, string GoodId)>();
        foreach (var entry in policy.Entries)
        {
            if (!marketSystems.ContainsKey(entry.MarketId) || !knownGoods.Contains(entry.GoodId) ||
                !keys.Add((entry.MarketId, entry.GoodId)) ||
                entry.BaseUnitPrice < 0 || entry.TargetStock <= 0 || entry.InitialStock < 0 ||
                entry.MaxStock < entry.TargetStock || entry.MaxStock < entry.InitialStock ||
                entry.MinUnitPrice < 0 || entry.MinUnitPrice > entry.BaseUnitPrice ||
                entry.MaxUnitPrice < entry.BaseUnitPrice || entry.PricingPolicyVersion != 1 ||
                !entry.BuyEnabled && !entry.SellEnabled)
                throw new InvalidDataException($"Invalid market stock policy entry '{entry.MarketId}/{entry.GoodId}'.");
            if (entry.BuyEnabled && entry.InitialStock == 0 ||
                entry.SellEnabled && entry.InitialStock >= entry.MaxStock)
                throw new InvalidDataException($"Initial stock cannot support the configured trade sides for '{entry.MarketId}/{entry.GoodId}'.");
            _ = MarketPriceCalculator.Calculate(entry.BaseUnitPrice, entry.TargetStock, entry.InitialStock,
                entry.MinUnitPrice, entry.MaxUnitPrice);
        }

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted,
            cancellationToken);

        var installedCatalogHash = await ReadCatalogHashAsync(connection, transaction, cancellationToken);
        if (installedCatalogHash is null || installedCatalogHash.Length != 32 ||
            !CryptographicOperations.FixedTimeEquals(installedCatalogHash, seed.SourceSha256))
            throw new InvalidOperationException("Installed market catalog does not match this stock policy report.");

        var installation = await LockInstallationAsync(connection, transaction, cancellationToken);
        if (installation is null)
            throw new InvalidOperationException("Market stock policy marker is missing; apply migration 010 first.");
        if (installation.PolicySha256 is not null)
        {
            if (installation.PolicyRevision == policy.PolicyRevision &&
                installation.PolicyVersion == policy.PolicyVersion &&
                installation.CatalogSha256 is not null &&
                CryptographicOperations.FixedTimeEquals(installation.CatalogSha256, seed.SourceSha256) &&
                CryptographicOperations.FixedTimeEquals(installation.PolicySha256, policy.PolicySha256.Span))
            {
                await transaction.CommitAsync(cancellationToken);
                return MarketStockPolicyImportResult.AlreadyImported;
            }
            throw new InvalidOperationException("A different market stock policy is already installed; policy replacement needs an explicit migration.");
        }

        if (installation.CatalogSha256 is not null &&
            !CryptographicOperations.FixedTimeEquals(installation.CatalogSha256, seed.SourceSha256))
            throw new InvalidOperationException("Market stock policy marker is bound to another catalog report.");
        if (await CountAsync(connection, transaction, "market_good_state", cancellationToken) != 0 ||
            await CountAsync(connection, transaction, "market_stock_policy_entries", cancellationToken) != 0)
            throw new InvalidOperationException("Live market state exists without an installed stock policy; refusing to overwrite it.");

        var systems = policy.Entries.Select(entry => marketSystems[entry.MarketId])
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        foreach (var systemId in systems)
        {
            var revision = await LockSystemRevisionAsync(connection, transaction, systemId, cancellationToken);
            if (revision is null || revision == long.MaxValue)
                throw new InvalidOperationException($"Market system '{systemId}' is missing or its revision is exhausted.");
        }

        await InsertStateBatchesAsync(connection, transaction, policy.Entries, marketSystems,
            policy.PolicyRevision, cancellationToken);
        await InsertPolicyEntryBatchesAsync(connection, transaction, policy.Entries,
            policy.PolicyRevision, cancellationToken);

        foreach (var systemId in systems)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "UPDATE market_system_revisions SET system_revision = system_revision + 1 WHERE system_id = @system";
            command.Parameters.AddWithValue("@system", systemId);
            if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new InvalidOperationException($"Market system '{systemId}' disappeared during policy import.");
        }

        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = """
                UPDATE market_stock_policy_installation
                SET catalog_seed_sha256 = @catalogHash, policy_sha256 = @policyHash,
                    policy_revision = @revision, policy_version = @version,
                    imported_utc = UTC_TIMESTAMP(6), entry_count = @entries
                WHERE policy_slot = 1 AND policy_sha256 IS NULL
                """;
            command.Parameters.AddWithValue("@catalogHash", seed.SourceSha256);
            command.Parameters.AddWithValue("@policyHash", policy.PolicySha256.ToArray());
            command.Parameters.AddWithValue("@revision", policy.PolicyRevision);
            command.Parameters.AddWithValue("@version", policy.PolicyVersion);
            command.Parameters.AddWithValue("@entries", policy.Entries.Count);
            if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
                throw new InvalidOperationException("Market stock policy marker changed during import.");
        }

        await transaction.CommitAsync(cancellationToken);
        return MarketStockPolicyImportResult.Imported;
    }

    private static async Task<byte[]?> ReadCatalogHashAsync(MySqlConnection connection,
        MySqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT seed_sha256 FROM market_catalog_seed WHERE seed_slot = 1 FOR UPDATE";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is byte[] bytes ? bytes : null;
    }

    private static async Task<PolicyInstallation?> LockInstallationAsync(MySqlConnection connection,
        MySqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT catalog_seed_sha256, policy_sha256, policy_revision, policy_version FROM market_stock_policy_installation WHERE policy_slot = 1 FOR UPDATE";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new PolicyInstallation(
            reader.IsDBNull(0) ? null : (byte[])reader[0],
            reader.IsDBNull(1) ? null : (byte[])reader[1],
            reader.IsDBNull(2) ? null : reader.GetInt64(2),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    private static async Task<long?> LockSystemRevisionAsync(MySqlConnection connection,
        MySqlTransaction transaction, string systemId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT system_revision FROM market_system_revisions WHERE system_id = @system FOR UPDATE";
        command.Parameters.AddWithValue("@system", systemId);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull ? null : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<long> CountAsync(MySqlConnection connection, MySqlTransaction transaction,
        string table, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task InsertStateBatchesAsync(MySqlConnection connection, MySqlTransaction transaction,
        IReadOnlyList<MarketStockPolicyEntry> entries, IReadOnlyDictionary<string, string> marketSystems,
        long policyRevision, CancellationToken cancellationToken)
    {
        const int batchSize = 100;
        for (var offset = 0; offset < entries.Count; offset += batchSize)
        {
            var batch = entries.Skip(offset).Take(batchSize).ToArray();
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            var rows = new string[batch.Length];
            for (var index = 0; index < batch.Length; index++)
            {
                var entry = batch[index];
                var prefix = $"@r{index}_";
                rows[index] = $"({prefix}market,{prefix}good,{prefix}system,{prefix}base,{prefix}price,{prefix}stock,0,0,{prefix}target,{prefix}max,{prefix}minPrice,{prefix}maxPrice,{prefix}pricingVersion,{prefix}policyRevision,{prefix}faction,{prefix}buy,{prefix}sell,1)";
                command.Parameters.AddWithValue(prefix + "market", entry.MarketId);
                command.Parameters.AddWithValue(prefix + "good", entry.GoodId);
                command.Parameters.AddWithValue(prefix + "system", marketSystems[entry.MarketId]);
                command.Parameters.AddWithValue(prefix + "base", entry.BaseUnitPrice);
                command.Parameters.AddWithValue(prefix + "price", MarketPriceCalculator.Calculate(entry.BaseUnitPrice,
                    entry.TargetStock, entry.InitialStock, entry.MinUnitPrice, entry.MaxUnitPrice));
                command.Parameters.AddWithValue(prefix + "stock", entry.InitialStock);
                command.Parameters.AddWithValue(prefix + "target", entry.TargetStock);
                command.Parameters.AddWithValue(prefix + "max", entry.MaxStock);
                command.Parameters.AddWithValue(prefix + "minPrice", entry.MinUnitPrice);
                command.Parameters.AddWithValue(prefix + "maxPrice", entry.MaxUnitPrice);
                command.Parameters.AddWithValue(prefix + "pricingVersion", entry.PricingPolicyVersion);
                command.Parameters.AddWithValue(prefix + "policyRevision", policyRevision);
                command.Parameters.AddWithValue(prefix + "faction", (object?)entry.MarketFactionOverrideId ?? DBNull.Value);
                command.Parameters.AddWithValue(prefix + "buy", entry.BuyEnabled);
                command.Parameters.AddWithValue(prefix + "sell", entry.SellEnabled);
            }
            command.CommandText = $"""
                INSERT INTO market_good_state
                    (market_id, good_id, system_id, base_unit_price, current_unit_price,
                     available_stock, reserved_buy_stock, reserved_sell_stock, target_stock,
                     max_stock, min_unit_price, max_unit_price, pricing_policy_version,
                     policy_revision, market_faction_override_id, buy_enabled, sell_enabled, market_revision)
                VALUES {string.Join(",", rows)}
                """;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task InsertPolicyEntryBatchesAsync(MySqlConnection connection,
        MySqlTransaction transaction, IReadOnlyList<MarketStockPolicyEntry> entries,
        long policyRevision, CancellationToken cancellationToken)
    {
        const int batchSize = 200;
        for (var offset = 0; offset < entries.Count; offset += batchSize)
        {
            var batch = entries.Skip(offset).Take(batchSize).ToArray();
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            var rows = new string[batch.Length];
            for (var index = 0; index < batch.Length; index++)
            {
                var entry = batch[index];
                var prefix = $"@r{index}_";
                rows[index] = $"({prefix}market,{prefix}good,{prefix}revision,{prefix}review)";
                command.Parameters.AddWithValue(prefix + "market", entry.MarketId);
                command.Parameters.AddWithValue(prefix + "good", entry.GoodId);
                command.Parameters.AddWithValue(prefix + "revision", policyRevision);
                command.Parameters.AddWithValue(prefix + "review", (object?)entry.ReviewReason ?? DBNull.Value);
            }
            command.CommandText = $"""
                INSERT INTO market_stock_policy_entries (market_id, good_id, policy_revision, review_reason)
                VALUES {string.Join(",", rows)}
                """;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private sealed record PolicyInstallation(byte[]? CatalogSha256, byte[]? PolicySha256,
        long? PolicyRevision, string? PolicyVersion);
}
