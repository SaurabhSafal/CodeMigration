using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class UserPriceBidLotChargesMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<UserPriceBidLotChargesMigration> _logger;
    private MigrationLogger? _migrationLogger;

    protected override string SelectQuery => @"
        SELECT
            PB_BuyerChargesId,
            EVENT_ID,
            PB_ChargesID
        FROM TBL_PB_BUYEROTHERCHARGES
        ORDER BY PB_BuyerChargesId";

    protected override string InsertQuery => @"
        INSERT INTO user_price_bid_lot_charges (
            user_price_bid_lot_charges_id,
            event_id,
            price_bid_charges_id,
            mandatory,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @user_price_bid_lot_charges_id,
            @event_id,
            @price_bid_charges_id,
            @mandatory,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (user_price_bid_lot_charges_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            price_bid_charges_id = EXCLUDED.price_bid_charges_id,
            mandatory = EXCLUDED.mandatory,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public UserPriceBidLotChargesMigration(IConfiguration configuration, ILogger<UserPriceBidLotChargesMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // user_price_bid_lot_charges_id
            "Direct",  // event_id
            "Direct",  // price_bid_charges_id
            "Fixed",   // mandatory
            "Fixed",   // created_by
            "Fixed",   // created_date
            "Fixed",   // modified_by
            "Fixed",   // modified_date
            "Fixed",   // is_deleted
            "Fixed",   // deleted_by
            "Fixed"    // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PB_BuyerChargesId", logic = "PB_BuyerChargesId -> user_price_bid_lot_charges_id (Primary key, autoincrement - UserPriceBidLotChargesID)", target = "user_price_bid_lot_charges_id" },
            new { source = "EVENT_ID", logic = "EVENT_ID -> event_id (Ref from Event Master - EventId)", target = "event_id" },
            new { source = "PB_ChargesID", logic = "PB_ChargesID -> price_bid_charges_id (Ref from PriceBidChargesMaster - PriceBidChargesId)", target = "price_bid_charges_id" },
            new { source = "-", logic = "mandatory -> false (Fixed Default)", target = "mandatory" },
            new { source = "-", logic = "created_by -> NULL (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: false);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "user_price_bid_lot_charges");
        _migrationLogger.LogInfo("Starting migration");

        _logger.LogInformation("Starting User Price Bid Lot Charges migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid event IDs
            var validEventIds = await LoadValidEventIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validEventIds.Count} valid event IDs");

            // Load valid price_bid_charges IDs
            var validPriceBidChargesIds = await LoadValidPriceBidChargesIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validPriceBidChargesIds.Count} valid price_bid_charges IDs");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var pbBuyerChargesId = reader["PB_BuyerChargesId"];
                var eventId = reader["EVENT_ID"];
                var pbChargesId = reader["PB_ChargesID"];

                // Skip if PB_BuyerChargesId is NULL
                if (pbBuyerChargesId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - PB_BuyerChargesId is NULL");
                    continue;
                }

                int pbBuyerChargesIdValue = Convert.ToInt32(pbBuyerChargesId);

                // Skip duplicates
                if (processedIds.Contains(pbBuyerChargesIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Validate event_id
                if (eventId != DBNull.Value)
                {
                    int eventIdValue = Convert.ToInt32(eventId);
                    if (!validEventIds.Contains(eventIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping PB_BuyerChargesId {pbBuyerChargesIdValue} - Invalid event_id: {eventIdValue}");
                        continue;
                    }
                }

                // Validate price_bid_charges_id
                if (pbChargesId != DBNull.Value)
                {
                    int pbChargesIdValue = Convert.ToInt32(pbChargesId);
                    if (!validPriceBidChargesIds.Contains(pbChargesIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping PB_BuyerChargesId {pbBuyerChargesIdValue} - Invalid price_bid_charges_id: {pbChargesIdValue}");
                        continue;
                    }
                }

                var record = new Dictionary<string, object>
                {
                    ["user_price_bid_lot_charges_id"] = pbBuyerChargesIdValue,
                    ["event_id"] = eventId ?? DBNull.Value,
                    ["price_bid_charges_id"] = pbChargesId ?? DBNull.Value,
                    ["mandatory"] = false,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(pbBuyerChargesIdValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchAsync(batch, pgConn);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchAsync(batch, pgConn);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"User Price Bid Lot Charges migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during User Price Bid Lot Charges migration");
            throw;
        }
    }

    private async Task<HashSet<int>> LoadValidEventIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT event_id FROM event_master WHERE event_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid event IDs from event_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid event IDs");
        }

        return validIds;
    }

    private async Task<HashSet<int>> LoadValidPriceBidChargesIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT price_bid_charges_id FROM price_bid_charges_master WHERE price_bid_charges_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid price_bid_charges IDs from price_bid_charges_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid price_bid_charges IDs");
        }

        return validIds;
    }

    private async Task<int> InsertBatchAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn)
    {
        if (batch == null || batch.Count == 0)
            return 0;

        int insertedCount = 0;
        try
        {
            using (var importer = pgConn.BeginBinaryImport(@"COPY user_price_bid_lot_charges (
                user_price_bid_lot_charges_id,
                event_id,
                price_bid_charges_id,
                mandatory,
                created_by,
                created_date,
                modified_by,
                modified_date,
                is_deleted,
                deleted_by,
                deleted_date
            ) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var record in batch)
                {
                    await importer.StartRowAsync();
                    importer.Write(record["user_price_bid_lot_charges_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["event_id"] == DBNull.Value ? null : record["event_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["price_bid_charges_id"] == DBNull.Value ? null : record["price_bid_charges_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["mandatory"], NpgsqlTypes.NpgsqlDbType.Boolean);
                    importer.Write(record["created_by"] == DBNull.Value ? null : record["created_by"], NpgsqlTypes.NpgsqlDbType.Text);
                    importer.Write(record["created_date"] == DBNull.Value ? null : record["created_date"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                    importer.Write(record["modified_by"] == DBNull.Value ? null : record["modified_by"], NpgsqlTypes.NpgsqlDbType.Text);
                    importer.Write(record["modified_date"] == DBNull.Value ? null : record["modified_date"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                    importer.Write(record["is_deleted"], NpgsqlTypes.NpgsqlDbType.Boolean);
                    importer.Write(record["deleted_by"] == DBNull.Value ? null : record["deleted_by"], NpgsqlTypes.NpgsqlDbType.Text);
                    importer.Write(record["deleted_date"] == DBNull.Value ? null : record["deleted_date"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                    insertedCount++;
                }
                await importer.CompleteAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error during bulk COPY of {batch.Count} records.");
            // Don't throw - continue with next batch
        }
        return insertedCount;
    }
}
