using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;
using DataMigration.Services;

namespace DataMigration.Services
{
    public class SupplierPriceBoqItemsMigration
    {
        private readonly ILogger<SupplierPriceBoqItemsMigration> _logger;
    private MigrationLogger? _migrationLogger;
        private readonly IConfiguration _configuration;

        public SupplierPriceBoqItemsMigration(IConfiguration configuration, ILogger<SupplierPriceBoqItemsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

    public MigrationLogger? GetLogger() => _migrationLogger;

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "supplier_price_boq_items_id", type = "PostgreSQL auto-increment" },
                new { source = "TBL_PB_SUPPLIER_SUB.PBSubId", target = "event_boq_items_id", type = "int -> integer" },
                new { source = "TBL_PB_BUYER.EVENTID", target = "event_id", type = "int -> integer" },
                new { source = "Lookup: event_supplier_price_bid by EVENTID + SUPPLIERID", target = "event_supplier_price_bid_id", type = "Lookup -> integer" },
                new { source = "TBL_PB_SUPPLIER_SUB.PB_SupplierId", target = "supplier_id", type = "int -> integer" },
                new { source = "TBL_PB_SUPPLIER_SUB.Rate", target = "boq_item_unit_price", type = "decimal -> numeric" },
                new { source = "TBL_PB_SUPPLIER.PBID", target = "event_supplier_line_item_id", type = "int -> integer" }
            };
        }

        public async Task<int> MigrateAsync()
        {
        _migrationLogger = new MigrationLogger(_logger, "supplier_price_boq_items");
        _migrationLogger.LogInfo("Starting migration");

            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;
            var skippedDetails = new List<(string, string)>(); // (record id, reason)

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SupplierPriceBoqItems migration...");

                // Truncate and restart identity
                using (var cmd = new NpgsqlCommand(@"
                    TRUNCATE TABLE supplier_price_boq_items RESTART IDENTITY CASCADE;", pgConnection))
                {
                    await cmd.ExecuteNonQueryAsync();
                    _logger.LogInformation("Reset supplier_price_boq_items table and restarted identity sequence");
                }

                // Build lookup for valid event_ids from PostgreSQL
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_id 
                    FROM event_master 
                    WHERE event_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_id lookup with {validEventIds.Count} entries");

                // Build lookup for valid supplier_ids from PostgreSQL
                var validSupplierIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT supplier_id 
                    FROM supplier_master 
                    WHERE supplier_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validSupplierIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built supplier_id lookup with {validSupplierIds.Count} entries");

                // Build lookup for valid event_boq_items_ids from PostgreSQL
                var validEventBoqItemsIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_boq_items_id 
                    FROM event_boq_items 
                    WHERE event_boq_items_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventBoqItemsIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_boq_items_id lookup with {validEventBoqItemsIds.Count} entries");

                // Build lookup for valid event_supplier_line_item_ids from PostgreSQL
                var validEventSupplierLineItemIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_supplier_line_item_id 
                    FROM event_supplier_line_item 
                    WHERE event_supplier_line_item_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventSupplierLineItemIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_supplier_line_item_id lookup with {validEventSupplierLineItemIds.Count} entries");

                // Build lookup for event_supplier_price_bid_id from PostgreSQL
                // Key: "eventId_supplierId", Value: event_supplier_price_bid_id
                var eventSupplierPriceBidLookup = new Dictionary<string, int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_supplier_price_bid_id, event_id, supplier_id 
                    FROM event_supplier_price_bid 
                    WHERE event_id IS NOT NULL AND supplier_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var eventId = reader.GetInt32(1);
                        var supplierId = reader.GetInt32(2);
                        var key = $"{eventId}_{supplierId}";
                        if (!eventSupplierPriceBidLookup.ContainsKey(key))
                        {
                            eventSupplierPriceBidLookup[key] = id;
                        }
                    }
                }
                _logger.LogInformation($"Built event_supplier_price_bid_id lookup with {eventSupplierPriceBidLookup.Count} entries");

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        TBL_PB_SUPPLIER_SUB.PBSubId as event_boq_items_id,
                        TBL_PB_BUYER.EVENTID,
                        TBL_PB_SUPPLIER.PBID as event_supplier_line_item_id,
                        TBL_PB_SUPPLIER_SUB.PB_SupplierId as SUPPLIERID,
                        TBL_PB_SUPPLIER_SUB.Rate
                    FROM TBL_PB_SUPPLIER_SUB
                    INNER JOIN TBL_PB_BUYER_SUB ON TBL_PB_BUYER_SUB.PBSubId = TBL_PB_SUPPLIER_SUB.PBSubId
                    INNER JOIN TBL_PB_BUYER ON TBL_PB_BUYER.PBID = TBL_PB_BUYER_SUB.PBID
                    INNER JOIN TBL_PB_SUPPLIER ON TBL_PB_SUPPLIER.EVENTID = TBL_PB_BUYER.EVENTID 
                        AND TBL_PB_SUPPLIER.PRTRANSID = TBL_PB_BUYER.PRTRANSID 
                        AND TBL_PB_SUPPLIER.SUPPLIER_ID = TBL_PB_SUPPLIER_SUB.PB_SupplierId
                    WHERE TBL_PB_SUPPLIER_SUB.PBSubId IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        decimal? rate = null;
                        if (!reader.IsDBNull(4))
                        {
                            // Try to safely convert to decimal, handling both numeric and string types
                            var rateValue = reader.GetValue(4);
                            if (decimal.TryParse(rateValue.ToString(), out decimal parsedRate))
                            {
                                rate = parsedRate;
                            }
                            else
                            {
                                _logger.LogWarning($"Unable to parse Rate value: {rateValue}");
                            }
                        }

                        sourceData.Add(new SourceRow
                        {
                            EventBoqItemsId = reader.IsDBNull(0) ? null : reader.GetInt32(0),
                            EventId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            EventSupplierLineItemId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            SupplierId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            Rate = rate
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from SQL Server");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id (REQUIRED - NOT NULL)
                        if (!record.EventId.HasValue)
                        {
                            var reason = "event_id is null";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        if (!validEventIds.Contains(record.EventId.Value))
                        {
                            var reason = $"event_id={record.EventId} not found in event_master";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        // Validate supplier_id (REQUIRED - NOT NULL)
                        if (!record.SupplierId.HasValue)
                        {
                            var reason = "supplier_id is null";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        if (!validSupplierIds.Contains(record.SupplierId.Value))
                        {
                            var reason = $"supplier_id={record.SupplierId} not found in supplier_master";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        // Validate event_boq_items_id (REQUIRED - NOT NULL)
                        if (!record.EventBoqItemsId.HasValue)
                        {
                            var reason = "event_boq_items_id is null";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        if (!validEventBoqItemsIds.Contains(record.EventBoqItemsId.Value))
                        {
                            var reason = $"event_boq_items_id={record.EventBoqItemsId} not found in event_boq_items";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        // Validate event_supplier_line_item_id (REQUIRED - NOT NULL)
                        if (!record.EventSupplierLineItemId.HasValue)
                        {
                            var reason = "event_supplier_line_item_id is null";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        if (!validEventSupplierLineItemIds.Contains(record.EventSupplierLineItemId.Value))
                        {
                            var reason = $"event_supplier_line_item_id={record.EventSupplierLineItemId} not found in event_supplier_line_item";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        // Lookup event_supplier_price_bid_id (REQUIRED - NOT NULL)
                        var lookupKey = $"{record.EventId.Value}_{record.SupplierId.Value}";
                        if (!eventSupplierPriceBidLookup.TryGetValue(lookupKey, out var eventSupplierPriceBidId))
                        {
                            var reason = $"event_supplier_price_bid_id not found for event_id={record.EventId}, supplier_id={record.SupplierId}";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        // Validate boq_item_unit_price (REQUIRED - NOT NULL)
                        if (!record.Rate.HasValue)
                        {
                            var reason = "Rate is null";
                            _logger.LogWarning($"Skipping record: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            EventBoqItemsId = record.EventBoqItemsId.Value,
                            EventId = record.EventId.Value,
                            EventSupplierPriceBidId = eventSupplierPriceBidId,
                            SupplierId = record.SupplierId.Value,
                            BoqItemUnitPrice = record.Rate.Value,
                            EventSupplierLineItemId = record.EventSupplierLineItemId.Value
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        var reason = ex.Message;
                        _logger.LogError($"Error processing record: {reason}");
                        skippedRecords++;
                        skippedDetails.Add(($"EventBoqItemsId:{record.EventBoqItemsId}", reason));
                    }
                }

                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");

                // Export migration stats to Excel
                var totalRecords = migratedRecords + skippedRecords;
                MigrationStatsExporter.ExportToExcel(
                    "migration_outputs/SupplierPriceBoqItemsMigration_Stats.xlsx",
                    totalRecords,
                    migratedRecords,
                    skippedRecords,
                    _logger,
                    skippedDetails
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            return migratedRecords;
        }

        private async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO supplier_price_boq_items (");
            sql.AppendLine("    event_boq_items_id, event_id, event_supplier_price_bid_id,");
            sql.AppendLine("    supplier_id, boq_item_unit_price, event_supplier_line_item_id,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventBoqItemsId{i}, @EventId{i}, @EventSupplierPriceBidId{i}, @SupplierId{i}, @BoqItemUnitPrice{i}, @EventSupplierLineItemId{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@EventBoqItemsId{i}", row.EventBoqItemsId);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@EventSupplierPriceBidId{i}", row.EventSupplierPriceBidId);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", row.SupplierId);
                cmd.Parameters.AddWithValue($"@BoqItemUnitPrice{i}", row.BoqItemUnitPrice);
                cmd.Parameters.AddWithValue($"@EventSupplierLineItemId{i}", row.EventSupplierLineItemId);
            }

            sql.AppendLine(string.Join(",\n", values));
            cmd.CommandText = sql.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                _logger.LogDebug($"Batch inserted {rowsAffected} records");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Batch insert failed: {ex.Message}");
                throw;
            }
        }

        public async Task<int> MigrateAndUpdateAsync()
        {
            // First run the initial migration
            var initialCount = await MigrateAsync();
            
            // Then run the update logic
            var updateCount = await UpdateFromAucSupplierSubAsync();
            
            _logger.LogInformation($"Total migration completed. Initial: {initialCount}, Updates: {updateCount}");
            
            return initialCount + updateCount;
        }

        public async Task<int> UpdateFromAucSupplierSubAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var upsertedRecords = 0;
            var skippedRecords = 0;
            var skippedDetails = new List<(string, string)>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SupplierPriceBoqItems UPSERT from TBL_AUC_SUPPLIER_SUB (ordered by UPDATEID)...");

                // Build lookup for valid event_ids
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_id 
                    FROM event_master 
                    WHERE event_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_id lookup with {validEventIds.Count} entries");

                // Build lookup for valid supplier_ids
                var validSupplierIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT supplier_id 
                    FROM supplier_master 
                    WHERE supplier_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validSupplierIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built supplier_id lookup with {validSupplierIds.Count} entries");

                // Build lookup for valid event_boq_items_ids
                var validEventBoqItemsIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_boq_items_id 
                    FROM event_boq_items 
                    WHERE event_boq_items_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventBoqItemsIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_boq_items_id lookup with {validEventBoqItemsIds.Count} entries");

                // Build lookup for valid event_supplier_line_item_ids
                var validEventSupplierLineItemIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_supplier_line_item_id 
                    FROM event_supplier_line_item 
                    WHERE event_supplier_line_item_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventSupplierLineItemIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Built event_supplier_line_item_id lookup with {validEventSupplierLineItemIds.Count} entries");

                // Build lookup for event_supplier_price_bid_id
                var eventSupplierPriceBidLookup = new Dictionary<string, int>();
                using (var cmd = new NpgsqlCommand(@"
                    SELECT event_supplier_price_bid_id, event_id, supplier_id 
                    FROM event_supplier_price_bid 
                    WHERE event_id IS NOT NULL AND supplier_id IS NOT NULL", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetInt32(0);
                        var eventId = reader.GetInt32(1);
                        var supplierId = reader.GetInt32(2);
                        var key = $"{eventId}_{supplierId}";
                        if (!eventSupplierPriceBidLookup.ContainsKey(key))
                        {
                            eventSupplierPriceBidLookup[key] = id;
                        }
                    }
                }
                _logger.LogInformation($"Built event_supplier_price_bid_id lookup with {eventSupplierPriceBidLookup.Count} entries");

                // Fetch source data from TBL_AUC_SUPPLIER_SUB ordered by UPDATEID
                var sourceData = new List<AucSupplierSubRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        TBL_AUC_SUPPLIER_SUB.PBSubId as event_boq_items_id,
                        TBL_PB_BUYER.EVENTID,
                        TBL_AUC_SUPPLIER.PBID as event_supplier_line_item_id,
                        TBL_AUC_SUPPLIER_SUB.PB_SupplierId as SUPPLIERID,
                        TBL_AUC_SUPPLIER_SUB.Rate,
                        TBL_AUC_SUPPLIER_SUB.UPDATEID
                    FROM TBL_AUC_SUPPLIER_SUB
                    INNER JOIN TBL_PB_BUYER_SUB ON TBL_PB_BUYER_SUB.PBSubId = TBL_AUC_SUPPLIER_SUB.PBSubId
                    INNER JOIN TBL_PB_BUYER ON TBL_PB_BUYER.PBID = TBL_PB_BUYER_SUB.PBID
                    INNER JOIN TBL_AUC_SUPPLIER ON TBL_AUC_SUPPLIER.EVENTID = TBL_PB_BUYER.EVENTID 
                        AND TBL_AUC_SUPPLIER.PRTRANSID = TBL_PB_BUYER.PRTRANSID 
                        AND TBL_AUC_SUPPLIER.SUPPLIER_ID = TBL_AUC_SUPPLIER_SUB.PB_SupplierId
                    WHERE TBL_AUC_SUPPLIER_SUB.PBSubId IS NOT NULL
                    ORDER BY TBL_AUC_SUPPLIER_SUB.UPDATEID", sqlConnection))
                {
                    cmd.CommandTimeout = 600;
                    using var reader = await cmd.ExecuteReaderAsync();
                    
                    while (await reader.ReadAsync())
                    {
                        decimal? rate = null;
                        if (!reader.IsDBNull(4))
                        {
                            var rateValue = reader.GetValue(4);
                            if (decimal.TryParse(rateValue.ToString(), out decimal parsedRate))
                            {
                                rate = parsedRate;
                            }
                        }

                        sourceData.Add(new AucSupplierSubRow
                        {
                            EventBoqItemsId = reader.IsDBNull(0) ? null : reader.GetInt32(0),
                            EventId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            EventSupplierLineItemId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            SupplierId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            Rate = rate,
                            UPDATEID = reader.IsDBNull(5) ? 0 : reader.GetInt32(5)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_AUC_SUPPLIER_SUB (ordered by UPDATEID)");

                // Process each record in order
                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate required fields
                        if (!record.EventId.HasValue || !record.SupplierId.HasValue || !record.EventBoqItemsId.HasValue ||
                            !record.EventSupplierLineItemId.HasValue || !record.Rate.HasValue)
                        {
                            var reason = "Missing required fields";
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                            continue;
                        }

                        // Validate foreign keys
                        if (!validEventIds.Contains(record.EventId.Value))
                        {
                            var reason = $"event_id={record.EventId} not found";
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                            continue;
                        }

                        if (!validSupplierIds.Contains(record.SupplierId.Value))
                        {
                            var reason = $"supplier_id={record.SupplierId} not found";
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                            continue;
                        }

                        if (!validEventBoqItemsIds.Contains(record.EventBoqItemsId.Value))
                        {
                            var reason = $"event_boq_items_id={record.EventBoqItemsId} not found";
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                            continue;
                        }

                        if (!validEventSupplierLineItemIds.Contains(record.EventSupplierLineItemId.Value))
                        {
                            var reason = $"event_supplier_line_item_id={record.EventSupplierLineItemId} not found";
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                            continue;
                        }

                        // Lookup event_supplier_price_bid_id
                        var lookupKey = $"{record.EventId.Value}_{record.SupplierId.Value}";
                        if (!eventSupplierPriceBidLookup.TryGetValue(lookupKey, out var eventSupplierPriceBidId))
                        {
                            var reason = "event_supplier_price_bid_id not found";
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: {reason}");
                            skippedRecords++;
                            skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                            continue;
                        }

                        // UPSERT based on unique key: (event_boq_items_id, event_supplier_line_item_id, supplier_id)
                        using var upsertCmd = new NpgsqlCommand(@"
                            INSERT INTO supplier_price_boq_items (
                                event_boq_items_id,
                                event_id,
                                event_supplier_price_bid_id,
                                supplier_id,
                                boq_item_unit_price,
                                event_supplier_line_item_id,
                                created_by,
                                created_date,
                                modified_by,
                                modified_date,
                                is_deleted,
                                deleted_by,
                                deleted_date
                            ) VALUES (
                                @event_boq_items_id,
                                @event_id,
                                @event_supplier_price_bid_id,
                                @supplier_id,
                                @boq_item_unit_price,
                                @event_supplier_line_item_id,
                                NULL,
                                CURRENT_TIMESTAMP,
                                NULL,
                                CURRENT_TIMESTAMP,
                                false,
                                NULL,
                                NULL
                            )
                            ON CONFLICT (event_boq_items_id, event_supplier_line_item_id, supplier_id) DO UPDATE SET
                                boq_item_unit_price = EXCLUDED.boq_item_unit_price,
                                event_supplier_price_bid_id = EXCLUDED.event_supplier_price_bid_id,
                                modified_date = CURRENT_TIMESTAMP", pgConnection);

                        upsertCmd.Parameters.AddWithValue("@event_boq_items_id", record.EventBoqItemsId.Value);
                        upsertCmd.Parameters.AddWithValue("@event_id", record.EventId.Value);
                        upsertCmd.Parameters.AddWithValue("@event_supplier_price_bid_id", eventSupplierPriceBidId);
                        upsertCmd.Parameters.AddWithValue("@supplier_id", record.SupplierId.Value);
                        upsertCmd.Parameters.AddWithValue("@boq_item_unit_price", record.Rate.Value);
                        upsertCmd.Parameters.AddWithValue("@event_supplier_line_item_id", record.EventSupplierLineItemId.Value);

                        await upsertCmd.ExecuteNonQueryAsync();
                        upsertedRecords++;

                        if (upsertedRecords % 100 == 0)
                        {
                            _logger.LogInformation($"Processed {upsertedRecords} UPSERT operations...");
                        }
                    }
                    catch (Exception ex)
                    {
                        var reason = ex.Message;
                        _logger.LogError($"UPDATEID {record.UPDATEID}: {reason}");
                        skippedRecords++;
                        skippedDetails.Add(($"UPDATEID:{record.UPDATEID}", reason));
                    }
                }

                _logger.LogInformation($"UPSERT completed. Upserted: {upsertedRecords}, Skipped: {skippedRecords}");

                // Export migration stats to Excel
                var totalRecords = upsertedRecords + skippedRecords;
                MigrationStatsExporter.ExportToExcel(
                    "migration_outputs/SupplierPriceBoqItemsUpsert_Stats.xlsx",
                    totalRecords,
                    upsertedRecords,
                    skippedRecords,
                    _logger,
                    skippedDetails
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "UPSERT migration failed");
                throw;
            }

            return upsertedRecords;
        }

        private class SourceRow
        {
            public int? EventBoqItemsId { get; set; }
            public int? EventId { get; set; }
            public int? EventSupplierLineItemId { get; set; }
            public int? SupplierId { get; set; }
            public decimal? Rate { get; set; }
        }

        private class TargetRow
        {
            public int EventBoqItemsId { get; set; }
            public int EventId { get; set; }
            public int EventSupplierPriceBidId { get; set; }
            public int SupplierId { get; set; }
            public decimal BoqItemUnitPrice { get; set; }
            public int EventSupplierLineItemId { get; set; }
        }

        private class AucSupplierSubRow
        {
            public int? EventBoqItemsId { get; set; }
            public int? EventId { get; set; }
            public int? EventSupplierLineItemId { get; set; }
            public int? SupplierId { get; set; }
            public decimal? Rate { get; set; }
            public int UPDATEID { get; set; }
        }
    }
}
