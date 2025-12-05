using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class SupplierPriceBidLotPriceMigration
    {
        private readonly ILogger<SupplierPriceBidLotPriceMigration> _logger;
        private readonly IConfiguration _configuration;

        public SupplierPriceBidLotPriceMigration(IConfiguration configuration, ILogger<SupplierPriceBidLotPriceMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "Auto-generated", target = "supplier_price_bid_lot_price_id", type = "PostgreSQL auto-increment" },
                new { source = "EVENTID", target = "event_id", type = "int -> integer, NOT NULL" },
                new { source = "VendorId", target = "supplier_id", type = "int -> integer, NOT NULL" },
                new { source = "TOTAL", target = "supplier_price_bid_lot_price", type = "decimal -> numeric, NOT NULL (UPSERT: update if event_id+supplier_id exists)" },
                new { source = "CreatedBy", target = "created_by", type = "int -> integer" },
                new { source = "CreatedDate", target = "created_date", type = "datetime -> timestamp with time zone" },
                new { source = "UpdatedBy", target = "modified_by", type = "int -> integer" },
                new { source = "UpdatedDate", target = "modified_date", type = "datetime -> timestamp with time zone" },
                new { source = "Default: false", target = "is_deleted", type = "NOT NULL, default false" },
                new { source = "Default: NULL", target = "deleted_by", type = "NULL" },
                new { source = "Default: NULL", target = "deleted_date", type = "NULL" },
                new { source = "UPDATEID", target = "Processing Order", type = "Used for ORDER BY to ensure correct update sequence" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SupplierPriceBidLotPrice migration (UPSERT mode)...");

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

                // Fetch source data
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        PBLotID,
                        EVENTID,
                        VendorId,
                        TOTAL,
                        CreatedBy,
                        CreatedDate
                    FROM TBL_PB_SUPPLIERLotPrice
                    WHERE PBLotID IS NOT NULL", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new SourceRow
                        {
                            PBLotID = reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            VendorId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            TOTAL = reader.IsDBNull(3) ? null : reader.GetDecimal(3),
                            CreatedBy = reader.IsDBNull(4) ? null : reader.GetInt32(4),
                            CreatedDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_PB_SUPPLIERLotPrice");

                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id (REQUIRED - NOT NULL constraint)
                        if (!record.EVENTID.HasValue)
                        {
                            _logger.LogWarning($"Skipping PBLotID {record.PBLotID}: EVENTID is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEventIds.Contains(record.EVENTID.Value))
                        {
                            _logger.LogWarning($"Skipping PBLotID {record.PBLotID}: event_id={record.EVENTID} not found in event_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate supplier_id (REQUIRED - NOT NULL constraint)
                        if (!record.VendorId.HasValue)
                        {
                            _logger.LogWarning($"Skipping PBLotID {record.PBLotID}: VendorId is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validSupplierIds.Contains(record.VendorId.Value))
                        {
                            _logger.LogWarning($"Skipping PBLotID {record.PBLotID}: supplier_id={record.VendorId} not found in supplier_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate supplier_price_bid_lot_price (REQUIRED - NOT NULL constraint)
                        if (!record.TOTAL.HasValue)
                        {
                            _logger.LogWarning($"Skipping PBLotID {record.PBLotID}: TOTAL is null");
                            skippedRecords++;
                            continue;
                        }

                        var targetRow = new TargetRow
                        {
                            EventId = record.EVENTID.Value,
                            SupplierId = record.VendorId.Value,
                            SupplierPriceBidLotPrice = record.TOTAL.Value,
                            CreatedBy = record.CreatedBy,
                            CreatedDate = record.CreatedDate,
                            ModifiedBy = null,
                            ModifiedDate = null,
                            IsDeleted = false,
                            DeletedBy = null,
                            DeletedDate = null
                        };

                        insertBatch.Add(targetRow);
                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch);
                            insertBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"PBLotID {record.PBLotID}: {ex.Message}");
                        skippedRecords++;
                    }
                }

                // Execute remaining batch
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch);
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
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
            sql.AppendLine("INSERT INTO supplier_price_bid_lot_price (");
            sql.AppendLine("    event_id, supplier_id, supplier_price_bid_lot_price,");
            sql.AppendLine("    created_by, created_date, modified_by, modified_date,");
            sql.AppendLine("    is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@EventId{i}, @SupplierId{i}, @SupplierPriceBidLotPrice{i}, @CreatedBy{i}, @CreatedDate{i}, @ModifiedBy{i}, @ModifiedDate{i}, @IsDeleted{i}, @DeletedBy{i}, @DeletedDate{i})");
                
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@SupplierId{i}", row.SupplierId);
                cmd.Parameters.AddWithValue($"@SupplierPriceBidLotPrice{i}", row.SupplierPriceBidLotPrice);
                cmd.Parameters.AddWithValue($"@CreatedBy{i}", (object?)row.CreatedBy ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@CreatedDate{i}", (object?)row.CreatedDate ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@ModifiedBy{i}", DBNull.Value);
                cmd.Parameters.AddWithValue($"@ModifiedDate{i}", DBNull.Value);
                cmd.Parameters.AddWithValue($"@IsDeleted{i}", row.IsDeleted);
                cmd.Parameters.AddWithValue($"@DeletedBy{i}", DBNull.Value);
                cmd.Parameters.AddWithValue($"@DeletedDate{i}", DBNull.Value);
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
            var updateCount = await UpdateFromAucSupplierLotPriceAsync();
            
            _logger.LogInformation($"Total migration completed. Initial: {initialCount}, Updates: {updateCount}");
            
            return initialCount + updateCount;
        }

        public async Task<int> UpdateFromAucSupplierLotPriceAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var upsertedRecords = 0;
            var skippedRecords = 0;

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting SupplierPriceBidLotPrice UPSERT from TBL_AUC_SUPPLIERLotPrice (ordered by UPDATEID)...");

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

                // Fetch source data from TBL_AUC_SUPPLIERLotPrice ordered by UPDATEID
                var sourceData = new List<AucSupplierLotPriceRow>();
                
                using (var cmd = new SqlCommand(@"
                    SELECT 
                        EVENTID,
                        VendorId,
                        TOTAL,
                        CreatedBy,
                        CreatedDate,
                        UpdatedBy,
                        UpdatedDate,
                        UPDATEID
                    FROM TBL_AUC_SUPPLIERLotPrice
                    WHERE EVENTID IS NOT NULL 
                      AND VendorId IS NOT NULL
                    ORDER BY UPDATEID", sqlConnection))
                {
                    cmd.CommandTimeout = 600;
                    using var reader = await cmd.ExecuteReaderAsync();
                    
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add(new AucSupplierLotPriceRow
                        {
                            EVENTID = reader.IsDBNull(0) ? null : reader.GetInt32(0),
                            VendorId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            TOTAL = reader.IsDBNull(2) ? null : reader.GetDecimal(2),
                            CreatedBy = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                            CreatedDate = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                            UpdatedBy = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                            UpdatedDate = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                            UPDATEID = reader.IsDBNull(7) ? 0 : reader.GetInt32(7)
                        });
                    }
                }

                _logger.LogInformation($"Fetched {sourceData.Count} records from TBL_AUC_SUPPLIERLotPrice (ordered by UPDATEID)");

                // Process each record in order (UPDATEID ensures correct sequence)
                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id
                        if (!record.EVENTID.HasValue)
                        {
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: EVENTID is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validEventIds.Contains(record.EVENTID.Value))
                        {
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: event_id={record.EVENTID} not found in event_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate supplier_id
                        if (!record.VendorId.HasValue)
                        {
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: VendorId is null");
                            skippedRecords++;
                            continue;
                        }

                        if (!validSupplierIds.Contains(record.VendorId.Value))
                        {
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: supplier_id={record.VendorId} not found in supplier_master");
                            skippedRecords++;
                            continue;
                        }

                        // Validate TOTAL
                        if (!record.TOTAL.HasValue)
                        {
                            _logger.LogWarning($"Skipping UPDATEID {record.UPDATEID}: TOTAL is null");
                            skippedRecords++;
                            continue;
                        }

                        // UPSERT: Insert or Update based on event_id + supplier_id
                        using var upsertCmd = new NpgsqlCommand(@"
                            INSERT INTO supplier_price_bid_lot_price (
                                event_id, 
                                supplier_id, 
                                supplier_price_bid_lot_price,
                                created_by, 
                                created_date, 
                                modified_by, 
                                modified_date,
                                is_deleted, 
                                deleted_by, 
                                deleted_date
                            ) VALUES (
                                @event_id, 
                                @supplier_id, 
                                @supplier_price_bid_lot_price,
                                @created_by, 
                                @created_date, 
                                @modified_by, 
                                @modified_date,
                                @is_deleted, 
                                @deleted_by, 
                                @deleted_date
                            )
                            ON CONFLICT (event_id, supplier_id) DO UPDATE SET
                                supplier_price_bid_lot_price = EXCLUDED.supplier_price_bid_lot_price,
                                modified_by = EXCLUDED.modified_by,
                                modified_date = EXCLUDED.modified_date", pgConnection);

                        upsertCmd.Parameters.AddWithValue("@event_id", record.EVENTID.Value);
                        upsertCmd.Parameters.AddWithValue("@supplier_id", record.VendorId.Value);
                        upsertCmd.Parameters.AddWithValue("@supplier_price_bid_lot_price", record.TOTAL.Value);
                        upsertCmd.Parameters.AddWithValue("@created_by", (object?)record.CreatedBy ?? DBNull.Value);
                        upsertCmd.Parameters.AddWithValue("@created_date", (object?)record.CreatedDate ?? DBNull.Value);
                        upsertCmd.Parameters.AddWithValue("@modified_by", (object?)record.UpdatedBy ?? DBNull.Value);
                        upsertCmd.Parameters.AddWithValue("@modified_date", (object?)record.UpdatedDate ?? DBNull.Value);
                        upsertCmd.Parameters.AddWithValue("@is_deleted", false);
                        upsertCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                        upsertCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                        await upsertCmd.ExecuteNonQueryAsync();
                        upsertedRecords++;

                        if (upsertedRecords % 100 == 0)
                        {
                            _logger.LogInformation($"Processed {upsertedRecords} UPSERT operations...");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"UPDATEID {record.UPDATEID} (Event: {record.EVENTID}, Vendor: {record.VendorId}): {ex.Message}");
                        skippedRecords++;
                    }
                }

                _logger.LogInformation($"UPSERT completed. Upserted: {upsertedRecords}, Skipped: {skippedRecords}");
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
            public int PBLotID { get; set; }
            public int? EVENTID { get; set; }
            public int? VendorId { get; set; }
            public decimal? TOTAL { get; set; }
            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }
        }

        private class TargetRow
        {
            public int EventId { get; set; } // NOT NULL
            public int SupplierId { get; set; } // NOT NULL
            public decimal SupplierPriceBidLotPrice { get; set; } // NOT NULL
            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }
            public int? ModifiedBy { get; set; }
            public DateTime? ModifiedDate { get; set; }
            public bool IsDeleted { get; set; } = false; // NOT NULL
            public int? DeletedBy { get; set; }
            public DateTime? DeletedDate { get; set; }
        }

        private class AucSupplierLotPriceRow
        {
            public int? EVENTID { get; set; }
            public int? VendorId { get; set; }
            public decimal? TOTAL { get; set; }
            public int? CreatedBy { get; set; }
            public DateTime? CreatedDate { get; set; }
            public int? UpdatedBy { get; set; }
            public DateTime? UpdatedDate { get; set; }
            public int UPDATEID { get; set; }
        }
    }
}
