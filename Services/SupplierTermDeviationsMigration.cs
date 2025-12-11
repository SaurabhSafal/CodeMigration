using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class SupplierTermDeviationsMigration : MigrationService
{
    private const int BATCH_SIZE = 5000; // Increased for COPY operations
    private readonly ILogger<SupplierTermDeviationsMigration> _logger;
    private MigrationLogger? _migrationLogger;
    
    // Data class for batch processing
    private class SupplierTermDeviationRecord
    {
        public int SupplierTermDeviationId { get; set; }
        public int SupplierTermId { get; set; }
        public int? EventId { get; set; }
        public string DeviationRemarks { get; set; } = "";
        public int? UserId { get; set; }
        public int? SupplierId { get; set; }
        public string? UserType { get; set; }
    }

    protected override string SelectQuery => @"
        SELECT
            VENDORDEVIATIONTRNID,
            VENDORDEVIATIONMSTID,
            DEVIATIONREMARKS,
            USERTYPE,
            ACTIONBY,
            ACTIONDATE,
            ISUPDATEDCLAUSE
        FROM TBL_VENDORDEVIATIONTRN
        ORDER BY VENDORDEVIATIONTRNID";

    protected override string InsertQuery => ""; // Not used with COPY

    public SupplierTermDeviationsMigration(IConfiguration configuration, ILogger<SupplierTermDeviationsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",      // supplier_term_deviation_id
            "Direct",      // supplier_term_id
            "Lookup",      // event_id (from supplier_terms via supplier_term_id)
            "Direct",      // deviation_remarks
            "Conditional", // user_id (ACTIONBY if USERTYPE != 'Vendor', else NULL)
            "Conditional", // supplier_id (ACTIONBY if USERTYPE = 'Vendor', else from supplier_terms lookup)
            "Fixed",       // created_by
            "Fixed",       // created_date
            "Fixed",       // modified_by
            "Fixed",       // modified_date
            "Fixed",       // is_deleted
            "Fixed",       // deleted_by
            "Fixed",       // deleted_date
            "Direct"       // user_type
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "VENDORDEVIATIONTRNID", logic = "VENDORDEVIATIONTRNID -> supplier_term_deviation_id (Primary key, autoincrement - SupplierTermDeviationId)", target = "supplier_term_deviation_id" },
            new { source = "VENDORDEVIATIONMSTID", logic = "VENDORDEVIATIONMSTID -> supplier_term_id (Foreign key to supplier_terms - SupplierTermId)", target = "supplier_term_id" },
            new { source = "-", logic = "event_id -> Lookup from supplier_terms (EventId)", target = "event_id" },
            new { source = "DEVIATIONREMARKS", logic = "DEVIATIONREMARKS -> deviation_remarks (DEVIATIONREMARKS)", target = "deviation_remarks" },
            new { source = "ACTIONBY", logic = "ACTIONBY -> user_id (Conditional: if USERTYPE != 'Vendor', else NULL)", target = "user_id" },
            new { source = "ACTIONBY", logic = "ACTIONBY -> supplier_id (Conditional: if USERTYPE = 'Vendor', else from supplier_terms lookup)", target = "supplier_id" },
            new { source = "ACTIONDATE", logic = "ACTIONDATE -> Not mapped to target table", target = "-" },
            new { source = "ISUPDATEDCLAUSE", logic = "ISUPDATEDCLAUSE -> Not mapped to target table", target = "-" },
            new { source = "USERTYPE", logic = "USERTYPE -> user_type", target = "user_type" },
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
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "supplier_term_deviations");
        _migrationLogger.LogInfo("Starting optimized migration with COPY");
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;
        var skippedDetails = new List<(string, string)>();

        try
        {
            // Load validation data
            var supplierTermsMap = await LoadSupplierTermsMapAsync(pgConn);
            var validUserIds = await LoadValidUserIdsAsync(pgConn);
            _logger.LogInformation($"Loaded validation data: {supplierTermsMap.Count} supplier_terms, {validUserIds.Count} users");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;
            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<SupplierTermDeviationRecord>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                // Fast field access by ordinal
                var vendorDeviationTrnId = reader.IsDBNull(0) ? (int?)null : reader.GetInt32(0);
                var vendorDeviationMstId = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
                var deviationRemarks = reader.IsDBNull(2) ? "" : reader.GetString(2);
                var userType = reader.IsDBNull(3) ? null : reader.GetString(3);
                var actionBy = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);

                // Skip if primary key is NULL
                if (!vendorDeviationTrnId.HasValue)
                {
                    skippedRecords++;
                    skippedDetails.Add(("NULL", "VENDORDEVIATIONTRNID is NULL"));
                    continue;
                }

                int id = vendorDeviationTrnId.Value;

                // Skip duplicates
                if (processedIds.Contains(id))
                {
                    skippedRecords++;
                    skippedDetails.Add((id.ToString(), "Duplicate VENDORDEVIATIONTRNID"));
                    continue;
                }

                // Skip if supplier_term_id is NULL
                if (!vendorDeviationMstId.HasValue)
                {
                    skippedRecords++;
                    skippedDetails.Add((id.ToString(), "supplier_term_id is NULL"));
                    continue;
                }

                int supplierTermId = vendorDeviationMstId.Value;

                // Validate supplier_term_id
                if (!supplierTermsMap.ContainsKey(supplierTermId))
                {
                    skippedRecords++;
                    skippedDetails.Add((id.ToString(), $"Invalid supplier_term_id: {supplierTermId}"));
                    continue;
                }

                var supplierTermData = supplierTermsMap[supplierTermId];

                // Determine user_id and supplier_id based on USERTYPE
                int? userId = null;
                int? supplierId = null;
                string userTypeValue = userType?.Trim() ?? "";
                
                if (string.Equals(userTypeValue, "Vendor", StringComparison.OrdinalIgnoreCase))
                {
                    supplierId = actionBy;
                }
                else
                {
                    userId = actionBy;
                    supplierId = supplierTermData.SupplierId;
                }

                // Validate user_id if not null and USERTYPE != 'Vendor'
                if (userId.HasValue && !validUserIds.Contains(userId.Value))
                {
                    skippedRecords++;
                    skippedDetails.Add((id.ToString(), $"Invalid user_id: {userId.Value}"));
                    continue;
                }

                var record = new SupplierTermDeviationRecord
                {
                    SupplierTermDeviationId = id,
                    SupplierTermId = supplierTermId,
                    EventId = supplierTermData.EventId,
                    DeviationRemarks = deviationRemarks,
                    UserId = userId,
                    SupplierId = supplierId,
                    UserType = userType
                };

                batch.Add(record);
                processedIds.Add(id);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchWithCopyAsync(batch, pgConn);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                    
                    if (totalRecords % 10000 == 0)
                    {
                        var elapsed = stopwatch.Elapsed;
                        var rate = totalRecords / elapsed.TotalSeconds;
                        _logger.LogInformation($"Progress: {totalRecords:N0} processed, {migratedRecords:N0} inserted, {rate:F1} records/sec");
                    }
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithCopyAsync(batch, pgConn);
                migratedRecords += batchMigrated;
            }

            stopwatch.Stop();
            var totalRate = migratedRecords / stopwatch.Elapsed.TotalSeconds;
            _logger.LogInformation($"Supplier Term Deviations migration completed in {stopwatch.Elapsed:mm\\:ss}. Total: {totalRecords:N0}, Migrated: {migratedRecords:N0}, Skipped: {skippedRecords:N0}, Rate: {totalRate:F1} records/sec");

            // Export migration stats
            MigrationStatsExporter.ExportToExcel(
                "migration_outputs/SupplierTermDeviationsMigration_Stats.xlsx",
                totalRecords,
                migratedRecords,
                skippedRecords,
                _logger,
                skippedDetails
            );

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Term Deviations migration");
            throw;
        }
    }

    private async Task<Dictionary<int, (int? EventId, int? SupplierId)>> LoadSupplierTermsMapAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, (int? EventId, int? SupplierId)>();

        try
        {
            var query = "SELECT supplier_term_id, event_id, supplier_id FROM supplier_terms WHERE supplier_term_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                int supplierTermId = reader.GetInt32(0);
                int? eventId = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
                int? supplierId = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2);
                
                map[supplierTermId] = (eventId, supplierId);
            }

            _logger.LogInformation($"Loaded {map.Count} supplier_term mappings from supplier_terms");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading supplier_term mappings");
        }

        return map;
    }

    private async Task<HashSet<int>> LoadValidUserIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT user_id FROM users WHERE user_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid user IDs from users");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid user IDs");
        }

        return validIds;
    }

    private async Task<int> InsertBatchWithCopyAsync(List<SupplierTermDeviationRecord> batch, NpgsqlConnection pgConn)
    {
        if (batch.Count == 0) return 0;

        try
        {
            const string copyCommand = @"COPY supplier_term_deviations (
                supplier_term_deviation_id, supplier_term_id, event_id, deviation_remarks,
                user_id, supplier_id, created_by, created_date, modified_by, modified_date,
                is_deleted, deleted_by, deleted_date, user_type
            ) FROM STDIN (FORMAT BINARY)";

            using var writer = await pgConn.BeginBinaryImportAsync(copyCommand);

            foreach (var record in batch)
            {
                await writer.StartRowAsync();
                await writer.WriteAsync(record.SupplierTermDeviationId, NpgsqlTypes.NpgsqlDbType.Integer);
                await writer.WriteAsync(record.SupplierTermId, NpgsqlTypes.NpgsqlDbType.Integer);
                
                if (record.EventId.HasValue)
                    await writer.WriteAsync(record.EventId.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                
                await writer.WriteAsync(record.DeviationRemarks, NpgsqlTypes.NpgsqlDbType.Text);
                
                if (record.UserId.HasValue)
                    await writer.WriteAsync(record.UserId.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                
                if (record.SupplierId.HasValue)
                    await writer.WriteAsync(record.SupplierId.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // created_by
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // created_date
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // modified_by
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // modified_date
                await writer.WriteAsync(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_deleted
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // deleted_by
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // deleted_date
                
                if (!string.IsNullOrEmpty(record.UserType))
                    await writer.WriteAsync(record.UserType, NpgsqlTypes.NpgsqlDbType.Text);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Text);
            }

            await writer.CompleteAsync();
            return batch.Count;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records with COPY");
            throw;
        }
    }
}
