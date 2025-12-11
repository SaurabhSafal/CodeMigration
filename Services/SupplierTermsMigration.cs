using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class SupplierTermsMigration : MigrationService
{
    private const int BATCH_SIZE = 5000; // Increased for COPY operations
    private readonly ILogger<SupplierTermsMigration> _logger;
    private readonly MigrationLogger migrationLogger;
    
    // Track skipped records for detailed reporting
    private List<(string RecordId, string Reason)> _skippedRecords = new List<(string, string)>();
    
    // Data class for batch processing
    private class SupplierTermRecord
    {
        public int SupplierTermId { get; set; }
        public int? EventId { get; set; }
        public int? SupplierId { get; set; }
        public int UserTermId { get; set; }
        public bool TermAccept { get; set; }
        public bool TermDeviate { get; set; }
        public DateTime? CreatedDate { get; set; }
    }

    protected override string SelectQuery => @"
        SELECT
            VENDORDEVIATIONMSTID,
            EVENTID,
            VENDORID,
            CLAUSEEVENTWISEID,
            ISACCEPT,
            ISDEVIATE,
            ENT_DATE,
            ISUPDATED
        FROM TBL_VENDORDEVIATIONMASTER
        ORDER BY VENDORDEVIATIONMSTID";

    protected override string InsertQuery => ""; // Not used with COPY

    public SupplierTermsMigration(IConfiguration configuration, ILogger<SupplierTermsMigration> logger) : base(configuration)
    {
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "SupplierTerms");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // supplier_term_id
            "Direct",  // event_id
            "Direct",  // supplier_id
            "Direct",  // user_term_id
            "Direct",  // term_accept
            "Direct",  // term_deviate
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
            new { source = "VENDORDEVIATIONMSTID", logic = "VENDORDEVIATIONMSTID -> supplier_term_id (Primary key, autoincrement - SupplierTermId)", target = "supplier_term_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Foreign key to event_master - EventId)", target = "event_id" },
            new { source = "VENDORID", logic = "VENDORID -> supplier_id (Foreign key to supplier_master - SupplierId)", target = "supplier_id" },
            new { source = "CLAUSEEVENTWISEID", logic = "CLAUSEEVENTWISEID -> user_term_id (Foreign key to user_term - UserTermId)", target = "user_term_id" },
            new { source = "ISACCEPT", logic = "ISACCEPT -> term_accept (Boolean - TermAccept)", target = "term_accept" },
            new { source = "ISDEVIATE", logic = "ISDEVIATE -> term_deviate (Boolean - TermDeviate)", target = "term_deviate" },
            new { source = "ENT_DATE", logic = "ENT_DATE -> Not mapped to target table", target = "-" },
            new { source = "ISUPDATED", logic = "ISUPDATED -> Not mapped to target table", target = "-" },
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
        _logger.LogInformation("Starting optimized Supplier Terms migration with COPY...");
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        int totalRecords = 0;
        int migratedRecords = 0;

        try
        {
            // Load valid IDs
            var validEventIds = await LoadValidEventIdsAsync(pgConn);
            var validSupplierIds = await LoadValidSupplierIdsAsync(pgConn);
            var validUserTermIds = await LoadValidUserTermIdsAsync(pgConn);
            _logger.LogInformation($"Loaded validation data: {validEventIds.Count} events, {validSupplierIds.Count} suppliers, {validUserTermIds.Count} user_terms");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;
            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<SupplierTermRecord>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                // Fast field access by ordinal
                var vendorDeviationMstId = reader.IsDBNull(0) ? (int?)null : reader.GetInt32(0);
                var eventId = reader.IsDBNull(1) ? (int?)null : reader.GetInt32(1);
                var vendorId = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2);
                var clauseEventWiseId = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
                var isAccept = reader.IsDBNull(4) ? 0 : reader.GetInt32(4);
                var isDeviate = reader.IsDBNull(5) ? 0 : reader.GetInt32(5);
                var entDate = reader.IsDBNull(6) ? (DateTime?)null : reader.GetDateTime(6);

                // Skip if primary key is NULL
                if (!vendorDeviationMstId.HasValue)
                {
                    migrationLogger.LogSkipped("VENDORDEVIATIONMSTID is NULL", "NULL");
                    _skippedRecords.Add(("NULL", "VENDORDEVIATIONMSTID is NULL"));
                    continue;
                }

                int id = vendorDeviationMstId.Value;

                // Skip duplicates
                if (processedIds.Contains(id))
                {
                    migrationLogger.LogSkipped("Duplicate record", id.ToString());
                    _skippedRecords.Add((id.ToString(), "Duplicate record"));
                    continue;
                }

                // Validate event_id if not null
                if (eventId.HasValue && !validEventIds.Contains(eventId.Value))
                {
                    migrationLogger.LogSkipped($"event_id={eventId.Value} not found", id.ToString());
                    _skippedRecords.Add((id.ToString(), $"Invalid event_id={eventId.Value}"));
                    continue;
                }

                // Validate supplier_id if not null
                if (vendorId.HasValue && !validSupplierIds.Contains(vendorId.Value))
                {
                    migrationLogger.LogSkipped($"supplier_id={vendorId.Value} not found", id.ToString());
                    _skippedRecords.Add((id.ToString(), $"Invalid supplier_id={vendorId.Value}"));
                    continue;
                }

                // Skip if user_term_id is NULL (NOT NULL constraint)
                if (!clauseEventWiseId.HasValue)
                {
                    migrationLogger.LogSkipped("user_term_id is NULL", id.ToString());
                    _skippedRecords.Add((id.ToString(), "user_term_id is NULL"));
                    continue;
                }

                // Validate user_term_id
                if (!validUserTermIds.Contains(clauseEventWiseId.Value))
                {
                    migrationLogger.LogSkipped($"user_term_id={clauseEventWiseId.Value} not found", id.ToString());
                    _skippedRecords.Add((id.ToString(), $"Invalid user_term_id={clauseEventWiseId.Value}"));
                    continue;
                }

                var record = new SupplierTermRecord
                {
                    SupplierTermId = id,
                    EventId = eventId,
                    SupplierId = vendorId,
                    UserTermId = clauseEventWiseId.Value,
                    TermAccept = isAccept == 1,
                    TermDeviate = isDeviate == 1,
                    CreatedDate = entDate
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
            _logger.LogInformation($"Supplier Terms migration completed in {stopwatch.Elapsed:mm\\:ss}. Total: {totalRecords:N0}, Migrated: {migratedRecords:N0}, Skipped: {totalRecords - migratedRecords:N0}, Rate: {totalRate:F1} records/sec");

            // Export migration statistics
            MigrationStatsExporter.ExportToExcel(
                "SupplierTerms_migration_stats.xlsx",
                totalRecords,
                migratedRecords,
                totalRecords - migratedRecords,
                _logger,
                _skippedRecords
            );

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Supplier Terms migration");
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

    private async Task<HashSet<int>> LoadValidSupplierIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT supplier_id FROM supplier_master WHERE supplier_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid supplier IDs from supplier_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid supplier IDs");
        }

        return validIds;
    }

    private async Task<HashSet<int>> LoadValidUserTermIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT user_term_id FROM user_term WHERE user_term_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid user_term IDs from user_term");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid user_term IDs");
        }

        return validIds;
    }

    private async Task<int> InsertBatchWithCopyAsync(List<SupplierTermRecord> batch, NpgsqlConnection pgConn)
    {
        if (batch.Count == 0) return 0;

        try
        {
            const string copyCommand = @"COPY supplier_terms (
                supplier_term_id, event_id, supplier_id, user_term_id, term_accept, term_deviate,
                created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
            ) FROM STDIN (FORMAT BINARY)";

            using var writer = await pgConn.BeginBinaryImportAsync(copyCommand);

            foreach (var record in batch)
            {
                await writer.StartRowAsync();
                await writer.WriteAsync(record.SupplierTermId, NpgsqlTypes.NpgsqlDbType.Integer);
                
                if (record.EventId.HasValue)
                    await writer.WriteAsync(record.EventId.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                
                if (record.SupplierId.HasValue)
                    await writer.WriteAsync(record.SupplierId.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer);
                
                await writer.WriteAsync(record.UserTermId, NpgsqlTypes.NpgsqlDbType.Integer);
                await writer.WriteAsync(record.TermAccept, NpgsqlTypes.NpgsqlDbType.Boolean);
                await writer.WriteAsync(record.TermDeviate, NpgsqlTypes.NpgsqlDbType.Boolean);
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // created_by
                
                if (record.CreatedDate.HasValue)
                    await writer.WriteAsync(record.CreatedDate.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                else
                    await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp);
                
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // modified_by
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // modified_date
                await writer.WriteAsync(false, NpgsqlTypes.NpgsqlDbType.Boolean); // is_deleted
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Integer); // deleted_by
                await writer.WriteAsync(DBNull.Value, NpgsqlTypes.NpgsqlDbType.Timestamp); // deleted_date
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
