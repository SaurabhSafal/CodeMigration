using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class TermMasterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<TermMasterMigration> _logger;
    private MigrationLogger? _migrationLogger;

    protected override string SelectQuery => @"
        SELECT
            TERMID,
            TERMDESCRIPTION
        FROM TBL_CLAUSETERMMASTER
        ORDER BY TERMID";

    protected override string InsertQuery => @"
        INSERT INTO term_master (
            term_master_id,
            term_description,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @term_master_id,
            @term_description,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (term_master_id) DO UPDATE SET
            term_description = EXCLUDED.term_description,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public TermMasterMigration(IConfiguration configuration, ILogger<TermMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // term_master_id
            "Direct", // term_description
            "Fixed",  // created_by
            "Fixed",  // created_date
            "Fixed",  // modified_by
            "Fixed",  // modified_date
            "Fixed",  // is_deleted
            "Fixed",  // deleted_by
            "Fixed"   // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "TERMID", logic = "TERMID -> term_master_id (Primary key, autoincrement)", target = "term_master_id" },
            new { source = "TERMDESCRIPTION", logic = "TERMDESCRIPTION -> term_description (Direct)", target = "term_description" },
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
        _migrationLogger = new MigrationLogger(_logger, "term_master");
        _migrationLogger.LogInfo("Starting migration");

        _logger.LogInformation("Starting Term Master migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;
        var skippedRecordsList = new List<(string RecordId, string Reason)>();

        try
        {
            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var termId = reader["TERMID"];
                var termDescription = reader["TERMDESCRIPTION"];

                // Skip if TERMID is NULL
                if (termId == DBNull.Value)
                {
                    skippedRecords++;
                    string reason = "TERMID is NULL";
                    _logger.LogWarning($"Skipping record - {reason}");
                    skippedRecordsList.Add(("", reason));
                    continue;
                }

                int termIdValue = Convert.ToInt32(termId);

                // Skip duplicates
                if (processedIds.Contains(termIdValue))
                {
                    skippedRecords++;
                    string reason = $"Duplicate TERMID {termIdValue}";
                    skippedRecordsList.Add((termIdValue.ToString(), reason));
                    continue;
                }

                var record = new Dictionary<string, object>
                {
                    ["term_master_id"] = termIdValue,
                    ["term_description"] = termDescription ?? DBNull.Value,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(termIdValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchAsync(batch, pgConn, transaction);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchAsync(batch, pgConn, transaction);
                migratedRecords += batchMigrated;
            }

            // Export migration statistics to Excel
            string outputPath = System.IO.Path.Combine("migration_outputs", $"TermMasterMigrationStats_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx");
            MigrationStatsExporter.ExportToExcel(
                outputPath,
                totalRecords,
                migratedRecords,
                skippedRecords,
                _logger,
                skippedRecordsList
            );
            _logger.LogInformation($"Migration statistics exported to {outputPath}");

            _logger.LogInformation($"Term Master migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during Term Master migration");
            throw;
        }
    }

    private async Task<int> InsertBatchAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        int insertedCount = 0;

        try
        {
            foreach (var record in batch)
            {
                using var cmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);

                foreach (var kvp in record)
                {
                    cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                }

                await cmd.ExecuteNonQueryAsync();
                insertedCount++;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error inserting batch of {batch.Count} records");
            throw;
        }

        return insertedCount;
    }
}
