using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using DataMigration.Services;

public class SupplierOtherContactMigration : MigrationService
{
    private readonly ILogger<SupplierOtherContactMigration> _logger;
    private MigrationLogger? _migrationLogger;
    private HashSet<int> _validSupplierIds = new HashSet<int>();
    private const int BATCH_SIZE = 500; // Optimized batch size
    private const int PROGRESS_UPDATE_INTERVAL = 100;
    protected override string SelectQuery => @"
        SELECT 
            ComunicationID,
            VendorID,
            Name,
            MobileNo,
            WhatsAppNo,
            Email,
            TimeZone,
            IsSales,
            IsSpares,
            IsService,
            OperationCapital,
            OperationSpare,
            OperationServices,
            IsFinance,
            AddDateTime
        FROM TBL_COMUNICATION
        ORDER BY ComunicationID";

    protected override string InsertQuery => @"
        INSERT INTO supplier_other_contact (
            supplier_other_contact_id,
            supplier_id,
            contact_name,
            contact_number,
            contact_email_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_other_contact_id,
            @supplier_id,
            @contact_name,
            @contact_number,
            @contact_email_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public SupplierOtherContactMigration(IConfiguration configuration, ILogger<SupplierOtherContactMigration> logger) : base(configuration) 
    { 
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "ComunicationID -> supplier_other_contact_id (Direct)",
            "VendorID -> supplier_id (Direct)",
            "Name -> contact_name (Direct)",
            "MobileNo -> contact_number (Direct)",
            "Email -> contact_email_id (Direct)",
            // Additional fields from MSSQL are ignored as they have no direct mapping
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ComunicationID", logic = "ComunicationID -> supplier_other_contact_id (Direct)", target = "supplier_other_contact_id" },
            new { source = "VendorID", logic = "VendorID -> supplier_id (Direct)", target = "supplier_id" },
            new { source = "Name", logic = "Name -> contact_name (Direct)", target = "contact_name" },
            new { source = "MobileNo", logic = "MobileNo -> contact_number (Direct)", target = "contact_number" },
            new { source = "Email", logic = "Email -> contact_email_id (Direct)", target = "contact_email_id" },
            new { source = "-", logic = "created_by -> 0 (Fixed)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "supplier_other_contact");
        _migrationLogger.LogInfo("Starting migration");

        var stopwatch = Stopwatch.StartNew();
        _logger.LogInformation("Starting SupplierOtherContact migration...");
        
        // Cache valid supplier_ids from supplier_master
        _logger.LogInformation("Loading valid supplier IDs from supplier_master...");
        _validSupplierIds = new HashSet<int>();
        using (var cmd = new NpgsqlCommand("SELECT supplier_id FROM supplier_master WHERE is_deleted = false OR is_deleted IS NULL", pgConn, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                _validSupplierIds.Add(reader.GetInt32(0));
            }
        }
        _logger.LogInformation($"Loaded {_validSupplierIds.Count} valid supplier IDs");

        // Get total count for progress tracking
        int totalRecords = 0;
        using (var countCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_COMUNICATION", sqlConn))
        {
            totalRecords = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
        }
        _logger.LogInformation($"Total records to process: {totalRecords}");

        int insertedCount = 0;
        int skippedCount = 0;
        int processedCount = 0;
        var batch = new List<Dictionary<string, object>>();
        
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300; // 5 minutes timeout
        
        _logger.LogInformation("Reading data from TBL_COMUNICATION...");
        using var reader2 = await selectCmd.ExecuteReaderAsync();
        
        while (await reader2.ReadAsync())
        {
            processedCount++;
            
            try
            {
                var supplierIdObj = reader2["VendorID"];
                int supplierId = supplierIdObj == DBNull.Value ? 0 : Convert.ToInt32(supplierIdObj);
                
                // Skip if supplier_id is not present in supplier_master
                if (!_validSupplierIds.Contains(supplierId))
                {
                    skippedCount++;
                    if (skippedCount <= 10) // Log first 10 skipped records
                    {
                        _logger.LogWarning($"Skipping ComunicationID {reader2["ComunicationID"]} - VendorID {supplierId} not found in supplier_master");
                    }
                    continue;
                }

                var record = new Dictionary<string, object>
                {
                    ["@supplier_other_contact_id"] = reader2["ComunicationID"],
                    ["@supplier_id"] = supplierId,
                    ["@contact_name"] = reader2["Name"] ?? (object)DBNull.Value,
                    ["@contact_number"] = reader2["MobileNo"] ?? (object)DBNull.Value,
                    ["@contact_email_id"] = reader2["Email"] ?? (object)DBNull.Value,
                    ["@created_by"] = 0,
                    ["@created_date"] = DateTime.UtcNow,
                    ["@modified_by"] = DBNull.Value,
                    ["@modified_date"] = DBNull.Value,
                    ["@is_deleted"] = false,
                    ["@deleted_by"] = DBNull.Value,
                    ["@deleted_date"] = DBNull.Value
                };
                batch.Add(record);
                
                if (batch.Count >= BATCH_SIZE)
                {
                    int batchInserted = await InsertBatchAsync(pgConn, batch, transaction);
                    insertedCount += batchInserted;
                    _logger.LogInformation($"Batch inserted: {batchInserted} records. Total: {insertedCount}/{processedCount} (Skipped: {skippedCount})");
                    batch.Clear();
                }
                
                // Progress update
                if (processedCount % PROGRESS_UPDATE_INTERVAL == 0)
                {
                    var elapsed = stopwatch.Elapsed;
                    var recordsPerSecond = processedCount / elapsed.TotalSeconds;
                    var estimatedTimeRemaining = TimeSpan.FromSeconds((totalRecords - processedCount) / recordsPerSecond);
                    
                    _logger.LogInformation(
                        $"Progress: {processedCount}/{totalRecords} ({(processedCount * 100.0 / totalRecords):F1}%) " +
                        $"| Inserted: {insertedCount} | Skipped: {skippedCount} " +
                        $"| Speed: {recordsPerSecond:F0} rec/s | ETA: {estimatedTimeRemaining:hh\\:mm\\:ss}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing record at position {processedCount}: {ex.Message}");
                throw;
            }
        }
        
        // Insert remaining batch
        if (batch.Count > 0)
        {
            int batchInserted = await InsertBatchAsync(pgConn, batch, transaction);
            insertedCount += batchInserted;
            _logger.LogInformation($"Final batch inserted: {batchInserted} records");
        }
        
        stopwatch.Stop();
        _logger.LogInformation(
            $"SupplierOtherContact migration completed. " +
            $"Total processed: {processedCount} | Inserted: {insertedCount} | Skipped: {skippedCount} | " +
            $"Duration: {stopwatch.Elapsed:hh\\:mm\\:ss}");
        
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction = null)
    {
        if (batch.Count == 0) return 0;

        try
        {
            // Use multi-row insert for better performance
            var values = new List<string>();
            var parameters = new List<NpgsqlParameter>();

            for (int i = 0; i < batch.Count; i++)
            {
                var record = batch[i];
                var paramPrefix = $"p{i}";
                
                values.Add($"(@{paramPrefix}_id, @{paramPrefix}_supplier, @{paramPrefix}_name, @{paramPrefix}_number, @{paramPrefix}_email, @{paramPrefix}_created_by, @{paramPrefix}_created_date, @{paramPrefix}_modified_by, @{paramPrefix}_modified_date, @{paramPrefix}_deleted, @{paramPrefix}_deleted_by, @{paramPrefix}_deleted_date)");
                
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_id", record["@supplier_other_contact_id"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_supplier", record["@supplier_id"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_name", record["@contact_name"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_number", record["@contact_number"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_email", record["@contact_email_id"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_created_by", record["@created_by"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_created_date", record["@created_date"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_modified_by", record["@modified_by"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_modified_date", record["@modified_date"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_deleted", record["@is_deleted"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_deleted_by", record["@deleted_by"]));
                parameters.Add(new NpgsqlParameter($"@{paramPrefix}_deleted_date", record["@deleted_date"]));
            }

            var query = @"
                INSERT INTO supplier_other_contact (
                    supplier_other_contact_id, supplier_id, contact_name, contact_number, contact_email_id,
                    created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
                ) VALUES " + string.Join(", ", values);

            using var cmd = new NpgsqlCommand(query, pgConn, transaction);
            cmd.Parameters.AddRange(parameters.ToArray());
            cmd.CommandTimeout = 300;
            
            int result = await cmd.ExecuteNonQueryAsync();
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error inserting batch of {batch.Count} records: {ex.Message}");
            throw;
        }
    }
}

