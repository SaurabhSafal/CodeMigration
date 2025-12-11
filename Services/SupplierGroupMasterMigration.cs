using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Common.Helpers;
using System.Diagnostics;
using System.Threading;
using DataMigration.Services;

public class SupplierGroupMasterMigration : MigrationService
{
    private readonly ILogger<SupplierGroupMasterMigration> _logger;
    private MigrationLogger? _migrationLogger;
    private const int BATCH_SIZE = 1000; // Process in batches of 1000 records
    private const int PROGRESS_UPDATE_INTERVAL = 100; // Update progress every 100 records

    // SQL Server: TBL_VendorGroupMaster -> PostgreSQL: supplier_groupmaster
    protected override string SelectQuery => @"
        SELECT 
            VendorGroupId,
            VendorGroupCode,
            VendorGroupName,
            Password,
            GSTIN,
            City,
            PostalCode,
            CreatedBy,
            CreatedDate,
            PANNO
        FROM TBL_VendorGroupMaster
        ORDER BY VendorGroupId";

    protected override string InsertQuery => @"
        INSERT INTO supplier_groupmaster (
            supplier_group_master_id,
            supplier_group_code,
            supplier_group_name,
            password,
            gst_number,
            supplier_city,
            zip_code,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_group_master_id,
            @supplier_group_code,
            @supplier_group_name,
            @password,
            @gst_number,
            @supplier_city,
            @zip_code,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    // Optimized batch insert query
    private readonly string BatchInsertQuery = @"
        INSERT INTO supplier_groupmaster (
            supplier_group_master_id,
            supplier_group_code,
            supplier_group_name,
            password,
            gst_number,
            supplier_city,
            zip_code,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES ";

    public SupplierGroupMasterMigration(IConfiguration configuration, ILogger<SupplierGroupMasterMigration> logger) : base(configuration)
    {
        _logger = logger; }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "VendorGroupId -> supplier_group_master_id (Direct)",
            "VendorGroupCode -> supplier_group_code (Direct)",
            "VendorGroupName -> supplier_group_name (Direct)",
            "Password -> password (Base64 Encoded)",
            "GSTIN -> gst_number (Direct)",
            "City -> supplier_city (Direct)",
            "PostalCode -> zip_code (Direct)",
            "CreatedBy -> created_by (Direct)",
            "CreatedDate -> created_date (Direct)",
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
            new { source = "VendorGroupId", logic = "VendorGroupId -> supplier_group_master_id (Direct)", target = "supplier_group_master_id" },
            new { source = "VendorGroupCode", logic = "VendorGroupCode -> supplier_group_code (Direct)", target = "supplier_group_code" },
            new { source = "VendorGroupName", logic = "VendorGroupName -> supplier_group_name (Direct)", target = "supplier_group_name" },
            new { source = "Password", logic = "Password -> password (Base64 Encoded)", target = "password" },
            new { source = "GSTIN", logic = "GSTIN -> gst_number (Direct)", target = "gst_number" },
            new { source = "City", logic = "City -> supplier_city (Direct)", target = "supplier_city" },
            new { source = "PostalCode", logic = "PostalCode -> zip_code (Direct)", target = "zip_code" },
            new { source = "CreatedBy", logic = "CreatedBy -> created_by (Direct)", target = "created_by" },
            new { source = "CreatedDate", logic = "CreatedDate -> created_date (Direct)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await MigrateAsync(useTransaction: true);
    }

    public async Task<int> MigrateAsync(IMigrationProgress? progress = null)
    {
        return await MigrateAsync(useTransaction: true, progress);
    }

    public async Task<int> MigrateAsync(bool useTransaction, IMigrationProgress? progress = null)
    {
        progress ??= new ConsoleMigrationProgress();
        
        SqlConnection? sqlConn = null;
        NpgsqlConnection? pgConn = null;
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            sqlConn = GetSqlServerConnection();
            pgConn = GetPostgreSqlConnection();
            
            // Configure SQL connection for large datasets
            sqlConn.ConnectionString = sqlConn.ConnectionString + ";Connection Timeout=300;Command Timeout=300;";
            
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            progress.ReportProgress(0, 0, "Estimating total records...", stopwatch.Elapsed);
            
            // Get total count first for progress reporting
            int totalRecords = await GetTotalRecordsAsync(sqlConn);
            
            if (useTransaction)
            {
                using var transaction = await pgConn.BeginTransactionAsync();
                try
                {
                    int result = await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
                    await transaction.CommitAsync();
                    return result;
                }
                catch (Exception)
                {
                    await transaction.RollbackAsync();
                    throw;
                }
            }
            else
            {
                return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch);
            }
        }
        finally
        {
            sqlConn?.Dispose();
            pgConn?.Dispose();
        }
    }

    private async Task<int> GetTotalRecordsAsync(SqlConnection sqlConn)
    {
        using var cmd = new SqlCommand("SELECT COUNT(*) FROM TBL_VendorGroupMaster", sqlConn);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }

    private async Task<int> ExecuteOptimizedMigrationAsync(
        SqlConnection sqlConn, 
        NpgsqlConnection pgConn, 
        int totalRecords, 
        IMigrationProgress progress, 
        Stopwatch stopwatch, 
        NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "supplier_groupmaster");
        _migrationLogger.LogInfo("Starting migration");

        var insertedCount = 0;
        var processedCount = 0;
        var skippedCount = 0;
        var batch = new List<SupplierGroupRecord>();

        progress.ReportProgress(0, totalRecords, "Starting SupplierGroupMaster migration...", stopwatch.Elapsed);

        // Use streaming reader for memory efficiency
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        sqlCmd.CommandTimeout = 300; // 5 minutes timeout
        
        using var reader = await sqlCmd.ExecuteReaderAsync();

        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                
                try
                {
                    var record = ReadSupplierGroupRecord(reader, processedCount);
                    
                    if (record != null)
                    {
                        batch.Add(record);
                        _migrationLogger?.LogInserted($"VendorGroupId={record.SupplierGroupMasterId}");
                    }
                    else
                    {
                        skippedCount++;
                        _migrationLogger?.LogSkipped("Record is null", $"Record={processedCount}");
                    }

                    // Process batch when it reaches the batch size or it's the last record
                    if (batch.Count >= BATCH_SIZE || processedCount == totalRecords)
                    {
                        if (batch.Count > 0)
                        {
                            progress.ReportProgress(processedCount, totalRecords, 
                                $"Processing batch of {batch.Count} records...", stopwatch.Elapsed);
                            
                            int batchInserted = await InsertBatchAsync(pgConn, batch, transaction);
                            insertedCount += batchInserted;
                            
                            batch.Clear();
                        }
                    }

                    // Update progress periodically
                    if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
                    {
                        progress.ReportProgress(processedCount, totalRecords, 
                            $"Processed: {processedCount:N0}, Inserted: {insertedCount:N0}, Skipped: {skippedCount:N0}", 
                            stopwatch.Elapsed);
                    }
                }
                catch (Exception recordEx)
                {
                    skippedCount++;
                    _migrationLogger?.LogSkipped(recordEx.Message, $"Record={processedCount}");
                    progress.ReportError($"Error processing record {processedCount}: {recordEx.Message}", processedCount);
                }
            }
        }
        catch (Exception ex)
        {
            progress.ReportError($"Migration failed after processing {processedCount} records: {ex.Message}", processedCount);
        }

        stopwatch.Stop();
        progress.ReportCompleted(processedCount, insertedCount, stopwatch.Elapsed);

        // Export migration stats to Excel
        try
        {
            var outputPath = $"SupplierGroupMasterMigrationStats_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
            var skippedRecordsList = _migrationLogger?.GetSkippedRecords().Select(x => (x.RecordIdentifier, x.Message)).ToList() ?? new List<(string, string)>();
            MigrationStatsExporter.ExportToExcel(outputPath, totalRecords, insertedCount, skippedCount, _logger, skippedRecordsList);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Failed to export migration stats: {ex.Message}");
        }
        
        return insertedCount;
    }

    private SupplierGroupRecord? ReadSupplierGroupRecord(SqlDataReader reader, int recordNumber)
    {
        try
        {
            var vendorGroupId = reader.IsDBNull(reader.GetOrdinal("VendorGroupId")) ? 0 : Convert.ToInt32(reader["VendorGroupId"]);
            var vendorGroupCode = reader.IsDBNull(reader.GetOrdinal("VendorGroupCode")) ? "" : reader["VendorGroupCode"].ToString();
            var vendorGroupName = reader.IsDBNull(reader.GetOrdinal("VendorGroupName")) ? "" : reader["VendorGroupName"].ToString();
            var password = reader.IsDBNull(reader.GetOrdinal("Password")) ? "" : reader["Password"].ToString();
            var gstin = reader.IsDBNull(reader.GetOrdinal("GSTIN")) ? "" : reader["GSTIN"].ToString();
            var city = reader.IsDBNull(reader.GetOrdinal("City")) ? "" : reader["City"].ToString();
            var postalCode = reader.IsDBNull(reader.GetOrdinal("PostalCode")) ? "" : reader["PostalCode"].ToString();
            var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? 0 : Convert.ToInt32(reader["CreatedBy"]);
            var createdDate = reader.IsDBNull(reader.GetOrdinal("CreatedDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["CreatedDate"]);

            // Encode password using Base64
            var encodedPassword = string.IsNullOrEmpty(password) ? "" : Base64Helper.EncodeToBase64(password);
            
            if (!string.IsNullOrEmpty(password))
            {
                Console.WriteLine($"Record {recordNumber} (VendorGroupId: {vendorGroupId}) - Encoded password");
            }

            return new SupplierGroupRecord
            {
                SupplierGroupMasterId = vendorGroupId,
                SupplierGroupCode = vendorGroupCode ?? "",
                SupplierGroupName = vendorGroupName ?? "",
                Password = encodedPassword,
                GstNumber = gstin ?? "",
                SupplierCity = city ?? "",
                ZipCode = postalCode ?? "",
                CreatedBy = createdBy,
                CreatedDate = createdDate
            };
        }
        catch (Exception ex)
        {
            throw new Exception($"Error reading supplier group record {recordNumber}: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<SupplierGroupRecord> batch, NpgsqlTransaction? transaction = null)
    {
        if (batch.Count == 0) return 0;

        try
        {
            // Use COPY for maximum performance with large batches
            if (batch.Count >= 100)
            {
                return await InsertBatchWithCopyAsync(pgConn, batch, transaction);
            }
            else
            {
                return await InsertBatchWithParametersAsync(pgConn, batch, transaction);
            }
        }
        catch (Exception ex)
        {
            throw new Exception($"Error inserting batch of {batch.Count} records: {ex.Message}", ex);
        }
    }

    private async Task<int> InsertBatchWithCopyAsync(NpgsqlConnection pgConn, List<SupplierGroupRecord> batch, NpgsqlTransaction? transaction = null)
    {
        var copyCommand = $"COPY supplier_groupmaster (supplier_group_master_id, supplier_group_code, supplier_group_name, password, gst_number, supplier_city, zip_code, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) FROM STDIN (FORMAT BINARY)";
        
        using var writer = transaction != null ? 
            await pgConn.BeginBinaryImportAsync(copyCommand, CancellationToken.None) : 
            await pgConn.BeginBinaryImportAsync(copyCommand);
        
        foreach (var record in batch)
        {
            await writer.StartRowAsync();
            await writer.WriteAsync(record.SupplierGroupMasterId);
            await writer.WriteAsync(record.SupplierGroupCode);
            await writer.WriteAsync(record.SupplierGroupName);
            await writer.WriteAsync(record.Password);
            await writer.WriteAsync(record.GstNumber);
            await writer.WriteAsync(record.SupplierCity);
            await writer.WriteAsync(record.ZipCode);
            await writer.WriteAsync(record.CreatedBy);
            await writer.WriteAsync(record.CreatedDate);
            await writer.WriteAsync(DBNull.Value); // modified_by
            await writer.WriteAsync(DBNull.Value); // modified_date
            await writer.WriteAsync(false); // is_deleted
            await writer.WriteAsync(DBNull.Value); // deleted_by
            await writer.WriteAsync(DBNull.Value); // deleted_date
        }
        
        await writer.CompleteAsync();
        return batch.Count;
    }

    private async Task<int> InsertBatchWithParametersAsync(NpgsqlConnection pgConn, List<SupplierGroupRecord> batch, NpgsqlTransaction? transaction = null)
    {
        // Build multi-row insert statement
        var values = new List<string>();
        var parameters = new List<NpgsqlParameter>();

        for (int i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            var paramPrefix = $"p{i}";
            
            values.Add($"(@{paramPrefix}_supplier_group_master_id, @{paramPrefix}_supplier_group_code, @{paramPrefix}_supplier_group_name, @{paramPrefix}_password, @{paramPrefix}_gst_number, @{paramPrefix}_supplier_city, @{paramPrefix}_zip_code, @{paramPrefix}_created_by, @{paramPrefix}_created_date, @{paramPrefix}_modified_by, @{paramPrefix}_modified_date, @{paramPrefix}_is_deleted, @{paramPrefix}_deleted_by, @{paramPrefix}_deleted_date)");
            
            parameters.AddRange(new[]
            {
                new NpgsqlParameter($"@{paramPrefix}_supplier_group_master_id", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.SupplierGroupMasterId },
                new NpgsqlParameter($"@{paramPrefix}_supplier_group_code", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.SupplierGroupCode },
                new NpgsqlParameter($"@{paramPrefix}_supplier_group_name", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.SupplierGroupName },
                new NpgsqlParameter($"@{paramPrefix}_password", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.Password },
                new NpgsqlParameter($"@{paramPrefix}_gst_number", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.GstNumber },
                new NpgsqlParameter($"@{paramPrefix}_supplier_city", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.SupplierCity },
                new NpgsqlParameter($"@{paramPrefix}_zip_code", NpgsqlTypes.NpgsqlDbType.Text) { Value = record.ZipCode },
                new NpgsqlParameter($"@{paramPrefix}_created_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = record.CreatedBy },
                new NpgsqlParameter($"@{paramPrefix}_created_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = record.CreatedDate },
                new NpgsqlParameter($"@{paramPrefix}_modified_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_modified_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_is_deleted", NpgsqlTypes.NpgsqlDbType.Boolean) { Value = false },
                new NpgsqlParameter($"@{paramPrefix}_deleted_by", NpgsqlTypes.NpgsqlDbType.Integer) { Value = DBNull.Value },
                new NpgsqlParameter($"@{paramPrefix}_deleted_date", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = DBNull.Value }
            });
        }

        var query = BatchInsertQuery + string.Join(", ", values);
        
        using var cmd = new NpgsqlCommand(query, pgConn);
        if (transaction != null)
        {
            cmd.Transaction = transaction;
        }
        
        cmd.Parameters.AddRange(parameters.ToArray());
        cmd.CommandTimeout = 300; // 5 minutes timeout
        
        return await cmd.ExecuteNonQueryAsync();
    }

    // Keep the original method for backward compatibility
    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "supplier_groupmaster");
        _migrationLogger.LogInfo("Starting migration");

        var progress = new ConsoleMigrationProgress();
        var stopwatch = Stopwatch.StartNew();
        
        // Get total records count
        int totalRecords = await GetTotalRecordsAsync(sqlConn);
        
        return await ExecuteOptimizedMigrationAsync(sqlConn, pgConn, totalRecords, progress, stopwatch, transaction);
    }

    private class SupplierGroupRecord
    {
        public int SupplierGroupMasterId { get; set; }
        public string SupplierGroupCode { get; set; } = "";
        public string SupplierGroupName { get; set; } = "";
        public string Password { get; set; } = "";
        public string GstNumber { get; set; } = "";
        public string SupplierCity { get; set; } = "";
        public string ZipCode { get; set; } = "";
        public int CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
    }
}
