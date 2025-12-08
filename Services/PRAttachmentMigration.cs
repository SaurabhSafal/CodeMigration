using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class PRAttachmentMigration : MigrationService
{
    // Reduced batch size for binary data to prevent memory issues
    private const int BATCH_SIZE = 50; // Smaller batch size for binary data
    private const int BATCH_SIZE_WITHOUT_BINARY = 500; // Larger batch when skipping binary
    private const int PROGRESS_UPDATE_INTERVAL = 50;
    private const long MAX_BINARY_SIZE = 50 * 1024 * 1024; // 50 MB max per file
    
    private readonly ILogger<PRAttachmentMigration> _logger;
    private bool _skipBinaryData = false; // Option to skip binary data for faster metadata migration
    private System.Diagnostics.Stopwatch _stopwatch = new System.Diagnostics.Stopwatch();

    protected override string SelectQuery => @"
SELECT
    pa.PRATTACHMENTID,
    pa.PRID,
    pa.UPLOADPATH,
    pa.FILENAME,
    pa.UPLOADEDBYID,
    pa.PRType,
    pa.PRNo,
    pa.ItemCode,
    pt.PRTRANSID,
    pa.Remarks,
    pa.PRATTACHMENTDATA,
    pa.PR_ATTCHMNT_TYPE,
    0 AS created_by,
    NULL AS created_date,
    0 AS modified_by,
    NULL AS modified_date,
    0 AS is_deleted,
    NULL AS deleted_by,
    NULL AS deleted_date
FROM TBL_PRATTACHMENT pa
LEFT JOIN TBL_PRTRANSACTION pt ON pt.PRID = pa.PRID
";

    protected override string InsertQuery => @"
INSERT INTO pr_attachments (
    pr_attachment_id, erp_pr_lines_id, upload_path, file_name, remarks, is_header_doc, pr_attachment_data, pr_attachment_extensions, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @pr_attachment_id, @erp_pr_lines_id, @upload_path, @file_name, @remarks, @is_header_doc, @pr_attachment_data, @pr_attachment_extensions, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)
ON CONFLICT (pr_attachment_id) DO UPDATE SET
    erp_pr_lines_id = EXCLUDED.erp_pr_lines_id,
    upload_path = EXCLUDED.upload_path,
    file_name = EXCLUDED.file_name,
    remarks = EXCLUDED.remarks,
    is_header_doc = EXCLUDED.is_header_doc,
    pr_attachment_data = EXCLUDED.pr_attachment_data,
    pr_attachment_extensions = EXCLUDED.pr_attachment_extensions,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    public PRAttachmentMigration(IConfiguration configuration, ILogger<PRAttachmentMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // pr_attachment_id
        "Direct", // erp_pr_lines_id
        "Direct", // upload_path
        "Direct", // file_name
        "Direct", // remarks
        "Direct", // is_header_doc
        "Direct", // pr_attachment_data
        "Direct", // pr_attachment_extensions
        "Direct", // created_by
        "Direct", // created_date
        "Direct", // modified_by
        "Direct", // modified_date
        "Direct", // is_deleted
        "Direct", // deleted_by
        "Direct"  // deleted_date
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PRATTACHMENTID", logic = "PRATTACHMENTID -> pr_attachment_id (Direct)", target = "pr_attachment_id" },
            new { source = "PRTRANSID", logic = "PRTRANSID (from JOIN) -> erp_pr_lines_id (Direct)", target = "erp_pr_lines_id" },
            new { source = "UPLOADPATH", logic = "UPLOADPATH -> upload_path (Direct)", target = "upload_path" },
            new { source = "FILENAME", logic = "FILENAME -> file_name (Direct)", target = "file_name" },
            new { source = "Remarks", logic = "Remarks -> remarks (Direct)", target = "remarks" },
            new { source = "-", logic = "is_header_doc -> true (Fixed Default)", target = "is_header_doc" },
            new { source = "PRATTACHMENTDATA", logic = "PRATTACHMENTDATA -> pr_attachment_data (Direct Binary)", target = "pr_attachment_data" },
            new { source = "PR_ATTCHMNT_TYPE", logic = "PR_ATTCHMNT_TYPE -> pr_attachment_extensions (Direct)", target = "pr_attachment_extensions" },
            new { source = "-", logic = "created_by -> 0 (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> 0 (Fixed Default)", target = "modified_by" },
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

    public async Task<int> MigrateWithOptionsAsync(bool skipBinaryData)
    {
        _skipBinaryData = skipBinaryData;
        if (_skipBinaryData)
        {
            _logger.LogWarning("⚠️ Binary data will be SKIPPED for faster migration. Only metadata will be migrated.");
        }
        return await base.MigrateAsync(useTransaction: true);
    }

    private async Task<HashSet<int>> LoadValidErpPrLinesIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT erp_pr_lines_id FROM erp_pr_lines";
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        return validIds;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _stopwatch.Restart();
        
        // Get total count for progress tracking
        int totalRecords = 0;
        using (var countCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_PRATTACHMENT", sqlConn))
        {
            totalRecords = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
        }
        _logger.LogInformation($"📊 Total records to migrate: {totalRecords:N0}");
        
        // Load valid ERP PR Lines IDs
        _logger.LogInformation("🔍 Loading valid ERP PR Lines IDs from erp_pr_lines...");
        var validErpPrLinesIds = await LoadValidErpPrLinesIdsAsync(pgConn, transaction);
        _logger.LogInformation($"✓ Loaded {validErpPrLinesIds.Count:N0} valid ERP PR Lines IDs");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int skippedLargeBinary = 0;
        int processedCount = 0;
        int batchNumber = 0;
        long totalBinarySize = 0;
        var batch = new List<Dictionary<string, object>>();

        int currentBatchSize = _skipBinaryData ? BATCH_SIZE_WITHOUT_BINARY : BATCH_SIZE;
        _logger.LogInformation($"⚙️ Batch size: {currentBatchSize} (Binary data: {(_skipBinaryData ? "SKIPPED" : "INCLUDED")})");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 600; // 10 minutes timeout for binary data
        
        _logger.LogInformation("📖 Reading data from TBL_PRATTACHMENT...");
        
        // Use SequentialAccess for streaming binary data efficiently
        using var reader = await selectCmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess);

        while (await reader.ReadAsync())
        {
            processedCount++;
            
            try
            {
                // CRITICAL: With SequentialAccess, MUST read columns in SELECT order
                // Order: PRATTACHMENTID, PRID, UPLOADPATH, FILENAME, UPLOADEDBYID, PRType, PRNo, ItemCode, 
                //        PRTRANSID, Remarks, PRATTACHMENTDATA, PR_ATTCHMNT_TYPE, created_by, created_date, 
                //        modified_by, modified_date, is_deleted, deleted_by, deleted_date
                
                // Column 0: PRATTACHMENTID
                var prAttachmentId = reader.IsDBNull(0) ? DBNull.Value : (object)reader.GetInt32(0);
                
                // Column 1: PRID - skip
                // Column 2: UPLOADPATH
                var uploadPath = reader.IsDBNull(2) ? DBNull.Value : (object)reader.GetString(2);
                
                // Column 3: FILENAME
                var fileName = reader.IsDBNull(3) ? DBNull.Value : (object)reader.GetString(3);
                
                // Columns 4-7: UPLOADEDBYID, PRType, PRNo, ItemCode - skip
                
                // Column 8: PRTRANSID
                var prTransId = reader.IsDBNull(8) ? DBNull.Value : (object)reader.GetInt32(8);
                
                // Validate ERP PR Lines ID
                if (prTransId == DBNull.Value)
                {
                    if (skippedCount < 10) // Log first 10
                    {
                        _logger.LogWarning($"⚠️ Skipping PRATTACHMENTID {prAttachmentId}: PRTRANSID is NULL");
                    }
                    skippedCount++;
                    continue;
                }
                
                int erpPrLinesId = Convert.ToInt32(prTransId);
                if (!validErpPrLinesIds.Contains(erpPrLinesId))
                {
                    if (skippedCount < 10) // Log first 10
                    {
                        _logger.LogWarning($"⚠️ Skipping PRATTACHMENTID {prAttachmentId}: ERP PR Lines ID {erpPrLinesId} not found");
                    }
                    skippedCount++;
                    continue;
                }
                
                // Column 9: Remarks
                var remarks = reader.IsDBNull(9) ? DBNull.Value : (object)reader.GetString(9);
                
                // Column 10: PRATTACHMENTDATA (BINARY - must read in order)
                byte[]? binaryData = null;
                long binarySize = 0;
                
                if (!_skipBinaryData)
                {
                    if (!reader.IsDBNull(10))
                    {
                        // Get size first
                        binarySize = reader.GetBytes(10, 0, null, 0, 0);
                        
                        if (binarySize > 0)
                        {
                            // Check if size is reasonable
                            if (binarySize > MAX_BINARY_SIZE)
                            {
                                _logger.LogWarning($"⚠️ PRATTACHMENTID {prAttachmentId}: Binary data too large ({binarySize / 1024.0 / 1024.0:F2} MB), skipping");
                                skippedLargeBinary++;
                                binaryData = null;
                            }
                            else
                            {
                                // Read binary data in chunks for memory efficiency
                                binaryData = new byte[binarySize];
                                long bytesRead = 0;
                                int bufferSize = 8192; // 8 KB chunks
                                
                                while (bytesRead < binarySize)
                                {
                                    long bytesToRead = Math.Min(bufferSize, binarySize - bytesRead);
                                    long actualRead = reader.GetBytes(10, bytesRead, binaryData, (int)bytesRead, (int)bytesToRead);
                                    bytesRead += actualRead;
                                }
                                
                                totalBinarySize += binarySize;
                            }
                        }
                    }
                }
                
                // Column 11: PR_ATTCHMNT_TYPE
                var prAttchmntType = reader.IsDBNull(11) ? DBNull.Value : (object)reader.GetString(11);
                
                // Column 12: created_by
                var createdBy = reader.IsDBNull(12) ? DBNull.Value : (object)reader.GetInt32(12);
                
                // Column 13: created_date
                var createdDate = reader.IsDBNull(13) ? DBNull.Value : (object)reader.GetDateTime(13);
                
                // Column 14: modified_by
                var modifiedBy = reader.IsDBNull(14) ? DBNull.Value : (object)reader.GetInt32(14);
                
                // Column 15: modified_date
                var modifiedDate = reader.IsDBNull(15) ? DBNull.Value : (object)reader.GetDateTime(15);
                
                // Column 16: is_deleted
                var isDeleted = !reader.IsDBNull(16) && reader.GetInt32(16) == 1;
                
                // Column 17: deleted_by
                var deletedBy = reader.IsDBNull(17) ? DBNull.Value : (object)reader.GetInt32(17);
                
                // Column 18: deleted_date
                var deletedDate = reader.IsDBNull(18) ? DBNull.Value : (object)reader.GetDateTime(18);

                var record = new Dictionary<string, object>
                {
                    ["pr_attachment_id"] = prAttachmentId,
                    ["erp_pr_lines_id"] = prTransId,
                    ["upload_path"] = uploadPath,
                    ["file_name"] = fileName,
                    ["remarks"] = remarks,
                    ["is_header_doc"] = true,
                    ["pr_attachment_data"] = binaryData != null ? (object)binaryData : DBNull.Value,
                    ["pr_attachment_extensions"] = prAttchmntType,
                    ["created_by"] = createdBy,
                    ["created_date"] = createdDate,
                    ["modified_by"] = modifiedBy,
                    ["modified_date"] = modifiedDate,
                    ["is_deleted"] = isDeleted,
                    ["deleted_by"] = deletedBy,
                    ["deleted_date"] = deletedDate,
                    ["_binary_size"] = binarySize // For logging only
                };

                batch.Add(record);

                if (batch.Count >= currentBatchSize)
                {
                    batchNumber++;
                    int batchInserted = await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                    insertedCount += batchInserted;
                    
                    long batchBinarySize = batch.Sum(r => (long)r["_binary_size"]);
                    _logger.LogInformation($"✓ Batch {batchNumber}: {batchInserted} records inserted ({batchBinarySize / 1024.0 / 1024.0:F2} MB)");
                    
                    batch.Clear();
                }

                // Progress update
                if (processedCount % PROGRESS_UPDATE_INTERVAL == 0 || processedCount == totalRecords)
                {
                    var elapsed = _stopwatch.Elapsed;
                    var recordsPerSecond = processedCount / elapsed.TotalSeconds;
                    var estimatedTimeRemaining = totalRecords > processedCount 
                        ? TimeSpan.FromSeconds((totalRecords - processedCount) / recordsPerSecond) 
                        : TimeSpan.Zero;
                    
                    _logger.LogInformation(
                        $"📈 Progress: {processedCount:N0}/{totalRecords:N0} ({(processedCount * 100.0 / totalRecords):F1}%) " +
                        $"| Inserted: {insertedCount:N0} | Skipped: {skippedCount:N0} " +
                        $"| Binary: {totalBinarySize / 1024.0 / 1024.0:F2} MB " +
                        $"| Speed: {recordsPerSecond:F0} rec/s | ETA: {estimatedTimeRemaining:hh\\:mm\\:ss}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"❌ Error processing record at position {processedCount}: {ex.Message}");
                throw;
            }
        }

        // Insert remaining batch
        if (batch.Count > 0)
        {
            batchNumber++;
            int batchInserted = await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            insertedCount += batchInserted;
            _logger.LogInformation($"✓ Final batch {batchNumber}: {batchInserted} records inserted");
        }

        _stopwatch.Stop();
        _logger.LogInformation(
            $"✅ PRAttachment migration completed! " +
            $"Total: {processedCount:N0} | Inserted: {insertedCount:N0} | Skipped: {skippedCount:N0} " +
            $"| Large files skipped: {skippedLargeBinary:N0} | Binary data: {totalBinarySize / 1024.0 / 1024.0:F2} MB " +
            $"| Duration: {_stopwatch.Elapsed:hh\\:mm\\:ss}");
        
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        try
        {
            // Deduplicate by pr_attachment_id - keep last occurrence
            var deduplicatedBatch = batch
                .GroupBy(r => r["pr_attachment_id"])
                .Select(g => g.Last())
                .ToList();

            if (deduplicatedBatch.Count < batch.Count)
            {
                _logger.LogWarning($"⚠️ Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate records");
            }

            var columns = new List<string> {
                "pr_attachment_id", "erp_pr_lines_id", "upload_path", "file_name", "remarks", 
                "is_header_doc", "pr_attachment_data", "pr_attachment_extensions", 
                "created_by", "created_date", "modified_by", "modified_date", 
                "is_deleted", "deleted_by", "deleted_date"
            };

            var valueRows = new List<string>();
            var parameters = new List<NpgsqlParameter>();
            int paramIndex = 0;

            foreach (var record in deduplicatedBatch)
            {
                var valuePlaceholders = new List<string>();
                foreach (var col in columns)
                {
                    var paramName = $"@p{paramIndex}";
                    valuePlaceholders.Add(paramName);
                    
                    // Optimize binary data parameter
                    if (col == "pr_attachment_data" && record[col] is byte[] binaryData)
                    {
                        parameters.Add(new NpgsqlParameter(paramName, NpgsqlTypes.NpgsqlDbType.Bytea) { Value = binaryData });
                    }
                    else
                    {
                        parameters.Add(new NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                    }
                    paramIndex++;
                }
                valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
            }

            var updateColumns = columns.Where(c => c != "pr_attachment_id" && c != "created_by" && c != "created_date").ToList();
            var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));
            
            var sql = $@"INSERT INTO pr_attachments ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (pr_attachment_id) DO UPDATE SET {updateSet}";
            
            using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
            insertCmd.CommandTimeout = 600; // 10 minutes timeout for binary inserts
            insertCmd.Parameters.AddRange(parameters.ToArray());

            int result = await insertCmd.ExecuteNonQueryAsync();
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError($"❌ Error inserting batch {batchNumber} of {batch.Count} records: {ex.Message}");
            throw;
        }
    }
}
