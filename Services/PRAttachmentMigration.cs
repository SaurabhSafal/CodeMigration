using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class PRAttachmentMigration : MigrationService
{
    private const int BATCH_SIZE = 100; // Reduced for large binary data
    private readonly ILogger<PRAttachmentMigration> _logger;

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
)";

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

    public async Task<int> MigrateAsync()
    {
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
        // Load valid ERP PR Lines IDs
        var validErpPrLinesIds = await LoadValidErpPrLinesIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validErpPrLinesIds.Count} valid ERP PR Lines IDs from erp_pr_lines.");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300; // Increase timeout for binary data
        
        using var reader = await selectCmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess);

        while (await reader.ReadAsync())
        {
            // Read all columns in STRICT SEQUENTIAL ORDER by ordinal (required for SequentialAccess)
            // Column 0: PRATTACHMENTID
            var prAttachmentId = reader.IsDBNull(0) ? DBNull.Value : (object)reader.GetInt32(0);
            
            // Column 1: PRID
            var prId = reader.IsDBNull(1) ? DBNull.Value : (object)reader.GetInt32(1);
            
            // Column 2: UPLOADPATH
            var uploadPath = reader.IsDBNull(2) ? DBNull.Value : (object)reader.GetString(2);
            
            // Column 3: FILENAME
            var fileName = reader.IsDBNull(3) ? DBNull.Value : (object)reader.GetString(3);
            
            // Column 4: UPLOADEDBYID
            var uploadedById = reader.IsDBNull(4) ? DBNull.Value : (object)reader.GetInt32(4);
            
            // Column 5: PRType
            var prType = reader.IsDBNull(5) ? DBNull.Value : (object)reader.GetString(5);
            
            // Column 6: PRNo
            var prNo = reader.IsDBNull(6) ? DBNull.Value : (object)reader.GetString(6);
            
            // Column 7: ItemCode
            var itemCode = reader.IsDBNull(7) ? DBNull.Value : (object)reader.GetString(7);
            
            // Column 8: PRTRANSID (maps to erp_pr_lines_id)
            var prTransId = reader.IsDBNull(8) ? DBNull.Value : (object)reader.GetInt32(8);
            
            // Column 9: Remarks
            var remarks = reader.IsDBNull(9) ? DBNull.Value : (object)reader.GetString(9);
            
            // Validate ERP PR Lines ID (using PRTRANSID which maps to erp_pr_lines_id)
            if (prTransId != DBNull.Value)
            {
                int erpPrLinesId = Convert.ToInt32(prTransId);
                
                // Skip if ERP PR Lines ID not present in erp_pr_lines
                if (!validErpPrLinesIds.Contains(erpPrLinesId))
                {
                    _logger.LogWarning($"Skipping PRATTACHMENTID {prAttachmentId}: ERP PR Lines ID {erpPrLinesId} not found in erp_pr_lines.");
                    skippedCount++;
                    
                    // Must continue reading remaining columns to avoid breaking SequentialAccess
                    // Column 10: PRATTACHMENTDATA (binary - skip by checking IsDBNull)
                    if (!reader.IsDBNull(10))
                    {
                        // Read and discard to advance the reader
                        reader.GetBytes(10, 0, null, 0, 0);
                    }
                    // Skip remaining columns 11-18
                    continue;
                }
            }
            else
            {
                _logger.LogWarning($"Skipping PRATTACHMENTID {prAttachmentId}: ERP PR Lines ID (PRTRANSID) is NULL.");
                skippedCount++;
                
                // Must continue reading remaining columns to avoid breaking SequentialAccess
                // Column 10: PRATTACHMENTDATA (binary - skip by checking IsDBNull)
                if (!reader.IsDBNull(10))
                {
                    // Read and discard to advance the reader
                    reader.GetBytes(10, 0, null, 0, 0);
                }
                // Skip remaining columns 11-18
                continue;
            }
            
            // Column 10: PRATTACHMENTDATA (binary data)
            byte[]? binaryData = null;
            if (!reader.IsDBNull(10))
            {
                long dataLength = reader.GetBytes(10, 0, null, 0, 0);
                binaryData = new byte[dataLength];
                reader.GetBytes(10, 0, binaryData, 0, (int)dataLength);
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
            var isDeleted = reader.GetInt32(16) == 1;
            
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
                ["deleted_date"] = deletedDate
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
        }

        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}");
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        var columns = new List<string> {
            "pr_attachment_id", "erp_pr_lines_id", "upload_path", "file_name", "remarks", "is_header_doc", "pr_attachment_data", "pr_attachment_extensions", "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in batch)
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

        var sql = $"INSERT INTO pr_attachments ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300; // Increase timeout for binary inserts
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {result} records into pr_attachments.");
        return result;
    }
}
