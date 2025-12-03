using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class TechnicalDocumentsMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<TechnicalDocumentsMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    TECHNICALATTACHMENTID,
    EVENTID,
    UPLOADPATH,
    FILENAME,
    UPLOADEDBYID,
    UPLOADEDBYNAME,
    PARENTID,
    ENTDATE,
    USERTYPE,
    SUBMITSTATUS,
    PUBLISHSTATUS,
    SENT,
    DOCNAME,
    REMARK,
    SENTDATE,
    SELECTEDVENDOR,
    SELECTEDVENDORNAME,
    PRATTACHMENTID,
    IsViewVendor
FROM TBL_TECHNICALATTACHMENT
";

    protected override string InsertQuery => @"
INSERT INTO technical_documents (
    technical_document_id, event_id, file_path, file_name, uploaded_by, full_name, 
    user_type, document_name, remarks, supplier_specific_document_id, 
    supplier_specific_document_name, pr_attachment_id, is_visible_to_vendor, 
    created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, 
    deleted_date
) VALUES (
    @technical_document_id, @event_id, @file_path, @file_name, @uploaded_by, @full_name, 
    @user_type, @document_name, @remarks, @supplier_specific_document_id, 
    @supplier_specific_document_name, @pr_attachment_id, @is_visible_to_vendor, 
    @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, 
    @deleted_date
)
ON CONFLICT (technical_document_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    file_path = EXCLUDED.file_path,
    file_name = EXCLUDED.file_name,
    uploaded_by = EXCLUDED.uploaded_by,
    full_name = EXCLUDED.full_name,
    user_type = EXCLUDED.user_type,
    document_name = EXCLUDED.document_name,
    remarks = EXCLUDED.remarks,
    supplier_specific_document_id = EXCLUDED.supplier_specific_document_id,
    supplier_specific_document_name = EXCLUDED.supplier_specific_document_name,
    pr_attachment_id = EXCLUDED.pr_attachment_id,
    is_visible_to_vendor = EXCLUDED.is_visible_to_vendor,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    public TechnicalDocumentsMigration(IConfiguration configuration, ILogger<TechnicalDocumentsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // technical_document_id
        "Direct", // event_id
        "Direct", // file_path
        "Direct", // file_name
        "Direct", // uploaded_by
        "Direct", // full_name
        "Direct", // user_type
        "Direct", // document_name
        "Direct", // remarks
        "Direct", // supplier_specific_document_id
        "Direct", // supplier_specific_document_name
        "Direct", // pr_attachment_id
        "Direct", // is_visible_to_vendor
        "Fixed",  // created_by
        "Fixed",  // created_date
        "Fixed",  // modified_by
        "Fixed",  // modified_date
        "Fixed",  // is_deleted
        "Fixed",  // deleted_by
        "Fixed"   // deleted_date
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "TECHNICALATTACHMENTID", logic = "TECHNICALATTACHMENTID -> technical_document_id (Primary key, autoincrement)", target = "technical_document_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Ref from EventMaster)", target = "event_id" },
            new { source = "UPLOADPATH", logic = "UPLOADPATH -> file_path (Direct)", target = "file_path" },
            new { source = "FILENAME", logic = "FILENAME -> file_name (Direct)", target = "file_name" },
            new { source = "UPLOADEDBYID", logic = "UPLOADEDBYID -> uploaded_by (Ref from User Master)", target = "uploaded_by" },
            new { source = "UPLOADEDBYNAME", logic = "UPLOADEDBYNAME -> full_name (Ref from User Master)", target = "full_name" },
            new { source = "USERTYPE", logic = "USERTYPE -> user_type (Direct)", target = "user_type" },
            new { source = "DOCNAME", logic = "DOCNAME -> document_name (Direct)", target = "document_name" },
            new { source = "REMARK", logic = "REMARK -> remarks (Direct)", target = "remarks" },
            new { source = "SELECTEDVENDOR", logic = "SELECTEDVENDOR -> supplier_specific_document_id (Direct)", target = "supplier_specific_document_id" },
            new { source = "SELECTEDVENDORNAME", logic = "SELECTEDVENDORNAME -> supplier_specific_document_name (Direct)", target = "supplier_specific_document_name" },
            new { source = "PRATTACHMENTID", logic = "PRATTACHMENTID -> pr_attachment_id (Ref from PR Attachments table)", target = "pr_attachment_id" },
            new { source = "IsViewVendor", logic = "IsViewVendor -> is_visible_to_vendor (Direct)", target = "is_visible_to_vendor" },
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
        _logger.LogInformation("Starting TechnicalDocuments migration...");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        // Load valid event IDs
        var validEventIds = await LoadValidEventIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validEventIds.Count} valid event IDs.");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var technicalAttachmentId = reader["TECHNICALATTACHMENTID"] ?? DBNull.Value;
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var uploadPath = reader["UPLOADPATH"] ?? DBNull.Value;
            var fileName = reader["FILENAME"] ?? DBNull.Value;
            var uploadedById = reader["UPLOADEDBYID"] ?? DBNull.Value;
            var uploadedByName = reader["UPLOADEDBYNAME"] ?? DBNull.Value;
            var parentId = reader["PARENTID"] ?? DBNull.Value;
            var entDate = reader["ENTDATE"] ?? DBNull.Value;
            var userType = reader["USERTYPE"] ?? DBNull.Value;
            var submitStatus = reader["SUBMITSTATUS"] ?? DBNull.Value;
            var publishStatus = reader["PUBLISHSTATUS"] ?? DBNull.Value;
            var sent = reader["SENT"] ?? DBNull.Value;
            var docName = reader["DOCNAME"] ?? DBNull.Value;
            var remark = reader["REMARK"] ?? DBNull.Value;
            var sentDate = reader["SENTDATE"] ?? DBNull.Value;
            var selectedVendor = reader["SELECTEDVENDOR"] ?? DBNull.Value;
            var selectedVendorName = reader["SELECTEDVENDORNAME"] ?? DBNull.Value;
            var prAttachmentId = reader["PRATTACHMENTID"] ?? DBNull.Value;
            var isViewVendor = reader["IsViewVendor"] ?? DBNull.Value;

            // Validate required keys
            if (technicalAttachmentId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping row: TECHNICALATTACHMENTID is NULL.");
                skippedCount++;
                continue;
            }

            // Validate event_id exists in event_master
            if (eventId != DBNull.Value)
            {
                int eventIdValue = Convert.ToInt32(eventId);
                if (!validEventIds.Contains(eventIdValue))
                {
                    _logger.LogWarning($"Skipping TECHNICALATTACHMENTID {technicalAttachmentId}: event_id {eventIdValue} not found in event_master.");
                    skippedCount++;
                    continue;
                }
            }

            // Convert IsViewVendor integer to boolean if needed
            object isVisibleToVendor = DBNull.Value;
            if (isViewVendor != DBNull.Value)
            {
                int isViewVendorValue = Convert.ToInt32(isViewVendor);
                isVisibleToVendor = isViewVendorValue == 1;
            }

            // Convert SELECTEDVENDOR text to integer for supplier_specific_document_id
            object supplierSpecificDocumentId = DBNull.Value;
            if (selectedVendor != DBNull.Value && !string.IsNullOrWhiteSpace(selectedVendor.ToString()))
            {
                if (int.TryParse(selectedVendor.ToString(), out int vendorId))
                {
                    supplierSpecificDocumentId = vendorId;
                }
            }

            var record = new Dictionary<string, object>
            {
                ["technical_document_id"] = technicalAttachmentId,
                ["event_id"] = eventId,
                ["file_path"] = uploadPath,
                ["file_name"] = fileName,
                ["uploaded_by"] = uploadedById,
                ["full_name"] = uploadedByName,
                ["user_type"] = userType,
                ["document_name"] = docName,
                ["remarks"] = remark,
                ["supplier_specific_document_id"] = supplierSpecificDocumentId,
                ["supplier_specific_document_name"] = selectedVendorName,
                ["pr_attachment_id"] = prAttachmentId,
                ["is_visible_to_vendor"] = isVisibleToVendor,
                ["created_by"] = DBNull.Value,
                ["created_date"] = DBNull.Value,
                ["modified_by"] = DBNull.Value,
                ["modified_date"] = DBNull.Value,
                ["is_deleted"] = false,
                ["deleted_by"] = DBNull.Value,
                ["deleted_date"] = DBNull.Value
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Inserting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Inserting final batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
        }

        _logger.LogInformation($"TechnicalDocuments migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<HashSet<int>> LoadValidEventIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT event_id FROM event_master";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        
        return validIds;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        // Deduplicate by technical_document_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["technical_document_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate technical_document_id records.");
        }

        var columns = new List<string> {
            "technical_document_id", "event_id", "file_path", "file_name", "uploaded_by", "full_name", 
            "user_type", "document_name", "remarks", "supplier_specific_document_id", 
            "supplier_specific_document_name", "pr_attachment_id", "is_visible_to_vendor", 
            "created_by", "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", 
            "deleted_date"
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
                parameters.Add(new NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }

        var updateColumns = columns.Where(c => c != "technical_document_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO technical_documents ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (technical_document_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
