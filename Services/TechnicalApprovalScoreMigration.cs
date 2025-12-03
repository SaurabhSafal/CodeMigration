using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class TechnicalApprovalScoreMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<TechnicalApprovalScoreMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    TechApprovalId,
    EVENTID,
    PBID,
    VendorId,
    SCORE,
    TechnicalRemarks,
    IsSubmittoHOD,
    HODStatus,
    HODRemarks,
    IsSubmittoPlantHead,
    PlantHeadStatus,
    PlantHeadRemarks,
    IsSubmittoBuyer,
    TechnicalMainRemarks
FROM TBL_TechApproval
";

    protected override string InsertQuery => @"
INSERT INTO technical_approval_score (
    technical_approval_score_id, event_id, event_item_id, supplier_id, score, 
    remarks, created_by, created_date, modified_by, modified_date, is_deleted, 
    deleted_by, deleted_date
) VALUES (
    @technical_approval_score_id, @event_id, @event_item_id, @supplier_id, @score, 
    @remarks, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, 
    @deleted_by, @deleted_date
)
ON CONFLICT (technical_approval_score_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    event_item_id = EXCLUDED.event_item_id,
    supplier_id = EXCLUDED.supplier_id,
    score = EXCLUDED.score,
    remarks = EXCLUDED.remarks,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    public TechnicalApprovalScoreMigration(IConfiguration configuration, ILogger<TechnicalApprovalScoreMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // technical_approval_score_id
        "Direct", // event_id
        "Direct", // event_item_id
        "Direct", // supplier_id
        "Direct", // score
        "Direct", // remarks
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
            new { source = "TechApprovalId", logic = "TechApprovalId -> technical_approval_score_id (Primary key, autoincrement)", target = "technical_approval_score_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Direct)", target = "event_id" },
            new { source = "PBID", logic = "PBID -> event_item_id (Direct)", target = "event_item_id" },
            new { source = "VendorId", logic = "VendorId -> supplier_id (Direct)", target = "supplier_id" },
            new { source = "SCORE", logic = "SCORE -> score (Direct)", target = "score" },
            new { source = "TechnicalRemarks", logic = "TechnicalRemarks -> remarks (Direct)", target = "remarks" },
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
        _logger.LogInformation("Starting TechnicalApprovalScore migration...");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        // Load valid event IDs, event_item IDs, and supplier IDs
        var validEventIds = await LoadValidEventIdsAsync(pgConn, transaction);
        var validEventItemIds = await LoadValidEventItemIdsAsync(pgConn, transaction);
        var validSupplierIds = await LoadValidSupplierIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validEventIds.Count} valid event IDs, {validEventItemIds.Count} valid event_item IDs, and {validSupplierIds.Count} valid supplier IDs.");

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        selectCmd.CommandTimeout = 300;
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var techApprovalId = reader["TechApprovalId"] ?? DBNull.Value;
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var pbId = reader["PBID"] ?? DBNull.Value;
            var vendorId = reader["VendorId"] ?? DBNull.Value;
            var score = reader["SCORE"] ?? DBNull.Value;
            var technicalRemarks = reader["TechnicalRemarks"] ?? DBNull.Value;
            var isSubmittoHOD = reader["IsSubmittoHOD"] ?? DBNull.Value;
            var hodStatus = reader["HODStatus"] ?? DBNull.Value;
            var hodRemarks = reader["HODRemarks"] ?? DBNull.Value;
            var isSubmittoPlantHead = reader["IsSubmittoPlantHead"] ?? DBNull.Value;
            var plantHeadStatus = reader["PlantHeadStatus"] ?? DBNull.Value;
            var plantHeadRemarks = reader["PlantHeadRemarks"] ?? DBNull.Value;
            var isSubmittoBuyer = reader["IsSubmittoBuyer"] ?? DBNull.Value;
            var technicalMainRemarks = reader["TechnicalMainRemarks"] ?? DBNull.Value;

            // Validate required keys
            if (techApprovalId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping row: TechApprovalId is NULL.");
                skippedCount++;
                continue;
            }

            // Validate event_id exists in event_master
            if (eventId != DBNull.Value)
            {
                int eventIdValue = Convert.ToInt32(eventId);
                if (!validEventIds.Contains(eventIdValue))
                {
                    _logger.LogWarning($"Skipping TechApprovalId {techApprovalId}: event_id {eventIdValue} not found in event_master.");
                    skippedCount++;
                    continue;
                }
            }

            // Validate event_item_id exists in event_items
            if (pbId != DBNull.Value)
            {
                int eventItemIdValue = Convert.ToInt32(pbId);
                if (!validEventItemIds.Contains(eventItemIdValue))
                {
                    _logger.LogWarning($"Skipping TechApprovalId {techApprovalId}: event_item_id (PBID) {eventItemIdValue} not found in event_items.");
                    skippedCount++;
                    continue;
                }
            }

            // Validate supplier_id exists in supplier_master
            if (vendorId != DBNull.Value)
            {
                int supplierIdValue = Convert.ToInt32(vendorId);
                if (!validSupplierIds.Contains(supplierIdValue))
                {
                    _logger.LogWarning($"Skipping TechApprovalId {techApprovalId}: supplier_id (VendorId) {supplierIdValue} not found in supplier_master.");
                    skippedCount++;
                    continue;
                }
            }

            var record = new Dictionary<string, object>
            {
                ["technical_approval_score_id"] = techApprovalId,
                ["event_id"] = eventId,
                ["event_item_id"] = pbId,
                ["supplier_id"] = vendorId,
                ["score"] = score,
                ["remarks"] = technicalRemarks,
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

        _logger.LogInformation($"TechnicalApprovalScore migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
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

    private async Task<HashSet<int>> LoadValidEventItemIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT event_item_id FROM event_items";
        
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        
        return validIds;
    }

    private async Task<HashSet<int>> LoadValidSupplierIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT supplier_id FROM supplier_master";
        
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

        // Deduplicate by technical_approval_score_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["technical_approval_score_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate technical_approval_score_id records.");
        }

        var columns = new List<string> {
            "technical_approval_score_id", "event_id", "event_item_id", "supplier_id", "score", 
            "remarks", "created_by", "created_date", "modified_by", "modified_date", "is_deleted", 
            "deleted_by", "deleted_date"
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

        var updateColumns = columns.Where(c => c != "technical_approval_score_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO technical_approval_score ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (technical_approval_score_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
