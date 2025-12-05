using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class UserTermMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<UserTermMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    CLAUSEEVENTWISEID,
    EVENTID,
    CLAUSENAME,
    ACTIONBY,
    ACTIONDATE
FROM TBL_CLAUSEEVENTWISE
";

    protected override string InsertQuery => @"
INSERT INTO user_term (
    user_term_id, event_id, user_term_name, terms_mandatory, created_by, 
    created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @user_term_id, @event_id, @user_term_name, @terms_mandatory, @created_by, 
    @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)
ON CONFLICT (user_term_id) DO UPDATE SET
    event_id = EXCLUDED.event_id,
    user_term_name = EXCLUDED.user_term_name,
    terms_mandatory = EXCLUDED.terms_mandatory,
    modified_by = EXCLUDED.modified_by,
    modified_date = EXCLUDED.modified_date,
    is_deleted = EXCLUDED.is_deleted,
    deleted_by = EXCLUDED.deleted_by,
    deleted_date = EXCLUDED.deleted_date";

    public UserTermMigration(IConfiguration configuration, ILogger<UserTermMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // user_term_id
        "Direct", // event_id
        "Direct", // user_term_name
        "Fixed",  // terms_mandatory
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
            new { source = "CLAUSEEVENTWISEID", logic = "CLAUSEEVENTWISEID -> user_term_id (Primary key, autoincrement)", target = "user_term_id" },
            new { source = "EVENTID", logic = "EVENTID -> event_id (Ref from Event Master)", target = "event_id" },
            new { source = "CLAUSENAME", logic = "CLAUSENAME -> user_term_name (Direct)", target = "user_term_name" },
            new { source = "-", logic = "terms_mandatory -> NULL (New column, Fixed Default)", target = "terms_mandatory" },
            new { source = "ACTIONBY", logic = "ACTIONBY -> created_by (Direct)", target = "created_by" },
            new { source = "ACTIONDATE", logic = "ACTIONDATE -> created_date (Direct)", target = "created_date" },
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
        _logger.LogInformation("Starting UserTerm migration...");
        
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
            var clauseEventWiseId = reader["CLAUSEEVENTWISEID"] ?? DBNull.Value;
            var eventId = reader["EVENTID"] ?? DBNull.Value;
            var clauseName = reader["CLAUSENAME"] ?? DBNull.Value;
            var actionBy = reader["ACTIONBY"] ?? DBNull.Value;
            var actionDate = reader["ACTIONDATE"] ?? DBNull.Value;

            // Validate required keys
            if (clauseEventWiseId == DBNull.Value)
            {
                _logger.LogWarning($"Skipping row: CLAUSEEVENTWISEID is NULL.");
                skippedCount++;
                continue;
            }

            // Validate event_id exists in event_master
            if (eventId != DBNull.Value)
            {
                int eventIdValue = Convert.ToInt32(eventId);
                if (!validEventIds.Contains(eventIdValue))
                {
                    _logger.LogWarning($"Skipping CLAUSEEVENTWISEID {clauseEventWiseId}: event_id {eventIdValue} not found in event_master.");
                    skippedCount++;
                    continue;
                }
            }

            var record = new Dictionary<string, object>
            {
                ["user_term_id"] = clauseEventWiseId,
                ["event_id"] = eventId,
                ["user_term_name"] = clauseName,
                ["terms_mandatory"] = DBNull.Value, // New column, no mapping from source
                ["created_by"] = actionBy,
                ["created_date"] = actionDate,
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

        _logger.LogInformation($"UserTerm migration completed. Inserted: {insertedCount}, Skipped: {skippedCount}");
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

        // Deduplicate by user_term_id
        var deduplicatedBatch = batch
            .GroupBy(r => r["user_term_id"])
            .Select(g => g.Last())
            .ToList();

        if (deduplicatedBatch.Count < batch.Count)
        {
            _logger.LogWarning($"Batch {batchNumber}: Removed {batch.Count - deduplicatedBatch.Count} duplicate user_term_id records.");
        }

        var columns = new List<string> {
            "user_term_id", "event_id", "user_term_name", "terms_mandatory", "created_by", 
            "created_date", "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
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

        var updateColumns = columns.Where(c => c != "user_term_id" && c != "created_by" && c != "created_date").ToList();
        var updateSet = string.Join(", ", updateColumns.Select(c => $"{c} = EXCLUDED.{c}"));

        var sql = $@"INSERT INTO user_term ({string.Join(", ", columns)}) 
VALUES {string.Join(", ", valueRows)}
ON CONFLICT (user_term_id) DO UPDATE SET {updateSet}";

        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.CommandTimeout = 300;
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted/Updated {result} records.");
        return result;
    }
}
