using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;

public class UserCompanyMasterMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<UserCompanyMasterMigration> _logger;

    protected override string SelectQuery => @"
        SELECT
            UC_id,
            UserId,
            ClientId
        FROM TBL_UserClientMaster
        ORDER BY UC_id";

    protected override string InsertQuery => @"
        INSERT INTO user_company_master (
            user_company_id,
            company_id,
            user_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @user_company_id,
            @company_id,
            @user_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (user_company_id) DO UPDATE SET
            company_id = EXCLUDED.company_id,
            user_id = EXCLUDED.user_id,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    public UserCompanyMasterMigration(IConfiguration configuration, ILogger<UserCompanyMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // user_company_id
            "Direct",  // company_id
            "Direct",  // user_id
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
            new { source = "UC_id", logic = "UC_id -> user_company_id (Primary key, autoincrement)", target = "user_company_id" },
            new { source = "ClientId", logic = "ClientId -> company_id (Foreign key to company_master)", target = "company_id" },
            new { source = "UserId", logic = "UserId -> user_id (Foreign key to users)", target = "user_id" },
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
        _logger.LogInformation("Starting User Company Master migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load valid company IDs
            var validCompanyIds = await LoadValidCompanyIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validCompanyIds.Count} valid company IDs");

            // Load valid user IDs
            var validUserIds = await LoadValidUserIdsAsync(pgConn);
            _logger.LogInformation($"Loaded {validUserIds.Count} valid user IDs");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var ucId = reader["UC_id"];
                var userId = reader["UserId"];
                var clientId = reader["ClientId"];

                // Skip if UC_id is NULL
                if (ucId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - UC_id is NULL");
                    continue;
                }

                int ucIdValue = Convert.ToInt32(ucId);

                // Skip duplicates
                if (processedIds.Contains(ucIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Validate company_id
                if (clientId != DBNull.Value)
                {
                    int clientIdValue = Convert.ToInt32(clientId);
                    if (!validCompanyIds.Contains(clientIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping UC_id {ucIdValue} - Invalid company_id: {clientIdValue}");
                        continue;
                    }
                }

                // Validate user_id
                if (userId != DBNull.Value)
                {
                    int userIdValue = Convert.ToInt32(userId);
                    if (!validUserIds.Contains(userIdValue))
                    {
                        skippedRecords++;
                        _logger.LogWarning($"Skipping UC_id {ucIdValue} - Invalid user_id: {userIdValue}");
                        continue;
                    }
                }

                var record = new Dictionary<string, object>
                {
                    ["user_company_id"] = ucIdValue,
                    ["company_id"] = clientId ?? DBNull.Value,
                    ["user_id"] = userId ?? DBNull.Value,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(ucIdValue);

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

            _logger.LogInformation($"User Company Master migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during User Company Master migration");
            throw;
        }
    }

    private async Task<HashSet<int>> LoadValidCompanyIdsAsync(NpgsqlConnection pgConn)
    {
        var validIds = new HashSet<int>();

        try
        {
            var query = "SELECT company_id FROM company_master WHERE company_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                validIds.Add(reader.GetInt32(0));
            }

            _logger.LogInformation($"Loaded {validIds.Count} valid company IDs from company_master");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading valid company IDs");
        }

        return validIds;
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
