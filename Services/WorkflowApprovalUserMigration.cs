using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class WorkflowApprovalUserMigration : MigrationService
{
    private readonly ILogger<WorkflowApprovalUserMigration> _logger;
    private MigrationLogger? _migrationLogger;
    protected override string SelectQuery => "SELECT AutoID, WorkFlowMainId, WorkFlowSubId, ApprovedBy, AlternateApprovedBy, CreateDate, Level, CreatedBy FROM TBL_WorkFlowSubSub";
    
    protected override string InsertQuery => @"INSERT INTO workflow_approval_user (workflow_approval_user_id, workflow_master_id, workflow_amount_id, approved_by, level, created_by, created_date, deleted_by, deleted_date, is_deleted, modified_by, modified_date) 
                                             VALUES (@workflow_approval_user_id, @workflow_master_id, @workflow_amount_id, @approved_by, @level, @created_by, @created_date, @deleted_by, @deleted_date, @is_deleted, @modified_by, @modified_date)";

    public WorkflowApprovalUserMigration(IConfiguration configuration, ILogger<WorkflowApprovalUserMigration> logger) : base(configuration)
    {
        _logger = logger; }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "AutoID -> workflow_approval_user_id (Direct)",
            "WorkFlowMainId -> workflow_master_id (Direct)",
            "WorkFlowSubId -> workflow_amount_id (Direct)",
            "ApprovedBy -> approved_by (Convert to ARRAY of integers)",
            "Level -> level (Direct)",
            "CreatedBy -> created_by (Direct)",
            "CreateDate -> created_date (Direct)",
            "deleted_by -> NULL (Fixed Default)",
            "deleted_date -> NULL (Fixed Default)",
            "is_deleted -> false (Fixed Default)",
            "modified_by -> NULL (Fixed Default)",
            "modified_date -> NULL (Fixed Default)",
            "Note: AlternateApprovedBy field from source is not mapped to target table"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "AutoID", logic = "AutoID -> workflow_approval_user_id (Direct)", target = "workflow_approval_user_id" },
            new { source = "WorkFlowMainId", logic = "WorkFlowMainId -> workflow_master_id (Direct)", target = "workflow_master_id" },
            new { source = "WorkFlowSubId", logic = "WorkFlowSubId -> workflow_amount_id (Direct)", target = "workflow_amount_id" },
            new { source = "ApprovedBy", logic = "ApprovedBy -> approved_by (Convert to ARRAY)", target = "approved_by" },
            new { source = "Level", logic = "Level -> level (Direct)", target = "level" },
            new { source = "CreatedBy", logic = "CreatedBy -> created_by (Direct)", target = "created_by" },
            new { source = "CreateDate", logic = "CreateDate -> created_date (Direct)", target = "created_date" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" }
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "workflow_approval_user");
        _migrationLogger.LogInfo("Starting migration");

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        int insertedCount = 0;
        int processedCount = 0;
        int skippedCount = 0;
        var skippedRecords = new List<(int RecordNo, string Reason, Dictionary<string, object> Details)>();
        try
        {
            while (await reader.ReadAsync())
            {
                processedCount++;
                try
                {
                    // Map fields with validation
                    var autoID = reader.IsDBNull(reader.GetOrdinal("AutoID")) ? 0 : Convert.ToInt32(reader["AutoID"]);
                    var workFlowMainId = reader.IsDBNull(reader.GetOrdinal("WorkFlowMainId")) ? 0 : Convert.ToInt32(reader["WorkFlowMainId"]);
                    var workFlowSubId = reader.IsDBNull(reader.GetOrdinal("WorkFlowSubId")) ? 0 : Convert.ToInt32(reader["WorkFlowSubId"]);
                    var approvedBy = reader.IsDBNull(reader.GetOrdinal("ApprovedBy")) ? 0 : Convert.ToInt32(reader["ApprovedBy"]);
                    var level = reader.IsDBNull(reader.GetOrdinal("Level")) ? 0 : Convert.ToInt32(reader["Level"]);
                    var createdBy = reader.IsDBNull(reader.GetOrdinal("CreatedBy")) ? 0 : Convert.ToInt32(reader["CreatedBy"]);
                    var createDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? DateTime.UtcNow : Convert.ToDateTime(reader["CreateDate"]);

                    var details = new Dictionary<string, object>
                    {
                        {"AutoID", autoID},
                        {"WorkFlowMainId", workFlowMainId},
                        {"WorkFlowSubId", workFlowSubId},
                        {"ApprovedBy", approvedBy},
                        {"Level", level},
                        {"CreatedBy", createdBy},
                        {"CreateDate", createDate}
                    };

                    // Validate required fields
                    if (autoID <= 0)
                    {
                        skippedCount++;
                        skippedRecords.Add((processedCount, $"Invalid AutoID ({autoID})", details));
                        continue;
                    }
                    if (workFlowMainId <= 0)
                    {
                        skippedCount++;
                        skippedRecords.Add((processedCount, $"Invalid WorkFlowMainId ({workFlowMainId})", details));
                        continue;
                    }
                    if (workFlowSubId <= 0)
                    {
                        skippedCount++;
                        skippedRecords.Add((processedCount, $"Invalid WorkFlowSubId ({workFlowSubId})", details));
                        continue;
                    }

                    // Check if workflow_master_id exists in workflow_master table
                    using var checkMasterCmd = new NpgsqlCommand("SELECT 1 FROM workflow_master WHERE workflow_id = @workflow_id LIMIT 1", pgConn);
                    if (transaction != null) checkMasterCmd.Transaction = transaction;
                    checkMasterCmd.Parameters.AddWithValue("@workflow_id", workFlowMainId);
                    var masterExists = await checkMasterCmd.ExecuteScalarAsync();
                    if (masterExists == null)
                    {
                        skippedCount++;
                        skippedRecords.Add((processedCount, $"WorkFlowMainId ({workFlowMainId}) not found in workflow_master table", details));
                        continue;
                    }

                    // Check if workflow_amount_id exists in workflow_amount table
                    using var checkAmountCmd = new NpgsqlCommand("SELECT 1 FROM workflow_amount WHERE workflow_amount_id = @workflow_amount_id LIMIT 1", pgConn);
                    if (transaction != null) checkAmountCmd.Transaction = transaction;
                    checkAmountCmd.Parameters.AddWithValue("@workflow_amount_id", workFlowSubId);
                    var amountExists = await checkAmountCmd.ExecuteScalarAsync();
                    if (amountExists == null)
                    {
                        skippedCount++;
                        skippedRecords.Add((processedCount, $"WorkFlowSubId ({workFlowSubId}) not found in workflow_amount table", details));
                        continue;
                    }

                    // Convert ApprovedBy to array format
                    int[] approvedByArray = { approvedBy };

                    // Insert record
                    using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);
                    pgCmd.Parameters.AddWithValue("@workflow_approval_user_id", autoID);
                    pgCmd.Parameters.AddWithValue("@workflow_master_id", workFlowMainId);
                    pgCmd.Parameters.AddWithValue("@workflow_amount_id", workFlowSubId);
                    pgCmd.Parameters.AddWithValue("@approved_by", NpgsqlDbType.Array | NpgsqlDbType.Integer, approvedByArray);
                    pgCmd.Parameters.AddWithValue("@level", level);
                    pgCmd.Parameters.AddWithValue("@created_by", createdBy);
                    pgCmd.Parameters.AddWithValue("@created_date", createDate);
                    pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@is_deleted", false);
                    pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                    pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);

                    await pgCmd.ExecuteNonQueryAsync();
                    insertedCount++;
                }
                catch (Exception ex)
                {
                    skippedCount++;
                    var errorDetails = new Dictionary<string, object>
                    {
                        {"Exception", ex.Message},
                        {"AutoID", reader.IsDBNull(reader.GetOrdinal("AutoID")) ? "NULL" : reader["AutoID"]?.ToString() ?? "NULL" }
                    };
                    skippedRecords.Add((processedCount, $"Error: {ex.Message}", errorDetails));
                    if (transaction != null)
                    {
                        Console.WriteLine("Error occurred in transaction context. Rolling back and stopping migration.");
                        throw;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during migration: {ex.Message}");
            throw;
        }

        // Export migration statistics to Excel
        var outputPath = "workflow_approval_user_migration_stats.xlsx";
        MigrationStatsExporter.ExportToExcel(
            outputPath,
            processedCount,
            insertedCount,
            skippedCount,
            _logger,
            skippedRecords.Select(r => ((string)r.RecordNo.ToString(), r.Reason)).ToList()
        );

        Console.WriteLine($"WorkflowApprovalUser Migration completed. Processed: {processedCount}, Inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }
}
