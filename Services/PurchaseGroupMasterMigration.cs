using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class PurchaseGroupMasterMigration : MigrationService
{
    private readonly ILogger<PurchaseGroupMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT PurchaseGroupId, ClientSAPId, PurchaseGroupCode, PurchaseGroupName FROM TBL_PurchaseGroupMaster";
    protected override string InsertQuery => @"INSERT INTO purchase_group_master (purchase_group_id, company_id, purchase_group_code, purchase_group_name, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@purchase_group_id, @company_id, @purchase_group_code, @purchase_group_name, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PurchaseGroupMasterMigration(IConfiguration configuration, ILogger<PurchaseGroupMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "purchase_group_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // purchase_group_id
            "FK",               // company_id
            "Direct",           // purchase_group_code
            "Direct",           // purchase_group_name
            "Default: 0",       // created_by
            "Default: Now",     // created_date
            "Default: null",    // modified_by
            "Default: null",    // modified_date
            "Default: false",   // is_deleted
            "Default: null",    // deleted_by
            "Default: null"     // deleted_date
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }

        while (await reader.ReadAsync())
        {
            var purchaseGroupId = reader["PurchaseGroupId"];
            var recordId = $"ID={purchaseGroupId}";
            
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@purchase_group_id", purchaseGroupId);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@purchase_group_code", reader["PurchaseGroupCode"]);
            pgCmd.Parameters.AddWithValue("@purchase_group_name", reader["PurchaseGroupName"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0);
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_deleted", false);
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0)
            {
                migrationLogger.LogInserted(recordId);
            }
        }
        
        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"Purchase Group Master Migration completed. Inserted: {summary.TotalInserted}, Skipped: {summary.TotalSkipped}");

        // Export migration stats to Excel
        try
        {
            var outputPath = $"PurchaseGroupMasterMigrationStats_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
            var skippedRecordsList = migrationLogger.GetSkippedRecords().Select(x => (x.RecordIdentifier, x.Message)).ToList();
            MigrationStatsExporter.ExportToExcel(outputPath, summary.TotalProcessed, summary.TotalInserted, summary.TotalSkipped, _logger, skippedRecordsList);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Failed to export migration stats: {ex.Message}");
        }

        return summary.TotalInserted;
    }
}