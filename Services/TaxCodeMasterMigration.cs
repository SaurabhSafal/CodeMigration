using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class TaxCodeMasterMigration : MigrationService
{
    private readonly ILogger<TaxCodeMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    // SQL Server: TBL_TAXCODEMASTER -> PostgreSQL: tax_code_master
    protected override string SelectQuery => @"
        SELECT 
            TaxCode_Master_Id,
            TaxCode,
            TaxCodeDesc,
            ClientSAPId
        FROM TBL_TAXCODEMASTER";

    protected override string InsertQuery => @"
        INSERT INTO tax_code_master (
            tax_code_id,
            tax_code,
            tax_code_name,
            company_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @tax_code_id,
            @tax_code,
            @tax_code_name,
            @company_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public TaxCodeMasterMigration(IConfiguration configuration, ILogger<TaxCodeMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "tax_code_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // tax_code_id
            "Direct", // tax_code
            "Direct", // tax_code_name
            "Direct"  // company_id
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        migrationLogger.LogInfo("Starting TaxCodeMaster migration...");
        
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        migrationLogger.LogInfo("Query executed. Processing records...");
        
        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }

        int totalReadCount = 0;

        while (await reader.ReadAsync())
        {
            totalReadCount++;
            if (totalReadCount == 1)
            {
                migrationLogger.LogInfo("Found records! Processing...");
            }
            
            if (totalReadCount % 100 == 0)
            {
                migrationLogger.LogInfo($"Processed {totalReadCount} records so far... (Inserted: {migrationLogger.InsertedCount}, Errors: {migrationLogger.ErrorCount})");
            }
            
            var taxCodeId = reader["TaxCode_Master_Id"];
            var recordId = $"ID={taxCodeId}";
            
            try
            {
                pgCmd.Parameters.Clear();

                pgCmd.Parameters.AddWithValue("@tax_code_id", taxCodeId ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@tax_code", reader["TaxCode"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@tax_code_name", reader["TaxCodeDesc"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_date", DBNull.Value);
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
            catch (Exception ex)
            {
                migrationLogger.LogError($"Error migrating TaxCode_Master_Id {taxCodeId}: {ex.Message}", recordId, ex);
            }
        }

        if (totalReadCount == 0)
        {
            migrationLogger.LogInfo("WARNING: No records found in TBL_TAXCODEMASTER table!");
        }

        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"Tax Code Master Migration Summary: Total: {totalReadCount}, Inserted: {summary.TotalInserted}, Errors: {summary.TotalErrors}");

        return summary.TotalInserted;
    }
}
