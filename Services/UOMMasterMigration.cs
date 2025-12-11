using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class UOMMasterMigration : MigrationService
{
    private readonly ILogger<UOMMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT UOM_MAST_ID, ClientSAPId, UOMCODE, UOMNAME FROM TBL_UOM_MASTER";
    protected override string InsertQuery => @"INSERT INTO uom_master (uom_id, company_id, uom_code, uom_name, created_by, created_date) 
                                             VALUES (@uom_id, CASE WHEN @company_id IS NULL THEN NULL ELSE @company_id END, @uom_code, @uom_name, @created_by, @created_date)";

    public UOMMasterMigration(IConfiguration configuration, ILogger<UOMMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "uom_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",        // uom_id
            "FK",            // company_id
            "Direct",        // uom_code
            "Direct",        // uom_name
            "Default: 0",    // created_by
            "Default: Now"   // created_date
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

        int totalRecords = 0;
        var skippedRecordsList = new List<(string RecordId, string Reason)>();

        while (await reader.ReadAsync())
        {
            totalRecords++;
            var uomId = reader["UOM_MAST_ID"];
            var recordId = $"ID={uomId}";
            try
            {
                pgCmd.Parameters.Clear();
                pgCmd.Parameters.AddWithValue("@uom_id", uomId);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
                pgCmd.Parameters.AddWithValue("@uom_code", reader["UOMCODE"]);
                pgCmd.Parameters.AddWithValue("@uom_name", reader["UOMNAME"]);
                pgCmd.Parameters.AddWithValue("@created_by", 0);
                pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
                int result = await pgCmd.ExecuteNonQueryAsync();
                if (result > 0)
                {
                    migrationLogger.LogInserted(recordId);
                }
                else
                {
                    migrationLogger.LogSkipped(recordId, "Duplicate or conflict");
                    skippedRecordsList.Add((recordId, "Duplicate or conflict"));
                }
            }
            catch (Exception ex)
            {
                migrationLogger.LogSkipped(recordId, ex.Message);
                skippedRecordsList.Add((recordId, ex.Message));
            }
        }
        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"UOM Master Migration completed. Inserted: {summary.TotalInserted}, Skipped: {summary.TotalSkipped}");

        // Export migration statistics to Excel
        string outputPath = System.IO.Path.Combine("migration_outputs", $"UOMMasterMigrationStats_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx");
        MigrationStatsExporter.ExportToExcel(
            outputPath,
            totalRecords,
            summary.TotalInserted,
            summary.TotalSkipped,
            _logger,
            skippedRecordsList
        );
        _logger.LogInformation($"Migration statistics exported to {outputPath}");

        return summary.TotalInserted;
    }
}