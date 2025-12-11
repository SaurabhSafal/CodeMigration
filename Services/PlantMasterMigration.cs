using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class PlantMasterMigration : MigrationService
{
    private readonly ILogger<PlantMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT PlantId, ClientSAPId, PlantCode, PlantName, CompanyCode, Location FROM TBL_PlantMaster";
    protected override string InsertQuery => @"INSERT INTO plant_master (plant_id, company_id, plant_code, plant_name, plant_company_code, plant_location, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@plant_id, @company_id, @plant_code, @plant_name, @plant_company_code, @plant_location, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PlantMasterMigration(IConfiguration configuration, ILogger<PlantMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "plant_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // plant_id
            "FK",               // company_id
            "Direct",           // plant_code
            "Direct",           // plant_name
            "Direct",           // plant_company_code
            "Direct",           // plant_location
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

        int totalRecords = 0;
        int insertedRecords = 0;
        var skippedRecords = new List<(string RecordId, string Reason)>();

        while (await reader.ReadAsync())
        {
            totalRecords++;
            var plantId = reader["PlantId"];
            var recordId = $"ID={plantId}";
            try
            {
                pgCmd.Parameters.Clear();
                pgCmd.Parameters.AddWithValue("@plant_id", plantId);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
                pgCmd.Parameters.AddWithValue("@plant_code", reader["PlantCode"]);
                pgCmd.Parameters.AddWithValue("@plant_name", reader["PlantName"]);
                pgCmd.Parameters.AddWithValue("@plant_company_code", reader["CompanyCode"]);
                pgCmd.Parameters.AddWithValue("@plant_location", reader.IsDBNull(reader.GetOrdinal("Location")) ? (object)DBNull.Value : reader["Location"]);
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
                    insertedRecords++;
                }
                else
                {
                    skippedRecords.Add((recordId, "Insert returned 0 rows"));
                    migrationLogger.LogSkipped(recordId, "Insert returned 0 rows");
                }
            }
            catch (Exception ex)
            {
                skippedRecords.Add((recordId, ex.Message));
                migrationLogger.LogSkipped(recordId, ex.Message);
            }
        }

        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"Plant Master Migration completed. Total: {totalRecords}, Inserted: {insertedRecords}, Skipped: {skippedRecords.Count}");

        // Export migration stats to Excel
        string outputPath = "plant_master_migration_stats.xlsx";
        MigrationStatsExporter.ExportToExcel(
            outputPath,
            totalRecords,
            insertedRecords,
            skippedRecords.Count,
            _logger,
            skippedRecords
        );
        _logger.LogInformation($"Migration stats exported to migration_outputs/{outputPath}");

        return insertedRecords;
    }
}