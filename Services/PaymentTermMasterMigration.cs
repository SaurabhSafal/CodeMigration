using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class PaymentTermMasterMigration : MigrationService
{
    private readonly ILogger<PaymentTermMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;
    private readonly List<(string RecordId, string Reason)> _skippedRecords = new();

    protected override string SelectQuery => "SELECT PTID, PTCode, PTDescription, ClientSAPId FROM TBL_PAYMENTTERMMASTER";
    protected override string InsertQuery => @"INSERT INTO payment_term_master (payment_term_id, payment_term_code, payment_term_name, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@payment_term_id, @payment_term_code, @payment_term_name, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PaymentTermMasterMigration(IConfiguration configuration, ILogger<PaymentTermMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "payment_term_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // payment_term_id
            "Direct",           // payment_term_code
            "Direct",           // payment_term_name
            "FK",               // company_id
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
        var insertedRecordIds = new List<string>();

        while (await reader.ReadAsync())
        {
            totalRecords++;
            var ptid = reader["PTID"];
            var recordId = $"ID={ptid}";
            try
            {
                pgCmd.Parameters.Clear();
                pgCmd.Parameters.AddWithValue("@payment_term_id", ptid);
                pgCmd.Parameters.AddWithValue("@payment_term_code", reader["PTCode"]);
                pgCmd.Parameters.AddWithValue("@payment_term_name", reader["PTDescription"]);
                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
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
                    insertedRecordIds.Add(recordId);
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
        _logger.LogInformation($"Payment Term Master Migration completed. Total: {totalRecords}, Inserted: {insertedRecords}, Skipped: {skippedRecords.Count}");

        // Export migration stats to Excel
        string outputPath = "payment_term_master_migration_stats.xlsx";
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