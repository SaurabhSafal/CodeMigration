using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using DataMigration.Services;

public class PODocTypeMasterMigration : MigrationService
{
    private readonly ILogger<PODocTypeMasterMigration> _logger;
    private readonly MigrationLogger migrationLogger;

    protected override string SelectQuery => "SELECT PODocTypeId, PODocTypeCode, PODocTypeDesc, ClientSAPId FROM TBL_PO_DOC_TYPE";
    protected override string InsertQuery => @"INSERT INTO po_doc_type_master (po_doc_type_id, po_doc_type_code, po_doc_type_name, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@po_doc_type_id, @po_doc_type_code, @po_doc_type_name, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PODocTypeMasterMigration(IConfiguration configuration, ILogger<PODocTypeMasterMigration> logger) : base(configuration) 
    { 
        _logger = logger;
        migrationLogger = new MigrationLogger(_logger, "po_doc_type_master");
    }

    public MigrationLogger GetLogger() => migrationLogger;

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "PODocTypeId -> po_doc_type_id (Direct)",
            "PODocTypeCode -> po_doc_type_code (Direct)",
            "PODocTypeDesc -> po_doc_type_name (Direct)",
            "ClientSAPId -> company_id (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "modified_by -> NULL (Fixed)",
            "modified_date -> NULL (Fixed)",
            "is_deleted -> false (Fixed)",
            "deleted_by -> NULL (Fixed)",
            "deleted_date -> NULL (Fixed)"
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "PODocTypeId", logic = "PODocTypeId -> po_doc_type_id (Direct)", target = "po_doc_type_id" },
            new { source = "PODocTypeCode", logic = "PODocTypeCode -> po_doc_type_code (Direct)", target = "po_doc_type_code" },
            new { source = "PODocTypeDesc", logic = "PODocTypeDesc -> po_doc_type_name (Direct)", target = "po_doc_type_name" },
            new { source = "ClientSAPId", logic = "ClientSAPId -> company_id (Direct)", target = "company_id" },
            new { source = "-", logic = "created_by -> 0 (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
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
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }
        
        try
        {
            while (await reader.ReadAsync())
            {
                try
                {
                    // Validate field values before processing
                    var poDocTypeId = reader.IsDBNull(reader.GetOrdinal("PODocTypeId")) ? 0 : Convert.ToInt32(reader["PODocTypeId"]);
                    var poDocTypeCode = reader.IsDBNull(reader.GetOrdinal("PODocTypeCode")) ? "" : reader["PODocTypeCode"].ToString();
                    var poDocTypeDesc = reader.IsDBNull(reader.GetOrdinal("PODocTypeDesc")) ? "" : reader["PODocTypeDesc"].ToString();
                    var clientSAPId = reader.IsDBNull(reader.GetOrdinal("ClientSAPId")) ? 0 : Convert.ToInt32(reader["ClientSAPId"]);

                    var recordId = $"ID={poDocTypeId}";

                    // Validate required fields
                    if (string.IsNullOrWhiteSpace(poDocTypeCode))
                    {
                        migrationLogger.LogSkipped("PODocTypeCode is null or empty", recordId);
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(poDocTypeDesc))
                    {
                        migrationLogger.LogSkipped("PODocTypeDesc is null or empty", recordId);
                        continue;
                    }

                    pgCmd.Parameters.Clear();
                    pgCmd.Parameters.AddWithValue("@po_doc_type_id", poDocTypeId);
                    pgCmd.Parameters.AddWithValue("@po_doc_type_code", poDocTypeCode ?? "");
                    pgCmd.Parameters.AddWithValue("@po_doc_type_name", poDocTypeDesc ?? "");
                    pgCmd.Parameters.AddWithValue("@company_id", clientSAPId);
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
                catch (Exception recordEx)
                {
                    var poDocTypeId = reader.IsDBNull(reader.GetOrdinal("PODocTypeId")) ? 0 : Convert.ToInt32(reader["PODocTypeId"]);
                    migrationLogger.LogError($"Error processing record: {recordEx.Message}", $"ID={poDocTypeId}", recordEx);
                    throw new Exception($"Error processing record ID={poDocTypeId} in PO Document Type Master Migration: {recordEx.Message}", recordEx);
                }
            }
        }
        catch (Exception readerEx)
        {
            // Check if it's a connection or stream issue
            if (readerEx.Message.Contains("reading from stream") || readerEx.Message.Contains("connection") || readerEx.Message.Contains("timeout"))
            {
                throw new Exception($"SQL Server connection issue during PO Document Type Master Migration after processing {migrationLogger.ProcessedCount} records. " +
                                  $"This could be due to: 1) Network connectivity issues, 2) SQL Server timeout, 3) Large dataset causing memory issues, " +
                                  $"4) Connection string issues. Original error: {readerEx.Message}", readerEx);
            }
            else if (readerEx.Message.Contains("constraint") || readerEx.Message.Contains("foreign key") || readerEx.Message.Contains("violates"))
            {
                throw new Exception($"Database constraint violation during PO Document Type Master Migration at record {migrationLogger.ProcessedCount}. " +
                                  $"This could be due to: 1) Duplicate primary keys, " +
                                  $"2) Invalid data values. Original error: {readerEx.Message}", readerEx);
            }
            else
            {
                throw new Exception($"Unexpected error during PO Document Type Master Migration at record {migrationLogger.ProcessedCount}: {readerEx.Message}", readerEx);
            }
        }
        
        var summary = migrationLogger.GetSummary();
        _logger.LogInformation($"PO Document Type Master Migration completed. Inserted: {summary.TotalInserted}, Skipped: {summary.TotalSkipped}, Errors: {summary.TotalErrors}");
        
        return summary.TotalInserted;
    }
}
