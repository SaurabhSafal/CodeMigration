using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class CompanyMasterMigration : MigrationService
{
    // SQL Server: TBL_CLIENTSAPMASTER -> PostgreSQL: company_master
    protected override string SelectQuery => @"
        SELECT 
            ClientSAPId,
            ClientSAPCode,
            ClientSAPName,
            SAP,
            PRAllocationLogic,
            Address,
            UploadDocument,
            DocumentName
        FROM TBL_CLIENTSAPMASTER";

    protected override string InsertQuery => @"
        INSERT INTO company_master (
            company_id,
            company_code,
            company_name,
            sap_version,
            pr_allocation_logic,
            address,
            company_logo_url,
            company_logo_name,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @company_id,
            @company_code,
            @company_name,
            @sap_version,
            @pr_allocation_logic,
            @address,
            @company_logo_url,
            @company_logo_name,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public CompanyMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct", // company_id
            "Direct", // company_code
            "Direct", // company_name
            "Direct", // sap_version
            "Direct", // pr_allocation_logic
            "Direct", // address
            "Direct", // company_logo_url
            "Direct"  // company_logo_name
        };
    }

    public async Task<int> MigrateAsync()
    {
        return await base.MigrateAsync(useTransaction: true);
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        Console.WriteLine("🚀 Starting CompanyMaster migration...");
        Console.WriteLine($"📋 Executing query...");
        
        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        Console.WriteLine($"✓ Query executed. Processing records...");
        
        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        if (transaction != null)
        {
            pgCmd.Transaction = transaction;
        }

        int insertedCount = 0;
        int skippedCount = 0;
        int totalReadCount = 0;

        while (await reader.ReadAsync())
        {
            totalReadCount++;
            if (totalReadCount == 1)
            {
                Console.WriteLine($"✓ Found records! Processing...");
            }
            
            if (totalReadCount % 10 == 0)
            {
                Console.WriteLine($"📊 Processed {totalReadCount} records so far... (Inserted: {insertedCount}, Skipped: {skippedCount})");
            }
            
            try
            {
                pgCmd.Parameters.Clear();

                pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_code", reader["ClientSAPCode"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_name", reader["ClientSAPName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@sap_version", reader["SAP"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@pr_allocation_logic", reader["PRAllocationLogic"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@address", reader["Address"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_logo_url", reader["UploadDocument"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@company_logo_name", reader["DocumentName"] ?? DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@created_date", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@is_deleted", false);
                pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
                pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);

                int result = await pgCmd.ExecuteNonQueryAsync();
                if (result > 0) insertedCount++;
            }
            catch (Exception ex)
            {
                skippedCount++;
                Console.WriteLine($"❌ Error migrating ClientSAPId {reader["ClientSAPId"]}: {ex.Message}");
                Console.WriteLine($"   Stack Trace: {ex.StackTrace}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"   Inner Exception: {ex.InnerException.Message}");
                }
            }
        }

        Console.WriteLine($"\n📊 Migration Summary:");
        Console.WriteLine($"   Total records read: {totalReadCount}");
        Console.WriteLine($"   ✓ Successfully inserted: {insertedCount}");
        Console.WriteLine($"   ❌ Skipped (errors): {skippedCount}");
        
        if (totalReadCount == 0)
        {
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_CLIENTSAPMASTER table!");
        }

        return insertedCount;
    }
}
