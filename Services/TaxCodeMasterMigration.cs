using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class TaxCodeMasterMigration : MigrationService
{
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

    public TaxCodeMasterMigration(IConfiguration configuration) : base(configuration) { }

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
        Console.WriteLine("🚀 Starting TaxCodeMaster migration...");
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

                pgCmd.Parameters.AddWithValue("@tax_code_id", reader["TaxCode_Master_Id"] ?? DBNull.Value);
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
                if (result > 0) insertedCount++;
            }
            catch (Exception ex)
            {
                skippedCount++;
                Console.WriteLine($"❌ Error migrating TaxCode_Master_Id {reader["TaxCode_Master_Id"]}: {ex.Message}");
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
            Console.WriteLine($"\n⚠️  WARNING: No records found in TBL_TAXCODEMASTER table!");
        }

        return insertedCount;
    }
}
