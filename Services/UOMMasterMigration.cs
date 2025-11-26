using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class UOMMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT UOM_MAST_ID, ClientSAPId, UOMCODE, UOMNAME FROM TBL_UOM_MASTER";
    protected override string InsertQuery => @"INSERT INTO uom_master (uom_id, company_id, uom_code, uom_name, created_by, created_date) 
                                             VALUES (@uom_id, CASE WHEN @company_id IS NULL THEN NULL ELSE @company_id END, @uom_code, @uom_name, @created_by, @created_date)";

    public UOMMasterMigration(IConfiguration configuration) : base(configuration) { }

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

        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@uom_id", reader["UOM_MAST_ID"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@uom_code", reader["UOMCODE"]);
            pgCmd.Parameters.AddWithValue("@uom_name", reader["UOMNAME"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0);
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}