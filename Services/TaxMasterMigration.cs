using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class TaxMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT TaxId, TaxName, TaxPer FROM TBL_TaxMaster";
    protected override string InsertQuery => @"
        INSERT INTO tax_master 
            (tax_master_id, tax_name, tax_percentage, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
        VALUES 
            (@tax_master_id, @tax_name, @tax_percentage, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public TaxMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",           // tax_master_id
            "Direct",           // tax_name
            "Direct",           // tax_percentage
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

        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@tax_master_id", reader["TaxId"]);
            pgCmd.Parameters.AddWithValue("@tax_name", reader["TaxName"]);
            pgCmd.Parameters.AddWithValue("@tax_percentage", reader["TaxPer"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0);
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_deleted", false);
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}