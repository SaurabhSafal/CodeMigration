using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class PaymentTermMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT PTID, PTCode, PTDescription, ClientSAPId FROM TBL_PAYMENTTERMMASTER";
    protected override string InsertQuery => @"INSERT INTO payment_term_master (payment_term_id, payment_term_code, payment_term_name, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@payment_term_id, @payment_term_code, @payment_term_name, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PaymentTermMasterMigration(IConfiguration configuration) : base(configuration) { }

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

        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@payment_term_id", reader["PTID"]);
            pgCmd.Parameters.AddWithValue("@payment_term_code", reader["PTCode"]);
            pgCmd.Parameters.AddWithValue("@payment_term_name", reader["PTDescription"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@is_deleted", false); // Default: false
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}