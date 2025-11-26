using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class CurrencyMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT CurrencyMastID, Currency_Code, Currency_Name FROM TBL_CURRENCYMASTER";
    protected override string InsertQuery => @"INSERT INTO currency_master (currency_id, company_id, currency_code, currency_name, currency_short_name, decimal_places, iso_code, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@currency_id, @company_id, @currency_code, @currency_name, @currency_short_name, @decimal_places, @iso_code, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public CurrencyMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // currency_id
            "Default: 1",       // company_id
            "Direct",           // currency_code
            "Direct",           // currency_name
            "Default: null",    // currency_short_name
            "Default: 2",       // decimal_places
            "Default: null",    // iso_code
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
            pgCmd.Parameters.AddWithValue("@currency_id", reader["CurrencyMastID"]);
            pgCmd.Parameters.AddWithValue("@company_id", 1); // Default: 1
            pgCmd.Parameters.AddWithValue("@currency_code", reader["Currency_Code"]);
            pgCmd.Parameters.AddWithValue("@currency_name", reader["Currency_Name"]);
            pgCmd.Parameters.AddWithValue("@currency_short_name", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@decimal_places", 2); // Changed to default: 2 to avoid NOT NULL constraint
            pgCmd.Parameters.AddWithValue("@iso_code", DBNull.Value); // Default: null
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