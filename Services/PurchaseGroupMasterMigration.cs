using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class PurchaseGroupMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT PurchaseGroupId, ClientSAPId, PurchaseGroupCode, PurchaseGroupName FROM TBL_PurchaseGroupMaster";
    protected override string InsertQuery => @"INSERT INTO purchase_group_master (purchase_group_id, company_id, purchase_group_code, purchase_group_name, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@purchase_group_id, @company_id, @purchase_group_code, @purchase_group_name, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PurchaseGroupMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // purchase_group_id
            "FK",               // company_id
            "Direct",           // purchase_group_code
            "Direct",           // purchase_group_name
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
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();

        using var sqlCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(InsertQuery, pgConn);
        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@purchase_group_id", reader["PurchaseGroupId"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@purchase_group_code", reader["PurchaseGroupCode"]);
            pgCmd.Parameters.AddWithValue("@purchase_group_name", reader["PurchaseGroupName"]);
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