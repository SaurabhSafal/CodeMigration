using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class MaterialGroupMasterMigration : MigrationService
{
    protected override string SelectQuery => "SELECT MaterialGroupId, SAPClientId, MaterialGroupCode, MaterialGroupName, MaterialGroupDescription, IsActive FROM TBL_MaterialGroupMaster";
    protected override string InsertQuery => @"INSERT INTO material_group_master (material_group_id, company_id, material_group_code, material_group_name, material_group_description, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@material_group_id, @company_id, @material_group_code, @material_group_name, @material_group_description, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public MaterialGroupMasterMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string> 
        { 
            "Direct",           // material_group_id
            "FK",               // company_id
            "Direct",           // material_group_code
            "Direct",           // material_group_name
            "Direct",           // material_group_description
            "Default: 0",       // created_by
            "Default: Now",     // created_date
            "Default: null",    // modified_by
            "Default: null",    // modified_date
            "IsActive->IsDeleted", // is_deleted (inverted from IsActive)
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
            pgCmd.Parameters.AddWithValue("@material_group_id", reader["MaterialGroupId"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["SAPClientId"]);
            pgCmd.Parameters.AddWithValue("@material_group_code", reader["MaterialGroupCode"]);
            pgCmd.Parameters.AddWithValue("@material_group_name", reader["MaterialGroupName"]);
            pgCmd.Parameters.AddWithValue("@material_group_description", reader["MaterialGroupDescription"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
            bool isActive = (int)reader["IsActive"] == 1;
            pgCmd.Parameters.AddWithValue("@is_deleted", !isActive); // is_deleted = !isActive
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}