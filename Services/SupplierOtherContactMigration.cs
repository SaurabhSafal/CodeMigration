using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using DataMigration.Services;

public class SupplierOtherContactMigration : MigrationService
{
    private HashSet<int> _validSupplierIds = new HashSet<int>();
    private const int BATCH_SIZE = 1000;
    protected override string SelectQuery => @"
        SELECT 
            ComunicationID,
            VendorID,
            Name,
            MobileNo,
            WhatsAppNo,
            Email,
            TimeZone,
            IsSales,
            IsSpares,
            IsService,
            OperationCapital,
            OperationSpare,
            OperationServices,
            IsFinance,
            AddDateTime
        FROM TBL_COMUNICATION
        ORDER BY ComunicationID";

    protected override string InsertQuery => @"
        INSERT INTO supplier_other_contact (
            supplier_other_contact_id,
            supplier_id,
            contact_name,
            contact_number,
            contact_email_id,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @supplier_other_contact_id,
            @supplier_id,
            @contact_name,
            @contact_number,
            @contact_email_id,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )";

    public SupplierOtherContactMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "ComunicationID -> supplier_other_contact_id (Direct)",
            "VendorID -> supplier_id (Direct)",
            "Name -> contact_name (Direct)",
            "MobileNo -> contact_number (Direct)",
            "Email -> contact_email_id (Direct)",
            // Additional fields from MSSQL are ignored as they have no direct mapping
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
            new { source = "ComunicationID", logic = "ComunicationID -> supplier_other_contact_id (Direct)", target = "supplier_other_contact_id" },
            new { source = "VendorID", logic = "VendorID -> supplier_id (Direct)", target = "supplier_id" },
            new { source = "Name", logic = "Name -> contact_name (Direct)", target = "contact_name" },
            new { source = "MobileNo", logic = "MobileNo -> contact_number (Direct)", target = "contact_number" },
            new { source = "Email", logic = "Email -> contact_email_id (Direct)", target = "contact_email_id" },
            new { source = "-", logic = "created_by -> 0 (Fixed)", target = "created_by" },
            new { source = "-", logic = "created_date -> NOW() (Generated)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // Cache valid supplier_ids from supplier_master
        _validSupplierIds = new HashSet<int>();
        using (var cmd = new NpgsqlCommand("SELECT supplier_id FROM supplier_master", pgConn, transaction))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                _validSupplierIds.Add(reader.GetInt32(0));
            }
        }

        int insertedCount = 0;
        var batch = new List<Dictionary<string, object>>();
        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader2 = await selectCmd.ExecuteReaderAsync();
        while (await reader2.ReadAsync())
        {
            var supplierIdObj = reader2["VendorID"];
            int supplierId = supplierIdObj == DBNull.Value ? 0 : Convert.ToInt32(supplierIdObj);
            // Only skip if supplier_id is not present in supplier_master
            if (!_validSupplierIds.Contains(supplierId))
                continue;

            var record = new Dictionary<string, object>
            {
                ["@supplier_other_contact_id"] = reader2["ComunicationID"],
                ["@supplier_id"] = supplierId,
                ["@contact_name"] = reader2["Name"] ?? (object)DBNull.Value,
                ["@contact_number"] = reader2["MobileNo"] ?? (object)DBNull.Value,
                ["@contact_email_id"] = reader2["Email"] ?? (object)DBNull.Value,
                ["@created_by"] = 0,
                ["@created_date"] = DateTime.UtcNow,
                ["@modified_by"] = DBNull.Value,
                ["@modified_date"] = DBNull.Value,
                ["@is_deleted"] = false,
                ["@deleted_by"] = DBNull.Value,
                ["@deleted_date"] = DBNull.Value
            };
            batch.Add(record);
            if (batch.Count >= BATCH_SIZE)
            {
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction);
                batch.Clear();
            }
        }
        if (batch.Count > 0)
        {
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction);
        }
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction = null)
    {
        int count = 0;
        foreach (var record in batch)
        {
            using var insertCmd = new NpgsqlCommand(InsertQuery, pgConn, transaction);
            foreach (var kvp in record)
            {
                insertCmd.Parameters.AddWithValue(kvp.Key, kvp.Value ?? DBNull.Value);
            }
            count += await insertCmd.ExecuteNonQueryAsync();
        }
        return count;
    }
    }

