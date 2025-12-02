using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

public class PRBoqItemsMigration : MigrationService
{
    private const int BATCH_SIZE = 500;
    private readonly ILogger<PRBoqItemsMigration> _logger;

    protected override string SelectQuery => @"
SELECT
    ItemId,
    PRID,
    PRTRANSID,
    ICode,
    IName,
    IUOM,
    IQty,
    IRate,
    Remarks,
    ICurrency,
    Line_Number,
    Prise_Unit,
    Net_Value_In_Document_Currency,
    Material_Group,
    Serial_Number_For_Preq_Account,
    GL_Account,
    COST_CENTER,
    LONG_TEXT,
    NETWORK,
    WBS_ELEMENT,
    0 AS created_by,
    NULL AS created_date,
    0 AS modified_by,
    NULL AS modified_date,
    0 AS is_deleted,
    NULL AS deleted_by,
    NULL AS deleted_date
FROM tbl_PRBOQItems
";

    protected override string InsertQuery => @"
INSERT INTO pr_boq_items (
    pr_boq_id, erp_pr_lines_id, pr_boq_material_code, pr_boq_name, pr_boq_description, 
    pr_boq_rem_qty, pr_boq_status, pr_boq_uom_code, pr_boq_qty, pr_boq_rate, 
    pr_boq_remark, pr_boq_currency, pr_boq_line_number, pr_boq_unit_price, pr_boq_total_value, 
    pr_boq_material_group, pr_boq_serial_number_for_preq_account, pr_boq_gl_account, pr_boq_cost_center, 
    pr_boq_long_text, pr_boq_network, pr_boq_wbs_element, created_by, created_date, 
    modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @pr_boq_id, @erp_pr_lines_id, @pr_boq_material_code, @pr_boq_name, @pr_boq_description, 
    @pr_boq_rem_qty, @pr_boq_status, @pr_boq_uom_code, @pr_boq_qty, @pr_boq_rate, 
    @pr_boq_remark, @pr_boq_currency, @pr_boq_line_number, @pr_boq_unit_price, @pr_boq_total_value, 
    @pr_boq_material_group, @pr_boq_serial_number_for_preq_account, @pr_boq_gl_account, @pr_boq_cost_center, 
    @pr_boq_long_text, @pr_boq_network, @pr_boq_wbs_element, @created_by, @created_date, 
    @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)";

    public PRBoqItemsMigration(IConfiguration configuration, ILogger<PRBoqItemsMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // pr_boq_id
        "Direct", // erp_pr_lines_id
        "Direct", // pr_boq_material_code
        "Direct", // pr_boq_name
        "Direct", // pr_boq_description
        "Direct", // pr_boq_rem_qty
        "Direct", // pr_boq_status
        "Direct", // pr_boq_uom_code
        "Direct", // pr_boq_qty
        "Direct", // pr_boq_rate
        "Direct", // pr_boq_remark
        "Direct", // pr_boq_currency
        "Direct", // pr_boq_line_number
        "Direct", // pr_boq_unit_price
        "Direct", // pr_boq_total_value
        "Direct", // pr_boq_material_group
        "Direct", // pr_boq_serial_number_for_preq_account
        "Direct", // pr_boq_gl_account
        "Direct", // pr_boq_cost_center
        "Direct", // pr_boq_long_text
        "Direct", // pr_boq_network
        "Direct", // pr_boq_wbs_element
        "Direct", // created_by
        "Direct", // created_date
        "Direct", // modified_by
        "Direct", // modified_date
        "Direct", // is_deleted
        "Direct", // deleted_by
        "Direct"  // deleted_date
    };

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "ItemId", logic = "ItemId -> pr_boq_id (Direct)", target = "pr_boq_id" },
            new { source = "PRTRANSID", logic = "PRTRANSID -> erp_pr_lines_id (Direct)", target = "erp_pr_lines_id" },
            new { source = "ICode", logic = "ICode -> pr_boq_material_code (Direct)", target = "pr_boq_material_code" },
            new { source = "IName", logic = "IName -> pr_boq_name (Direct)", target = "pr_boq_name" },
            new { source = "IName", logic = "IName -> pr_boq_description (Direct)", target = "pr_boq_description" },
            new { source = "IQty", logic = "IQty -> pr_boq_rem_qty (Direct)", target = "pr_boq_rem_qty" },
            new { source = "-", logic = "pr_boq_status -> 1 (Fixed Default)", target = "pr_boq_status" },
            new { source = "IUOM", logic = "IUOM -> pr_boq_uom_code (Direct)", target = "pr_boq_uom_code" },
            new { source = "IQty", logic = "IQty -> pr_boq_qty (Direct)", target = "pr_boq_qty" },
            new { source = "IRate", logic = "IRate -> pr_boq_rate (Direct)", target = "pr_boq_rate" },
            new { source = "Remarks", logic = "Remarks -> pr_boq_remark (Direct)", target = "pr_boq_remark" },
            new { source = "ICurrency", logic = "ICurrency -> pr_boq_currency (Direct)", target = "pr_boq_currency" },
            new { source = "Line_Number", logic = "Line_Number -> pr_boq_line_number (Direct)", target = "pr_boq_line_number" },
            new { source = "Prise_Unit", logic = "Prise_Unit -> pr_boq_unit_price (Direct)", target = "pr_boq_unit_price" },
            new { source = "Net_Value_In_Document_Currency", logic = "Net_Value_In_Document_Currency -> pr_boq_total_value (Direct)", target = "pr_boq_total_value" },
            new { source = "Material_Group", logic = "Material_Group -> pr_boq_material_group (Direct)", target = "pr_boq_material_group" },
            new { source = "Serial_Number_For_Preq_Account", logic = "Serial_Number_For_Preq_Account -> pr_boq_serial_number_for_preq_account (Direct)", target = "pr_boq_serial_number_for_preq_account" },
            new { source = "GL_Account", logic = "GL_Account -> pr_boq_gl_account (Direct)", target = "pr_boq_gl_account" },
            new { source = "COST_CENTER", logic = "COST_CENTER -> pr_boq_cost_center (Direct)", target = "pr_boq_cost_center" },
            new { source = "LONG_TEXT", logic = "LONG_TEXT -> pr_boq_long_text (Direct)", target = "pr_boq_long_text" },
            new { source = "NETWORK", logic = "NETWORK -> pr_boq_network (Direct)", target = "pr_boq_network" },
            new { source = "WBS_ELEMENT", logic = "WBS_ELEMENT -> pr_boq_wbs_element (Direct)", target = "pr_boq_wbs_element" },
            new { source = "-", logic = "created_by -> 0 (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> 0 (Fixed Default)", target = "modified_by" },
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

    private async Task<HashSet<int>> LoadValidErpPrLinesIdsAsync(NpgsqlConnection pgConn, NpgsqlTransaction? transaction)
    {
        var validIds = new HashSet<int>();
        var query = "SELECT erp_pr_lines_id FROM erp_pr_lines";
        using var cmd = new NpgsqlCommand(query, pgConn, transaction);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            validIds.Add(reader.GetInt32(0));
        }
        return validIds;
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        // Load valid ERP PR Lines IDs
        var validErpPrLinesIds = await LoadValidErpPrLinesIdsAsync(pgConn, transaction);
        _logger.LogInformation($"Loaded {validErpPrLinesIds.Count} valid ERP PR Lines IDs from erp_pr_lines.");
        
        int insertedCount = 0;
        int skippedCount = 0;
        int batchNumber = 0;
        var batch = new List<Dictionary<string, object>>();

        using var selectCmd = new SqlCommand(SelectQuery, sqlConn);
        using var reader = await selectCmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            // Validate ERP PR Lines ID (using PRTRANSID which maps to erp_pr_lines_id)
            var erpPrLinesIdValue = reader["PRTRANSID"];
            if (erpPrLinesIdValue != DBNull.Value)
            {
                int erpPrLinesId = Convert.ToInt32(erpPrLinesIdValue);
                
                // Skip if ERP PR Lines ID not present in erp_pr_lines
                if (!validErpPrLinesIds.Contains(erpPrLinesId))
                {
                    _logger.LogWarning($"Skipping ItemId {reader["ItemId"]}: ERP PR Lines ID {erpPrLinesId} not found in erp_pr_lines.");
                    skippedCount++;
                    continue;
                }
            }
            else
            {
                _logger.LogWarning($"Skipping ItemId {reader["ItemId"]}: ERP PR Lines ID is NULL.");
                skippedCount++;
                continue;
            }
            

            var record = new Dictionary<string, object>
            {
                ["pr_boq_id"] = reader["ItemId"] ?? DBNull.Value,
                ["erp_pr_lines_id"] = reader["PRTRANSID"] ?? DBNull.Value,
                ["pr_boq_material_code"] = reader["ICode"] ?? DBNull.Value,
                ["pr_boq_name"] = reader["IName"] ?? DBNull.Value,
                ["pr_boq_description"] = DBNull.Value, // New column - not in source
                ["pr_boq_rem_qty"] = 0, // Default to 0 as column has NOT NULL constraint
                ["pr_boq_status"] = DBNull.Value, // New column - not in source
                ["pr_boq_uom_code"] = reader["IUOM"] ?? DBNull.Value,
                ["pr_boq_qty"] = reader["IQty"] != DBNull.Value ? Convert.ToDecimal(reader["IQty"]) : (object)DBNull.Value,
                ["pr_boq_rate"] = reader["IRate"] != DBNull.Value ? Convert.ToDecimal(reader["IRate"]) : (object)DBNull.Value,
                ["pr_boq_remark"] = reader["Remarks"] ?? DBNull.Value,
                ["pr_boq_currency"] = reader["ICurrency"] ?? DBNull.Value,
                ["pr_boq_line_number"] = reader["Line_Number"] ?? DBNull.Value,
                ["pr_boq_unit_price"] = reader["Prise_Unit"] != DBNull.Value ? Convert.ToDecimal(reader["Prise_Unit"]) : (object)DBNull.Value,
                ["pr_boq_total_value"] = reader["Net_Value_In_Document_Currency"] != DBNull.Value ? Convert.ToDecimal(reader["Net_Value_In_Document_Currency"]) : (object)DBNull.Value,
                ["pr_boq_material_group"] = reader["Material_Group"] ?? DBNull.Value,
                ["pr_boq_serial_number_for_preq_account"] = reader["Serial_Number_For_Preq_Account"] ?? DBNull.Value,
                ["pr_boq_gl_account"] = reader["GL_Account"] ?? DBNull.Value,
                ["pr_boq_cost_center"] = reader["COST_CENTER"] ?? DBNull.Value,
                ["pr_boq_long_text"] = reader["LONG_TEXT"] ?? DBNull.Value,
                ["pr_boq_network"] = reader["NETWORK"] ?? DBNull.Value,
                ["pr_boq_wbs_element"] = reader["WBS_ELEMENT"] ?? DBNull.Value,
                ["created_by"] = reader["created_by"] ?? DBNull.Value,
                ["created_date"] = reader["created_date"] ?? DBNull.Value,
                ["modified_by"] = reader["modified_by"] ?? DBNull.Value,
                ["modified_date"] = reader["modified_date"] ?? DBNull.Value,
                ["is_deleted"] = Convert.ToInt32(reader["is_deleted"]) == 1,
                ["deleted_by"] = reader["deleted_by"] ?? DBNull.Value,
                ["deleted_date"] = reader["deleted_date"] ?? DBNull.Value
            };

            batch.Add(record);

            if (batch.Count >= BATCH_SIZE)
            {
                batchNumber++;
                _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
                insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
                _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            _logger.LogInformation($"Starting batch {batchNumber} with {batch.Count} records...");
            insertedCount += await InsertBatchAsync(pgConn, batch, transaction, batchNumber);
            _logger.LogInformation($"Completed batch {batchNumber}. Total records inserted so far: {insertedCount}");
        }

        _logger.LogInformation($"Migration finished. Total records inserted: {insertedCount}, Skipped: {skippedCount}");
        return insertedCount;
    }

    private async Task<int> InsertBatchAsync(NpgsqlConnection pgConn, List<Dictionary<string, object>> batch, NpgsqlTransaction? transaction, int batchNumber)
    {
        if (batch.Count == 0) return 0;

        var columns = new List<string> {
            "pr_boq_id", "erp_pr_lines_id", "pr_boq_material_code", "pr_boq_name", "pr_boq_description",
            "pr_boq_rem_qty", "pr_boq_status", "pr_boq_uom_code", "pr_boq_qty", "pr_boq_rate",
            "pr_boq_remark", "pr_boq_currency", "pr_boq_line_number", "pr_boq_unit_price", "pr_boq_total_value",
            "pr_boq_material_group", "pr_boq_serial_number_for_preq_account", "pr_boq_gl_account", "pr_boq_cost_center",
            "pr_boq_long_text", "pr_boq_network", "pr_boq_wbs_element", "created_by", "created_date",
            "modified_by", "modified_date", "is_deleted", "deleted_by", "deleted_date"
        };

        var valueRows = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        int paramIndex = 0;

        foreach (var record in batch)
        {
            var valuePlaceholders = new List<string>();
            foreach (var col in columns)
            {
                var paramName = $"@p{paramIndex}";
                valuePlaceholders.Add(paramName);
                parameters.Add(new NpgsqlParameter(paramName, record[col] ?? DBNull.Value));
                paramIndex++;
            }
            valueRows.Add($"({string.Join(", ", valuePlaceholders)})");
        }

        var sql = $"INSERT INTO pr_boq_items ({string.Join(", ", columns)}) VALUES {string.Join(", ", valueRows)}";
        using var insertCmd = new NpgsqlCommand(sql, pgConn, transaction);
        insertCmd.Parameters.AddRange(parameters.ToArray());

        int result = await insertCmd.ExecuteNonQueryAsync();
        _logger.LogInformation($"Batch {batchNumber}: Inserted {result} records into pr_boq_items.");
        return result;
    }
}
