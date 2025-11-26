using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class ErpPrLinesMigration : MigrationService
{
    // 1. The SQL query covers all fields and lookups as per your mapping.
    protected override string SelectQuery => @"
SELECT
    t.PRTRANSID,
    pm.IsNumberGeneration AS temp_pr,
    t.BUYERID AS user_id,
    u.FULL_NAME AS user_full_name,
    t.PRStatus AS pr_status,
    pm.PR_NUM AS pr_number,
    t.PR_LINE AS pr_line,
    pm.DESCRIPTION AS header_text,
    t.ClientSAPId AS company_id,
    t.ClientSAPCode AS company_code,
    t.UOMId AS uom_id,
    t.UOMCODE AS uom_code,
    t.Plant AS plant_id,
    t.PlantCode AS plant_code,
    t.MaterialGroupId AS material_group_id,
    t.MaterialGroup AS material_group_code,
    t.PurchasingGroup AS purchase_group_id,
    t.PurchaseGroupCode AS purchase_group_code,
    t.ItemCode AS material_code,
    t.ItemName AS material_short_text,
    t.ITEM_DESCRIPTION AS material_item_text,
    CASE WHEN ISNULL(t.ItemCode, '') <> '' THEN 'Material' ELSE 'Service' END AS item_type,
    t.MaterialPODescription AS material_po_description,
    t.AMOUNT AS amount,
    t.UNIT_PRICE AS unit_price,
    t.RemQty AS rem_qty,
    t.QUANTITY AS qty,
    t.PONo AS po_number,
    t.PODate AS po_creation_date,
    t.POQty AS po_qty,
    t.POVendorCode AS po_vendor_code,
    t.POVendorName AS po_vendor_name,
    t.POTotalGrossAmount AS po_item_value,
    t.LastPONo AS lpo_number,
    t.POItemNo AS lpo_line_number,
    t.PODocType AS lpo_doc_type,
    t.LastPODate AS lpo_creation_date,
    t.POUom AS lpo_uom,
    t.POUnitPrice AS lpo_unit_price,
    t.POItemCurrency AS lpo_line_currency,
    t.VendorCode AS lpo_vendor_code,
    t.VendorName AS lpo_vendor_name,
    t.LastPODate AS lpo_date,
    t.LastPOQty AS lpo_qty,
    t.TotalStock AS total_stock,
    t.CostCenter AS cost_center,
    t.StoreLocation AS store_location,
    t.Department AS department,
    t.AcctAssignmentCat AS acct_assignment_cat,
    t.AcctAssignmentCatDesc AS acct_assignment_cat_desc,
    t.PROJECT_ID AS wbs_element_code,
    NULL AS wbs_element_name, -- Placeholder. Add join/lookup if needed.
    t.CurrencyCode AS currency_code,
    t.TrackingNumber AS tracking_number,
    pm.CreatedBy AS erp_created_by,
    pm.RequestBy AS erp_request_by,
    t.RequestDate AS erp_change_on_date,
    t.DeliveryDate AS delivery_date,
    t.is_closed,
    t.created_by,
    t.created_date,
    t.modified_by,
    t.modified_date,
    CASE WHEN t.DeletionIndicator = 'X' THEN TRUE ELSE FALSE END AS is_deleted,
    t.deleted_by,
    t.deleted_date
FROM TBL_PRTRANSACTION t
LEFT JOIN TBL_PRMASTER pm ON pm.PRID = t.PRID
LEFT JOIN TBL_USERMASTERFINAL u ON u.PERSON_ID = t.BUYERID
";

    // 2. The Postgres insert covers all fields as per your mapping
    protected override string InsertQuery => @"
INSERT INTO erp_pr_lines (
    erp_pr_lines_id, temp_pr, user_id, user_full_name, pr_status, pr_number, pr_line, header_text, company_id, company_code,
    uom_id, uom_code, plant_id, plant_code, material_group_id, material_group_code, purchase_group_id, purchase_group_code,
    material_code, material_short_text, material_item_text, item_type, material_po_description, amount, unit_price, rem_qty,
    qty, po_number, po_creation_date, po_qty, po_vendor_code, po_vendor_name, po_item_value, lpo_number, lpo_line_number, lpo_doc_type,
    lpo_creation_date, lpo_uom, lpo_unit_price, lpo_line_currency, lpo_vendor_code, lpo_vendor_name, lpo_date, lpo_qty,
    total_stock, cost_center, store_location, department, acct_assignment_cat, acct_assignment_cat_desc, wbs_element_code, wbs_element_name,
    currency_code, tracking_number, erp_created_by, erp_request_by, erp_change_on_date, delivery_date, is_closed, created_by,
    created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
) VALUES (
    @erp_pr_lines_id, @temp_pr, @user_id, @user_full_name, @pr_status, @pr_number, @pr_line, @header_text, @company_id, @company_code,
    @uom_id, @uom_code, @plant_id, @plant_code, @material_group_id, @material_group_code, @purchase_group_id, @purchase_group_code,
    @material_code, @material_short_text, @material_item_text, @item_type, @material_po_description, @amount, @unit_price, @rem_qty,
    @qty, @po_number, @po_creation_date, @po_qty, @po_vendor_code, @po_vendor_name, @po_item_value, @lpo_number, @lpo_line_number, @lpo_doc_type,
    @lpo_creation_date, @lpo_uom, @lpo_unit_price, @lpo_line_currency, @lpo_vendor_code, @lpo_vendor_name, @lpo_date, @lpo_qty,
    @total_stock, @cost_center, @store_location, @department, @acct_assignment_cat, @acct_assignment_cat_desc, @wbs_element_code, @wbs_element_name,
    @currency_code, @tracking_number, @erp_created_by, @erp_request_by, @erp_change_on_date, @delivery_date, @is_closed, @created_by,
    @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date
)";

    public ErpPrLinesMigration(IConfiguration configuration) : base(configuration) { }

    protected override List<string> GetLogics() => new List<string>
    {
        "Direct", // erp_pr_lines_id
        "Direct", // temp_pr
        "Direct", // user_id
        "Direct", // user_full_name
        "Direct", // pr_status
        "Direct", // pr_number
        "Direct", // pr_line
        "Direct", // header_text
        "Direct", // company_id
        "Direct", // company_code
        "Direct", // uom_id
        "Direct", // uom_code
        "Direct", // plant_id
        "Direct", // plant_code
        "Direct", // material_group_id
        "Direct", // material_group_code
        "Direct", // purchase_group_id
        "Direct", // purchase_group_code
        "Direct", // material_code
        "Direct", // material_short_text
        "Direct", // material_item_text
        "Direct", // item_type
        "Direct", // material_po_description
        "Direct", // amount
        "Direct", // unit_price
        "Direct", // rem_qty
        "Direct", // qty
        "Direct", // po_number
        "Direct", // po_creation_date
        "Direct", // po_qty
        "Direct", // po_vendor_code
        "Direct", // po_vendor_name
        "Direct", // po_item_value
        "Direct", // lpo_number
        "Direct", // lpo_line_number
        "Direct", // lpo_doc_type
        "Direct", // lpo_creation_date
        "Direct", // lpo_uom
        "Direct", // lpo_unit_price
        "Direct", // lpo_line_currency
        "Direct", // lpo_vendor_code
        "Direct", // lpo_vendor_name
        "Direct", // lpo_date
        "Direct", // lpo_qty
        "Direct", // total_stock
        "Direct", // cost_center
        "Direct", // store_location
        "Direct", // department
        "Direct", // acct_assignment_cat
        "Direct", // acct_assignment_cat_desc
        "Direct", // wbs_element_code
        "Direct", // wbs_element_name
        "Direct", // currency_code
        "Direct", // tracking_number
        "Direct", // erp_created_by
        "Direct", // erp_request_by
        "Direct", // erp_change_on_date
        "Direct", // delivery_date
        "Direct", // is_closed
        "Direct", // created_by
        "Direct", // created_date
        "Direct", // modified_by
        "Direct", // modified_date
        "Direct", // is_deleted
        "Direct", // deleted_by
        "Direct"  // deleted_date
    };

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

            pgCmd.Parameters.AddWithValue("@erp_pr_lines_id", reader["PRTRANSID"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@temp_pr", reader["temp_pr"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@user_id", reader["user_id"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@user_full_name", reader["user_full_name"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@pr_status", reader["pr_status"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@pr_number", reader["pr_number"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@pr_line", reader["pr_line"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@header_text", reader["header_text"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@company_id", reader["company_id"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@company_code", reader["company_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@uom_id", reader["uom_id"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@uom_code", reader["uom_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@plant_id", reader["plant_id"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@plant_code", reader["plant_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@material_group_id", reader["material_group_id"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@material_group_code", reader["material_group_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@purchase_group_id", reader["purchase_group_id"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@purchase_group_code", reader["purchase_group_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@material_code", reader["material_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@material_short_text", reader["material_short_text"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@material_item_text", reader["material_item_text"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@item_type", reader["item_type"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@material_po_description", reader["material_po_description"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@amount", reader["amount"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@unit_price", reader["unit_price"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@rem_qty", reader["rem_qty"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@qty", reader["qty"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@po_number", reader["po_number"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@po_creation_date", reader["po_creation_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@po_qty", reader["po_qty"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@po_vendor_code", reader["po_vendor_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@po_vendor_name", reader["po_vendor_name"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@po_item_value", reader["po_item_value"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_number", reader["lpo_number"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_line_number", reader["lpo_line_number"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_doc_type", reader["lpo_doc_type"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_creation_date", reader["lpo_creation_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_uom", reader["lpo_uom"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_unit_price", reader["lpo_unit_price"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_line_currency", reader["lpo_line_currency"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_vendor_code", reader["lpo_vendor_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_vendor_name", reader["lpo_vendor_name"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_date", reader["lpo_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@lpo_qty", reader["lpo_qty"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@total_stock", reader["total_stock"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@cost_center", reader["cost_center"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@store_location", reader["store_location"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@department", reader["department"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@acct_assignment_cat", reader["acct_assignment_cat"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@acct_assignment_cat_desc", reader["acct_assignment_cat_desc"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@wbs_element_code", reader["wbs_element_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@wbs_element_name", reader["wbs_element_name"] ?? DBNull.Value); // Null by default
            pgCmd.Parameters.AddWithValue("@currency_code", reader["currency_code"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@tracking_number", reader["tracking_number"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@erp_created_by", reader["erp_created_by"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@erp_request_by", reader["erp_request_by"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@erp_change_on_date", reader["erp_change_on_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@delivery_date", reader["delivery_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_closed", reader["is_closed"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@created_by", reader["created_by"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@created_date", reader["created_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@modified_by", reader["modified_by"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@modified_date", reader["modified_date"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_deleted", reader["is_deleted"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@deleted_by", reader["deleted_by"] ?? DBNull.Value);
            pgCmd.Parameters.AddWithValue("@deleted_date", reader["deleted_date"] ?? DBNull.Value);

            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0)
                insertedCount++;
        }
        return insertedCount;
    }
}