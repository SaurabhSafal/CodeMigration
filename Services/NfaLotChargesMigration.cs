using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using DataMigration.Services;

public class NfaLotChargesMigration : MigrationService
{
    private const int BATCH_SIZE = 1000;
    private readonly ILogger<NfaLotChargesMigration> _logger;
    private MigrationLogger? _migrationLogger;

    public NfaLotChargesMigration(IConfiguration configuration, ILogger<NfaLotChargesMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    public MigrationLogger? GetLogger() => _migrationLogger;

    protected override string SelectQuery => @"
        SELECT
            TBL_AWARDEVENTLOTCHARGES.AwardEventLotChargeId,
            TBL_AWARDEVENTLOTCHARGES.AWARDEVENTMAINID,
            TBL_AWARDEVENTLOTCHARGES.PB_BuyerChargesId,
            TBL_AWARDEVENTLOTCHARGES.Percentage,
            TBL_AWARDEVENTLOTCHARGES.Amount,
            TBL_AWARDEVENTLOTCHARGES.ChargesName,
            TBL_AWARDEVENTLOTCHARGES.GSTPer,
            TBL_AWARDEVENTLOTCHARGES.GSTAmount
        FROM TBL_AWARDEVENTLOTCHARGES
        ORDER BY TBL_AWARDEVENTLOTCHARGES.AwardEventLotChargeId";

    protected override string InsertQuery => @"
        INSERT INTO nfa_lot_charges (
            nfa_lot_charges_id,
            nfa_header_id,
            user_price_bid_lot_charges_id,
            price_bid_charges_id,
            percentage,
            basic_lot_charges_amount,
            charges_name,
            tax_percentage,
            tax_amount,
            tax_master_id,
            total_lot_charges_amount,
            created_by,
            created_date,
            modified_by,
            modified_date,
            is_deleted,
            deleted_by,
            deleted_date
        ) VALUES (
            @nfa_lot_charges_id,
            @nfa_header_id,
            @user_price_bid_lot_charges_id,
            @price_bid_charges_id,
            @percentage,
            @basic_lot_charges_amount,
            @charges_name,
            @tax_percentage,
            @tax_amount,
            @tax_master_id,
            @total_lot_charges_amount,
            @created_by,
            @created_date,
            @modified_by,
            @modified_date,
            @is_deleted,
            @deleted_by,
            @deleted_date
        )
        ON CONFLICT (nfa_lot_charges_id) DO UPDATE SET
            nfa_header_id = EXCLUDED.nfa_header_id,
            user_price_bid_lot_charges_id = EXCLUDED.user_price_bid_lot_charges_id,
            price_bid_charges_id = EXCLUDED.price_bid_charges_id,
            percentage = EXCLUDED.percentage,
            basic_lot_charges_amount = EXCLUDED.basic_lot_charges_amount,
            charges_name = EXCLUDED.charges_name,
            tax_percentage = EXCLUDED.tax_percentage,
            tax_amount = EXCLUDED.tax_amount,
            tax_master_id = EXCLUDED.tax_master_id,
            total_lot_charges_amount = EXCLUDED.total_lot_charges_amount,
            modified_by = EXCLUDED.modified_by,
            modified_date = EXCLUDED.modified_date,
            is_deleted = EXCLUDED.is_deleted,
            deleted_by = EXCLUDED.deleted_by,
            deleted_date = EXCLUDED.deleted_date";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "Direct",  // nfa_lot_charges_id
            "Direct",  // nfa_header_id
            "Direct",  // user_price_bid_lot_charges_id
            "Lookup",  // price_bid_charges_id
            "Direct",  // percentage
            "Direct",  // basic_lot_charges_amount
            "Direct",  // charges_name
            "Direct",  // tax_percentage
            "Direct",  // tax_amount
            "Lookup",  // tax_master_id
            "Calculated", // total_lot_charges_amount
            "Fixed",   // created_by
            "Fixed",   // created_date
            "Fixed",   // modified_by
            "Fixed",   // modified_date
            "Fixed",   // is_deleted
            "Fixed",   // deleted_by
            "Fixed"    // deleted_date
        };
    }

    public override List<object> GetMappings()
    {
        return new List<object>
        {
            new { source = "AwardEventLotChargeId", logic = "AwardEventLotChargeId -> nfa_lot_charges_id (Primary key autoincrement)", target = "nfa_lot_charges_id" },
            new { source = "AWARDEVENTMAINID", logic = "AWARDEVENTMAINID -> nfa_header_id (Foreign key to nfa_header - NFAHeaderId)", target = "nfa_header_id" },
            new { source = "PB_BuyerChargesId", logic = "PB_BuyerChargesId -> user_price_bid_lot_charges_id (Foreign key to user_price_bid_lot_charges - UserPriceBidLotChargesID)", target = "user_price_bid_lot_charges_id" },
            new { source = "PB_BuyerChargesId", logic = "PB_BuyerChargesId -> price_bid_charges_id (Lookup from price_bid_charges_master - PriceBidChargesId)", target = "price_bid_charges_id" },
            new { source = "Percentage", logic = "Percentage -> percentage (Percentage)", target = "percentage" },
            new { source = "Amount", logic = "Amount -> basic_lot_charges_amount (BasicLotChargesAmount)", target = "basic_lot_charges_amount" },
            new { source = "ChargesName", logic = "ChargesName -> charges_name (ChargesName)", target = "charges_name" },
            new { source = "GSTPer", logic = "GSTPer -> tax_percentage (TaxPercentage)", target = "tax_percentage" },
            new { source = "GSTAmount", logic = "GSTAmount -> tax_amount (TaxAmount)", target = "tax_amount" },
            new { source = "GSTPer", logic = "GSTPer -> tax_master_id (Lookup from tax_master via GSTPer - TaxMasterId)", target = "tax_master_id" },
            new { source = "Amount, GSTAmount", logic = "(Amount + GSTAmount) -> total_lot_charges_amount (TotalLotChargesAmount)", target = "total_lot_charges_amount" },
            new { source = "-", logic = "created_by -> NULL (Fixed Default)", target = "created_by" },
            new { source = "-", logic = "created_date -> NULL (Fixed Default)", target = "created_date" },
            new { source = "-", logic = "modified_by -> NULL (Fixed Default)", target = "modified_by" },
            new { source = "-", logic = "modified_date -> NULL (Fixed Default)", target = "modified_date" },
            new { source = "-", logic = "is_deleted -> false (Fixed Default)", target = "is_deleted" },
            new { source = "-", logic = "deleted_by -> NULL (Fixed Default)", target = "deleted_by" },
            new { source = "-", logic = "deleted_date -> NULL (Fixed Default)", target = "deleted_date" }
        };
    }

    protected override async Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null)
    {
        _migrationLogger = new MigrationLogger(_logger, "nfa_lot_charges");
        _migrationLogger.LogInfo("Starting migration");

        _logger.LogInformation("Starting NFA Lot Charges migration...");

        int totalRecords = 0;
        int migratedRecords = 0;
        int skippedRecords = 0;

        try
        {
            // Load lookup data
            var priceBidChargesMap = await LoadPriceBidChargesAsync(pgConn);
            var taxMasterMap = await LoadTaxMasterAsync(pgConn);
            
            _logger.LogInformation($"Loaded lookup data: PriceBidCharges={priceBidChargesMap.Count}, TaxMaster={taxMasterMap.Count}");

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConn);
            sqlCommand.CommandTimeout = 300;

            using var reader = await sqlCommand.ExecuteReaderAsync();

            var batch = new List<Dictionary<string, object>>();
            var processedIds = new HashSet<int>();

            while (await reader.ReadAsync())
            {
                totalRecords++;

                var awardEventLotChargesId = reader["AwardEventLotChargeId"];

                // Skip if AwardEventLotChargeId is NULL
                if (awardEventLotChargesId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning("Skipping record - AwardEventLotChargeId is NULL");
                    continue;
                }

                int awardEventLotChargesIdValue = Convert.ToInt32(awardEventLotChargesId);

                // Skip duplicates
                if (processedIds.Contains(awardEventLotChargesIdValue))
                {
                    skippedRecords++;
                    continue;
                }

                // Get nfa_header_id (AWARDEVENTMAINID)
                var awardEventMainId = reader["AWARDEVENTMAINID"];
                if (awardEventMainId == DBNull.Value)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventLotChargesIdValue} - AWARDEVENTMAINID is NULL");
                    continue;
                }

                // Verify nfa_header_id exists in nfa_header table
                int awardEventMainIdValue = Convert.ToInt32(awardEventMainId);
                bool headerExists = await VerifyNfaHeaderExistsAsync(pgConn, awardEventMainIdValue);
                if (!headerExists)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventLotChargesIdValue} - nfa_header_id {awardEventMainIdValue} does not exist in nfa_header table");
                    continue;
                }

                // Lookup price_bid_charges_id from user_price_bid_lot_charges
                var pbBuyerChargesId = reader["PB_BuyerChargesId"];
                int? priceBidChargesId = null;
                
                // Skip if PB_BuyerChargesId is NULL or 0
                if (pbBuyerChargesId == DBNull.Value || Convert.ToInt32(pbBuyerChargesId) == 0)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventLotChargesIdValue} - PB_BuyerChargesId is NULL or 0");
                    continue;
                }
                
                int pbBuyerChargesIdValue = Convert.ToInt32(pbBuyerChargesId);
                if (priceBidChargesMap.ContainsKey(pbBuyerChargesIdValue))
                {
                    priceBidChargesId = priceBidChargesMap[pbBuyerChargesIdValue];
                }
                
                // Skip if price_bid_charges_id lookup failed
                if (!priceBidChargesId.HasValue)
                {
                    skippedRecords++;
                    _logger.LogWarning($"Skipping record {awardEventLotChargesIdValue} - Could not find price_bid_charges_id for PB_BuyerChargesId: {pbBuyerChargesIdValue}");
                    continue;
                }

                // Lookup tax_master_id from tax_master via GSTPer, default to 0 if not found
                var gstPer = reader["GSTPer"];
                int taxMasterId = 0;
                if (gstPer != DBNull.Value)
                {
                    decimal gstPerValue = Convert.ToDecimal(gstPer);
                    if (taxMasterMap.ContainsKey(gstPerValue))
                    {
                        taxMasterId = taxMasterMap[gstPerValue];
                    }
                }

                // Calculate total_lot_charges_amount
                decimal amount = reader["Amount"] != DBNull.Value ? Convert.ToDecimal(reader["Amount"]) : 0;
                decimal gstAmount = reader["GSTAmount"] != DBNull.Value ? Convert.ToDecimal(reader["GSTAmount"]) : 0;
                decimal totalLotChargesAmount = amount + gstAmount;

                var record = new Dictionary<string, object>
                {
                    ["nfa_lot_charges_id"] = awardEventLotChargesIdValue,
                    ["nfa_header_id"] = awardEventMainId,
                    ["user_price_bid_lot_charges_id"] = pbBuyerChargesId ?? DBNull.Value,
                    ["price_bid_charges_id"] = priceBidChargesId.HasValue ? (object)priceBidChargesId.Value : DBNull.Value,
                    ["percentage"] = reader["Percentage"] ?? DBNull.Value,
                    ["basic_lot_charges_amount"] = reader["Amount"] ?? DBNull.Value,
                    ["charges_name"] = reader["ChargesName"] ?? DBNull.Value,
                    ["tax_percentage"] = reader["GSTPer"] ?? DBNull.Value,
                    ["tax_amount"] = reader["GSTAmount"] ?? DBNull.Value,
                    ["tax_master_id"] = taxMasterId,
                    ["total_lot_charges_amount"] = totalLotChargesAmount,
                    ["created_by"] = DBNull.Value,
                    ["created_date"] = DBNull.Value,
                    ["modified_by"] = DBNull.Value,
                    ["modified_date"] = DBNull.Value,
                    ["is_deleted"] = false,
                    ["deleted_by"] = DBNull.Value,
                    ["deleted_date"] = DBNull.Value
                };

                batch.Add(record);
                processedIds.Add(awardEventLotChargesIdValue);

                if (batch.Count >= BATCH_SIZE)
                {
                    int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn, transaction);
                    migratedRecords += batchMigrated;
                    batch.Clear();
                }
            }

            // Insert remaining records
            if (batch.Count > 0)
            {
                int batchMigrated = await InsertBatchWithTransactionAsync(batch, pgConn, transaction);
                migratedRecords += batchMigrated;
            }

            _logger.LogInformation($"NFA Lot Charges migration completed. Total: {totalRecords}, Migrated: {migratedRecords}, Skipped: {skippedRecords}");

            return migratedRecords;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during NFA Lot Charges migration");
            throw;
        }
    }

    private async Task<bool> VerifyNfaHeaderExistsAsync(NpgsqlConnection pgConn, int nfaHeaderId)
    {
        try
        {
            var query = "SELECT EXISTS(SELECT 1 FROM nfa_header WHERE nfa_header_id = @nfaHeaderId)";
            using var command = new NpgsqlCommand(query, pgConn);
            command.Parameters.AddWithValue("@nfaHeaderId", nfaHeaderId);
            var result = await command.ExecuteScalarAsync();
            return result != null && (bool)result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error verifying nfa_header_id {nfaHeaderId} exists");
            return false;
        }
    }

    private async Task<Dictionary<int, int>> LoadPriceBidChargesAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<int, int>();
        try
        {
            var query = "SELECT user_price_bid_lot_charges_id, price_bid_charges_id FROM user_price_bid_lot_charges WHERE user_price_bid_lot_charges_id IS NOT NULL AND price_bid_charges_id IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                map[reader.GetInt32(0)] = reader.GetInt32(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading price bid charges mapping");
        }
        return map;
    }

    private async Task<Dictionary<decimal, int>> LoadTaxMasterAsync(NpgsqlConnection pgConn)
    {
        var map = new Dictionary<decimal, int>();
        try
        {
            var query = "SELECT tax_master_id, tax_percentage FROM tax_master WHERE tax_master_id IS NOT NULL AND tax_percentage IS NOT NULL";
            using var command = new NpgsqlCommand(query, pgConn);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                decimal taxPercentage = reader.GetDecimal(1);
                int taxMasterId = reader.GetInt32(0);
                if (!map.ContainsKey(taxPercentage))
                {
                    map[taxPercentage] = taxMasterId;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading tax master mapping");
        }
        return map;
    }

    private async Task<int> InsertBatchWithTransactionAsync(List<Dictionary<string, object>> batch, NpgsqlConnection pgConn, NpgsqlTransaction? parentTransaction = null)
    {
        if (batch == null || batch.Count == 0)
            return 0;

        int insertedCount = 0;
        try
        {
            using (var importer = pgConn.BeginBinaryImport(@"COPY nfa_lot_charges (
                nfa_lot_charges_id,
                nfa_header_id,
                user_price_bid_lot_charges_id,
                price_bid_charges_id,
                percentage,
                basic_lot_charges_amount,
                charges_name,
                tax_percentage,
                tax_amount,
                tax_master_id,
                total_lot_charges_amount,
                created_by,
                created_date,
                modified_by,
                modified_date,
                is_deleted,
                deleted_by,
                deleted_date
            ) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var record in batch)
                {
                    await importer.StartRowAsync();
                    importer.Write(record["nfa_lot_charges_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["nfa_header_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["user_price_bid_lot_charges_id"] == DBNull.Value ? null : record["user_price_bid_lot_charges_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["price_bid_charges_id"] == DBNull.Value ? null : record["price_bid_charges_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["percentage"] == DBNull.Value ? null : record["percentage"], NpgsqlTypes.NpgsqlDbType.Numeric);
                    importer.Write(record["basic_lot_charges_amount"] == DBNull.Value ? null : record["basic_lot_charges_amount"], NpgsqlTypes.NpgsqlDbType.Numeric);
                    importer.Write(record["charges_name"] == DBNull.Value ? null : record["charges_name"], NpgsqlTypes.NpgsqlDbType.Text);
                    importer.Write(record["tax_percentage"] == DBNull.Value ? null : record["tax_percentage"], NpgsqlTypes.NpgsqlDbType.Numeric);
                    importer.Write(record["tax_amount"] == DBNull.Value ? null : record["tax_amount"], NpgsqlTypes.NpgsqlDbType.Numeric);
                    importer.Write(record["tax_master_id"] == DBNull.Value ? null : record["tax_master_id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["total_lot_charges_amount"] == DBNull.Value ? null : record["total_lot_charges_amount"], NpgsqlTypes.NpgsqlDbType.Numeric);
                    importer.Write(record["created_by"] == DBNull.Value ? null : record["created_by"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["created_date"] == DBNull.Value ? null : record["created_date"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                    importer.Write(record["modified_by"] == DBNull.Value ? null : record["modified_by"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["modified_date"] == DBNull.Value ? null : record["modified_date"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                    importer.Write(record["is_deleted"], NpgsqlTypes.NpgsqlDbType.Boolean);
                    importer.Write(record["deleted_by"] == DBNull.Value ? null : record["deleted_by"], NpgsqlTypes.NpgsqlDbType.Integer);
                    importer.Write(record["deleted_date"] == DBNull.Value ? null : record["deleted_date"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                    insertedCount++;
                }
                await importer.CompleteAsync();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error during bulk COPY of {batch.Count} records.");
            // Don't throw - continue with next batch
        }
        return insertedCount;
    }
}
