using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

public class EventMasterMigration : MigrationService
{
    private readonly ILogger<EventMasterMigration> _logger;

    public EventMasterMigration(IConfiguration configuration, ILogger<EventMasterMigration> logger) : base(configuration)
    {
        _logger = logger;
    }

    protected override string SelectQuery => @"
        SELECT 
            EVENTID, EVENTCODE, EVENTNAME, EVENTDESC, ROUND, EVENTTYPE, CURRENTSTATUS, 
            PARENTID, PRICINGSTATUS, ISEXTEND, EventCurrencyId, IschkIsSendMail, ClientSAPId,
            TechnicalApprovalSendDate, TechnicalApprovalApprovedDate, TechnicalApprovalStatus,
            EventMode, TiePreventLot, TiePreventItem, IsTargetPriceApplicable, 
            IsAutoExtendedEnable, NoofTimesAutoExtended, AutoExtendedMinutes, ApplyExtendedTimes,
            GREENPERCENTAGE, YELLOWPERCENTAGE, IsItemLevelRankShow, IsLotLevelRankShow,
            IsLotLevelAuction, IsBasicPriceApplicable, IsBasicPriceValidationReq, 
            IsMinMaxBidApplicable, IsLowestBidShow, BesideAuctionFirstBid, MinBid, MaxBid,
            LotLevelBasicPrice, IsPriceBidAttachmentcompulsory, IsDiscountApplicable,
            IsGSTCompulsory, IsTechnicalAttachmentcompulsory, IsProposedQty, IsRedyStockmandatory,
            MinBidMode, MaxBidMode
        FROM TBL_EVENTMASTER
        ORDER BY EVENTID";

    protected override string InsertQuery => @"
        INSERT INTO event_master (
            event_id, event_code, event_name, event_description, round, event_type, 
            event_status, parent_id, price_bid_template, is_standalone, pricing_status, 
            event_extended, event_currency_id, disable_mail_in_next_round, company_id,
            technical_approval_send_date, technical_approval_approved_date, 
            technical_approval_status, created_by, created_date
        ) VALUES (
            @event_id, @event_code, @event_name, @event_description, @round, @event_type, 
            @event_status, @parent_id, @price_bid_template, @is_standalone, @pricing_status, 
            @event_extended, @event_currency_id, @disable_mail_in_next_round, @company_id,
            @technical_approval_send_date, @technical_approval_approved_date, 
            @technical_approval_status, @created_by, @created_date
        ) RETURNING event_id";

    private string InsertEventSettingQuery => @"
        INSERT INTO event_setting (
            event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
            auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
            apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
            show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
            min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
            min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
            lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
            gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
            created_by, created_date, lot_level_target_price, max_lot_bid_type, 
            min_lot_bid_type, allow_currency_selection
        ) VALUES (
            @event_id, @event_mode, @tie_prevent_lot, @tie_prevent_item, @target_price_applicable,
            @auto_extended_enable, @no_of_times_auto_extended, @auto_extended_minutes,
            @apply_extended_times, @green_percentage, @yellow_percentage, @show_item_level_rank,
            @show_lot_level_rank, @basic_price_applicable, @basic_price_validation_mandatory,
            @min_max_bid_applicable, @show_lower_bid, @apply_all_settings_in_price_bid,
            @min_lot_auction_bid_value, @max_lot_auction_bid_value, @configure_lot_level_auction,
            @lot_level_basic_price, @price_bid_attachment_mandatory, @discount_applicable,
            @gst_mandatory, @technical_attachment_mandatory, @proposed_qty, @ready_stock_mandatory,
            @created_by, @created_date, @lot_level_target_price, @max_lot_bid_type,
            @min_lot_bid_type, @allow_currency_selection
        )";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "EVENTID -> event_id (Direct)",
            "EVENTCODE -> event_code (Direct)",
            "EVENTNAME -> event_name (Direct)",
            "EVENTDESC -> event_description (Direct)",
            "ROUND -> round (Direct)",
            "EVENTTYPE -> event_type (Transform: 1=RFQ, 2=Forward Auction, 3=Reverse Auction)",
            "CURRENTSTATUS -> event_status (Direct)",
            "PARENTID -> parent_id (Direct, 0 if NULL)",
            "price_bid_template -> TBL_PB_BUYER.PBType (Lookup: 1=material, 14=service)",
            "is_standalone -> 0 (Fixed)",
            "PRICINGSTATUS -> pricing_status (Direct)",
            "ISEXTEND -> event_extended (Direct)",
            "EventCurrencyId -> event_currency_id (Direct)",
            "IschkIsSendMail -> disable_mail_in_next_round (Direct)",
            "ClientSAPId -> company_id (Direct)",
            "TechnicalApprovalSendDate -> technical_approval_send_date (Direct)",
            "TechnicalApprovalApprovedDate -> technical_approval_approved_date (Direct)",
            "TechnicalApprovalStatus -> technical_approval_status (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "--- Event Setting Table ---",
            "event_id -> event_id (From event_master)",
            "EventMode -> event_mode (Direct)",
            "TiePreventLot -> tie_prevent_lot (Direct)",
            "TiePreventItem -> tie_prevent_item (Direct)",
            "IsTargetPriceApplicable -> target_price_applicable (Direct)",
            "IsAutoExtendedEnable -> auto_extended_enable (Direct)",
            "NoofTimesAutoExtended -> no_of_times_auto_extended (Direct)",
            "AutoExtendedMinutes -> auto_extended_minutes (Direct)",
            "ApplyExtendedTimes -> apply_extended_times (Direct)",
            "GREENPERCENTAGE -> green_percentage (Direct)",
            "YELLOWPERCENTAGE -> yellow_percentage (Direct)",
            "IsItemLevelRankShow -> show_item_level_rank (Direct)",
            "IsLotLevelRankShow -> show_lot_level_rank (Direct)",
            "basic_price_applicable -> IF(IsLotLevelAuction==1) THEN IsLotLevelAuction ELSE IsBasicPriceApplicable (Conditional)",
            "IsBasicPriceValidationReq -> basic_price_validation_mandatory (Direct)",
            "IsMinMaxBidApplicable -> min_max_bid_applicable (Direct)",
            "IsLowestBidShow -> show_lower_bid (Direct)",
            "BesideAuctionFirstBid -> apply_all_settings_in_price_bid (Direct)",
            "MinBid -> min_lot_auction_bid_value (Direct)",
            "MaxBid -> max_lot_auction_bid_value (Direct)",
            "IsLotLevelAuction -> configure_lot_level_auction (Direct)",
            "LotLevelBasicPrice -> lot_level_basic_price (Direct)",
            "IsPriceBidAttachmentcompulsory -> price_bid_attachment_mandatory (Direct)",
            "IsDiscountApplicable -> discount_applicable (Direct)",
            "IsGSTCompulsory -> gst_mandatory (Direct)",
            "IsTechnicalAttachmentcompulsory -> technical_attachment_mandatory (Direct)",
            "IsProposedQty -> proposed_qty (Direct)",
            "IsRedyStockmandatory -> ready_stock_mandatory (Direct)",
            "lot_level_target_price -> 0 (Fixed)",
            "MinBidMode -> max_lot_bid_type (Direct)",
            "MaxBidMode -> min_lot_bid_type (Direct)",
            "allow_currency_selection -> 0 (Fixed)"
        };
    }

    public async Task<(int SuccessCount, int FailedCount, List<string> Errors)> MigrateAsync()
    {
        int successCount = 0;
        int failedCount = 0;
        var errors = new List<string>();
        int totalRecords = 0;

        try
        {
            _logger.LogInformation("Starting EventMaster migration...");

            using var sqlConnection = GetSqlServerConnection();
            using var pgConnection = GetPostgreSqlConnection();

            await sqlConnection.OpenAsync();
            await pgConnection.OpenAsync();

            _logger.LogInformation("Database connections established successfully");

            // First, get total count for progress tracking
            using (var countCmd = new SqlCommand("SELECT COUNT(*) FROM TBL_EVENTMASTER", sqlConnection))
            {
                var scalarResult = await countCmd.ExecuteScalarAsync();
                totalRecords = scalarResult != null && scalarResult != DBNull.Value ? Convert.ToInt32(scalarResult) : 0;
                _logger.LogInformation($"Total records to migrate: {totalRecords}");
            }

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConnection);
            using var reader = await sqlCommand.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var eventId = reader["EVENTID"];
                var eventCode = reader["EVENTCODE"]?.ToString() ?? "NULL";

                try
                {
                    using var transaction = await pgConnection.BeginTransactionAsync();

                    try
                    {
                        _logger.LogDebug($"Processing Event ID: {eventId}, Code: {eventCode}");

                        // Check if record already exists
                        bool recordExists = false;
                        using (var checkCmd = new NpgsqlCommand("SELECT COUNT(*) FROM event_master WHERE event_id = @event_id", pgConnection, transaction))
                        {
                            checkCmd.Parameters.AddWithValue("@event_id", eventId);
                            var scalarResult = await checkCmd.ExecuteScalarAsync();
                            var count = scalarResult != null && scalarResult != DBNull.Value ? Convert.ToInt64(scalarResult) : 0;
                            recordExists = count > 0;
                        }

                        if (recordExists)
                        {
                            _logger.LogWarning($"Event ID {eventId} already exists, skipping...");
                            await transaction.RollbackAsync();
                            continue;
                        }

                        // Insert into event_master and get the generated event_id
                        int newEventId;
                        using (var insertCmd = new NpgsqlCommand(InsertQuery, pgConnection, transaction))
                        {
                            try
                            {
                                // Event Master fields with proper null handling
                                insertCmd.Parameters.AddWithValue("@event_id", eventId ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@event_code", reader["EVENTCODE"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@event_name", reader["EVENTNAME"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@event_description", reader["EVENTDESC"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@round", reader["ROUND"] != DBNull.Value ? reader["ROUND"] : 0);

                                // Convert EVENTTYPE integer to string with validation
                                string eventType = "";
                                if (reader["EVENTTYPE"] != DBNull.Value)
                                {
                                    if (int.TryParse(reader["EVENTTYPE"].ToString(), out int eventTypeValue))
                                    {
                                        eventType = eventTypeValue switch
                                        {
                                            1 => "RFQ",
                                            2 => "Forward Auction",
                                            3 => "Reverse Auction",
                                            _ => $"Unknown_{eventTypeValue}"
                                        };
                                    }
                                    else
                                    {
                                        eventType = reader["EVENTTYPE"]?.ToString() ?? string.Empty;
                                    }
                                }
                                insertCmd.Parameters.AddWithValue("@event_type", string.IsNullOrEmpty(eventType) ? DBNull.Value : eventType);

                                insertCmd.Parameters.AddWithValue("@event_status", reader["CURRENTSTATUS"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@parent_id", reader["PARENTID"] != DBNull.Value ? reader["PARENTID"] : 0);

                                // Lookup price_bid_template from TBL_PB_BUYER with improved error handling
                                string priceBidTemplate = string.Empty;
                                try
                                {
                                    using (var pbConnection = GetSqlServerConnection())
                                    {
                                        await pbConnection.OpenAsync();
                                        using (var pbCmd = new SqlCommand("SELECT TOP 1 PBType FROM TBL_PB_BUYER WHERE SEQUENCEID = 0 AND EVENTID = @eventId", pbConnection))
                                        {
                                            pbCmd.Parameters.AddWithValue("@eventId", eventId);
                                            var pbResult = await pbCmd.ExecuteScalarAsync();
                                            if (pbResult != null && pbResult != DBNull.Value)
                                            {
                                                if (int.TryParse(pbResult.ToString(), out int pbType))
                                                {
                                                    priceBidTemplate = pbType switch
                                                    {
                                                        1 => "Material",
                                                        14 => "Service",
                                                        _ => $"Type_{pbType}"
                                                    };
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception pbEx)
                                {
                                    _logger.LogWarning($"Failed to lookup price_bid_template for Event ID {eventId}: {pbEx.Message}");
                                }
                                
                                insertCmd.Parameters.AddWithValue("@price_bid_template", string.IsNullOrEmpty(priceBidTemplate) ? DBNull.Value : (object)priceBidTemplate);

                                // Safe boolean conversions
                                insertCmd.Parameters.AddWithValue("@is_standalone", false);
                                insertCmd.Parameters.AddWithValue("@pricing_status", SafeConvertToBoolean(reader["PRICINGSTATUS"]));
                                insertCmd.Parameters.AddWithValue("@event_extended", SafeConvertToBoolean(reader["ISEXTEND"]));
                                insertCmd.Parameters.AddWithValue("@event_currency_id", SafeConvertToInt(reader["EventCurrencyId"]));
                                insertCmd.Parameters.AddWithValue("@disable_mail_in_next_round", SafeConvertToBoolean(reader["IschkIsSendMail"]));
                                insertCmd.Parameters.AddWithValue("@company_id", SafeConvertToInt(reader["ClientSAPId"]));
                                insertCmd.Parameters.AddWithValue("@technical_approval_send_date", reader["TechnicalApprovalSendDate"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@technical_approval_approved_date", reader["TechnicalApprovalApprovedDate"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@technical_approval_status", reader["TechnicalApprovalStatus"] ?? DBNull.Value);
                                insertCmd.Parameters.AddWithValue("@created_by", 0);
                                insertCmd.Parameters.AddWithValue("@created_date", DateTime.Now);

                                var result = await insertCmd.ExecuteScalarAsync();
                                newEventId = Convert.ToInt32(result);

                                _logger.LogDebug($"Successfully inserted event_master record for Event ID: {eventId}");
                            }
                            catch (Exception insertEx)
                            {
                                var error = $"Failed to insert event_master for Event ID {eventId}: {insertEx.Message}";
                                _logger.LogError(error);
                                errors.Add(error);
                                throw;
                            }
                        }

                        // Insert into event_setting with improved error handling
                        using (var settingCmd = new NpgsqlCommand(InsertEventSettingQuery, pgConnection, transaction))
                        {
                            try
                            {
                                settingCmd.Parameters.AddWithValue("@event_id", newEventId);
                                settingCmd.Parameters.AddWithValue("@event_mode", reader["EventMode"] ?? DBNull.Value);
                                settingCmd.Parameters.AddWithValue("@tie_prevent_lot", SafeConvertToBoolean(reader["TiePreventLot"]));
                                settingCmd.Parameters.AddWithValue("@tie_prevent_item", SafeConvertToBoolean(reader["TiePreventItem"]));
                                settingCmd.Parameters.AddWithValue("@target_price_applicable", SafeConvertToBoolean(reader["IsTargetPriceApplicable"]));
                                settingCmd.Parameters.AddWithValue("@auto_extended_enable", SafeConvertToBoolean(reader["IsAutoExtendedEnable"]));
                                settingCmd.Parameters.AddWithValue("@no_of_times_auto_extended", SafeConvertToInt(reader["NoofTimesAutoExtended"]));
                                settingCmd.Parameters.AddWithValue("@auto_extended_minutes", SafeConvertToInt(reader["AutoExtendedMinutes"]));
                                settingCmd.Parameters.AddWithValue("@apply_extended_times", SafeConvertToBoolean(reader["ApplyExtendedTimes"]));
                                settingCmd.Parameters.AddWithValue("@green_percentage", SafeConvertToDecimal(reader["GREENPERCENTAGE"]));
                                settingCmd.Parameters.AddWithValue("@yellow_percentage", SafeConvertToDecimal(reader["YELLOWPERCENTAGE"]));
                                settingCmd.Parameters.AddWithValue("@show_item_level_rank", SafeConvertToBoolean(reader["IsItemLevelRankShow"]));
                                settingCmd.Parameters.AddWithValue("@show_lot_level_rank", SafeConvertToBoolean(reader["IsLotLevelRankShow"]));

                                // Conditional logic for basic_price_applicable
                                var isLotLevelAuction = SafeConvertToBoolean(reader["IsLotLevelAuction"]);
                                if (isLotLevelAuction)
                                {
                                    settingCmd.Parameters.AddWithValue("@basic_price_applicable", isLotLevelAuction);
                                }
                                else
                                {
                                    settingCmd.Parameters.AddWithValue("@basic_price_applicable", SafeConvertToBoolean(reader["IsBasicPriceApplicable"]));
                                }

                                settingCmd.Parameters.AddWithValue("@basic_price_validation_mandatory", SafeConvertToBoolean(reader["IsBasicPriceValidationReq"]));
                                settingCmd.Parameters.AddWithValue("@min_max_bid_applicable", SafeConvertToBoolean(reader["IsMinMaxBidApplicable"]));
                                settingCmd.Parameters.AddWithValue("@show_lower_bid", SafeConvertToBoolean(reader["IsLowestBidShow"]));
                                settingCmd.Parameters.AddWithValue("@apply_all_settings_in_price_bid", SafeConvertToBoolean(reader["BesideAuctionFirstBid"]));
                                settingCmd.Parameters.AddWithValue("@min_lot_auction_bid_value", SafeConvertToDecimal(reader["MinBid"]));
                                settingCmd.Parameters.AddWithValue("@max_lot_auction_bid_value", SafeConvertToDecimal(reader["MaxBid"]));
                                settingCmd.Parameters.AddWithValue("@configure_lot_level_auction", SafeConvertToBoolean(reader["IsLotLevelAuction"]));
                                settingCmd.Parameters.AddWithValue("@lot_level_basic_price", SafeConvertToDecimal(reader["LotLevelBasicPrice"]));
                                settingCmd.Parameters.AddWithValue("@price_bid_attachment_mandatory", SafeConvertToBoolean(reader["IsPriceBidAttachmentcompulsory"]));
                                settingCmd.Parameters.AddWithValue("@discount_applicable", SafeConvertToBoolean(reader["IsDiscountApplicable"]));
                                settingCmd.Parameters.AddWithValue("@gst_mandatory", SafeConvertToBoolean(reader["IsGSTCompulsory"]));
                                settingCmd.Parameters.AddWithValue("@technical_attachment_mandatory", SafeConvertToBoolean(reader["IsTechnicalAttachmentcompulsory"]));
                                settingCmd.Parameters.AddWithValue("@proposed_qty", SafeConvertToBoolean(reader["IsProposedQty"]));
                                settingCmd.Parameters.AddWithValue("@ready_stock_mandatory", SafeConvertToBoolean(reader["IsRedyStockmandatory"]));
                                settingCmd.Parameters.AddWithValue("@created_by", 0);
                                settingCmd.Parameters.AddWithValue("@created_date", DateTime.Now);
                                settingCmd.Parameters.AddWithValue("@lot_level_target_price", 0);
                                settingCmd.Parameters.AddWithValue("@max_lot_bid_type", reader["MinBidMode"] ?? DBNull.Value);
                                settingCmd.Parameters.AddWithValue("@min_lot_bid_type", reader["MaxBidMode"] ?? DBNull.Value);
                                settingCmd.Parameters.AddWithValue("@allow_currency_selection", false);

                                await settingCmd.ExecuteNonQueryAsync();

                                _logger.LogDebug($"Successfully inserted event_setting record for Event ID: {eventId}");
                            }
                            catch (Exception settingEx)
                            {
                                var error = $"Failed to insert event_setting for Event ID {eventId}: {settingEx.Message}";
                                _logger.LogError(error);
                                errors.Add(error);
                                throw;
                            }
                        }

                        await transaction.CommitAsync();
                        successCount++;

                        if (successCount % 100 == 0)
                        {
                            _logger.LogInformation($"Migrated {successCount}/{totalRecords} records successfully");
                        }
                    }
                    catch (Exception transactionEx)
                    {
                        var error = $"Transaction failed for Event ID {eventId}: {transactionEx.Message}";
                        _logger.LogError(error);
                        errors.Add(error);
                        failedCount++;
                        
                        try
                        {
                            await transaction.RollbackAsync();
                        }
                        catch (Exception rollbackEx)
                        {
                            _logger.LogError($"Failed to rollback transaction for Event ID {eventId}: {rollbackEx.Message}");
                        }
                    }
                }
                catch (Exception recordEx)
                {
                    var error = $"Failed to process Event ID {eventId}: {recordEx.Message}";
                    _logger.LogError(error);
                    errors.Add(error);
                    failedCount++;
                }
            }
        }
        catch (Exception ex)
        {
            var error = $"Migration failed with exception: {ex.Message}";
            _logger.LogError(error);
            errors.Add(error);
        }

        _logger.LogInformation($"EventMaster migration completed. Success: {successCount}, Failed: {failedCount}, Total: {totalRecords}");
        
        if (errors.Any())
        {
            _logger.LogWarning($"Migration completed with {errors.Count} errors:");
            foreach (var error in errors.Take(10)) // Log first 10 errors
            {
                _logger.LogWarning($"  - {error}");
            }
        }

        return (successCount, failedCount, errors);
    }

    private bool SafeConvertToBoolean(object value)
    {
        if (value == null || value == DBNull.Value) return false;
        
        if (value is bool boolValue) return boolValue;
        
        if (bool.TryParse(value.ToString(), out bool result)) return result;
        
        if (int.TryParse(value.ToString(), out int intValue)) return intValue != 0;
        
        return false;
    }

    private int SafeConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value) return 0;
        
        if (value is int intValue) return intValue;
        
        if (int.TryParse(value.ToString(), out int result)) return result;
        
        return 0;
    }

    private decimal SafeConvertToDecimal(object value)
    {
        if (value == null || value == DBNull.Value) return 0;
        
        if (value is decimal decimalValue) return decimalValue;
        
        if (decimal.TryParse(value.ToString(), out decimal result)) return result;
        
        return 0;
    }
}
