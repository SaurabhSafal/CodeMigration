using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class ErpCurrencyExchangeRateMigration
    {
        private readonly ILogger<ErpCurrencyExchangeRateMigration> _logger;
        private readonly IConfiguration _configuration;

        public ErpCurrencyExchangeRateMigration(IConfiguration configuration, ILogger<ErpCurrencyExchangeRateMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "RecId", target = "erp_currency_exchange_rate_id", logic = "RecId -> erp_currency_exchange_rate_id (Direct)", type = "int -> integer (NOT NULL)" },
                new { source = "FromCurrency", target = "from_currency", logic = "FromCurrency -> from_currency (Direct, default 'USD' if NULL)", type = "varchar -> character varying(10) (NOT NULL)" },
                new { source = "ToCurrency", target = "to_currency", logic = "ToCurrency -> to_currency (Direct, default 'INR' if NULL)", type = "varchar -> character varying(10) (NOT NULL)" },
                new { source = "ExchangeRate", target = "exchange_rate", logic = "ExchangeRate -> exchange_rate (Direct, default 1.0 if NULL)", type = "decimal -> numeric (NOT NULL)" },
                new { source = "FromDate", target = "valid_from", logic = "FromDate -> valid_from (Direct, default NOW() if NULL)", type = "datetime -> timestamp with time zone (NOT NULL)" },
                new { source = "N/A (Generated)", target = "company_id", logic = "Auto-inserted for each company in company_master", type = "FK -> integer (NOT NULL)" },
                new { source = "N/A", target = "created_by", logic = "Default: NULL", type = "integer (NULLABLE)" },
                new { source = "N/A", target = "created_date", logic = "Default: CURRENT_TIMESTAMP", type = "timestamp with time zone (NULLABLE)" },
                new { source = "N/A", target = "modified_by", logic = "Default: NULL", type = "integer (NULLABLE)" },
                new { source = "N/A", target = "modified_date", logic = "Default: NULL", type = "timestamp with time zone (NULLABLE)" },
                new { source = "N/A", target = "is_deleted", logic = "Default: false", type = "boolean (NULLABLE)" },
                new { source = "N/A", target = "deleted_by", logic = "Default: NULL", type = "integer (NULLABLE)" },
                new { source = "N/A", target = "deleted_date", logic = "Default: NULL", type = "timestamp with time zone (NULLABLE)" }
            };
        }

        public async Task<int> MigrateAsync()
        {
            var sqlConnectionString = _configuration.GetConnectionString("SqlServer");
            var pgConnectionString = _configuration.GetConnectionString("PostgreSql");

            if (string.IsNullOrEmpty(sqlConnectionString) || string.IsNullOrEmpty(pgConnectionString))
            {
                throw new InvalidOperationException("Database connection strings are not configured properly.");
            }

            var migratedRecords = 0;
            var skippedRecords = 0;
            var errors = new List<string>();

            try
            {
                using var sqlConnection = new SqlConnection(sqlConnectionString);
                using var pgConnection = new NpgsqlConnection(pgConnectionString);

                await sqlConnection.OpenAsync();
                await pgConnection.OpenAsync();

                _logger.LogInformation("Starting ErpCurrencyExchangeRate migration...");

                // Get valid company_ids from PostgreSQL
                var validCompanyIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT company_id FROM company_master WHERE is_deleted = false", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validCompanyIds.Add(reader.GetInt32(0));
                    }
                }

                if (!validCompanyIds.Any())
                {
                    _logger.LogError("No valid company_ids found in company_master table");
                    return 0;
                }

                _logger.LogInformation($"Found {validCompanyIds.Count} companies. Will insert each record for all companies.");

                // Fetch data from SQL Server
                var sourceData = new List<(int RecId, string? FromCurrency, string? ToCurrency, decimal? ExchangeRate, DateTime? FromDate)>();
                
                using (var cmd = new SqlCommand(@"SELECT 
                    RecId,
                    FromCurrency,
                    ToCurrency,
                    ExchangeRate,
                    FromDate
                FROM TBL_CurrencyConversionMaster", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        sourceData.Add((
                            reader.GetInt32(0),
                            reader.IsDBNull(1) ? null : reader.GetString(1),
                            reader.IsDBNull(2) ? null : reader.GetString(2),
                            reader.IsDBNull(3) ? null : reader.GetDecimal(3),
                            reader.IsDBNull(4) ? null : reader.GetDateTime(4)
                        ));
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                if (sourceData.Count == 0)
                {
                    _logger.LogWarning("No data found in TBL_CurrencyConversionMaster");
                    return 0;
                }

                // Generate unique IDs for each company combination
                int recordCounter = 1;
                int processedRecords = 0;

                _logger.LogInformation($"Starting migration: {sourceData.Count} records × {validCompanyIds.Count} companies = {sourceData.Count * validCompanyIds.Count} total inserts");

                foreach (var record in sourceData)
                {
                    processedRecords++;
                    
                    // Log progress every 100 records
                    if (processedRecords % 100 == 0)
                    {
                        _logger.LogInformation($"Progress: {processedRecords}/{sourceData.Count} records processed");
                    }
                    
                    // Handle NULL values with proper defaults (all target columns are NOT NULL)
                    var fromCurrency = GetStringValue(record.FromCurrency, 10);
                    if (string.IsNullOrWhiteSpace(fromCurrency))
                    {
                        fromCurrency = "USD"; // Default currency
                        _logger.LogWarning($"RecId {record.RecId}: FromCurrency is NULL, using default 'USD'");
                    }

                    var toCurrency = GetStringValue(record.ToCurrency, 10);
                    if (string.IsNullOrWhiteSpace(toCurrency))
                    {
                        toCurrency = "INR"; // Default currency
                        _logger.LogWarning($"RecId {record.RecId}: ToCurrency is NULL, using default 'INR'");
                    }

                    var validFrom = GetDateTimeValue(record.FromDate) ?? DateTime.UtcNow;
                    var exchangeRate = record.ExchangeRate ?? 1.0m;

                    if (exchangeRate == 0)
                    {
                        exchangeRate = 1.0m; // Avoid zero exchange rate
                        _logger.LogWarning($"RecId {record.RecId}: ExchangeRate is 0, using default 1.0");
                    }

                    // Insert for each company with unique IDs
                    foreach (var companyId in validCompanyIds)
                    {
                        try
                        {
                            // Generate unique ID for each company combination
                            int uniqueId = recordCounter++;

                            // Check if record already exists
                            int? existingRecordId = null;
                            using (var checkCmd = new NpgsqlCommand(
                                @"SELECT erp_currency_exchange_rate_id 
                                  FROM erp_currency_exchange_rate 
                                  WHERE from_currency = @FromCurrency 
                                    AND to_currency = @ToCurrency 
                                    AND company_id = @CompanyId
                                    AND valid_from = @ValidFrom",
                                pgConnection))
                            {
                                checkCmd.Parameters.AddWithValue("@FromCurrency", fromCurrency);
                                checkCmd.Parameters.AddWithValue("@ToCurrency", toCurrency);
                                checkCmd.Parameters.AddWithValue("@CompanyId", companyId);
                                checkCmd.Parameters.AddWithValue("@ValidFrom", validFrom);
                                var result = await checkCmd.ExecuteScalarAsync();
                                if (result != null && result != DBNull.Value)
                                {
                                    existingRecordId = Convert.ToInt32(result);
                                }
                            }

                            if (existingRecordId.HasValue)
                            {
                                // Update existing record
                                using var updateCmd = new NpgsqlCommand(
                                    @"UPDATE erp_currency_exchange_rate SET
                                        exchange_rate = @ExchangeRate,
                                        modified_by = NULL,
                                        modified_date = CURRENT_TIMESTAMP
                                    WHERE erp_currency_exchange_rate_id = @Id",
                                    pgConnection);

                                updateCmd.Parameters.AddWithValue("@Id", existingRecordId.Value);
                                updateCmd.Parameters.AddWithValue("@ExchangeRate", exchangeRate);

                                await updateCmd.ExecuteNonQueryAsync();
                                _logger.LogDebug($"Updated record ID: {existingRecordId.Value} for company: {companyId}");
                            }
                            else
                            {
                                // Insert new record with unique ID
                                using var insertCmd = new NpgsqlCommand(
                                    @"INSERT INTO erp_currency_exchange_rate (
                                        erp_currency_exchange_rate_id,
                                        from_currency,
                                        to_currency,
                                        valid_from,
                                        exchange_rate,
                                        company_id,
                                        created_by,
                                        created_date,
                                        modified_by,
                                        modified_date,
                                        is_deleted,
                                        deleted_by,
                                        deleted_date
                                    ) VALUES (
                                        @Id,
                                        @FromCurrency,
                                        @ToCurrency,
                                        @ValidFrom,
                                        @ExchangeRate,
                                        @CompanyId,
                                        NULL,
                                        CURRENT_TIMESTAMP,
                                        NULL,
                                        NULL,
                                        false,
                                        NULL,
                                        NULL
                                    )
                                    ON CONFLICT (erp_currency_exchange_rate_id) DO UPDATE SET
                                        exchange_rate = EXCLUDED.exchange_rate,
                                        modified_date = CURRENT_TIMESTAMP",
                                    pgConnection);

                                insertCmd.Parameters.AddWithValue("@Id", uniqueId);
                                insertCmd.Parameters.AddWithValue("@FromCurrency", fromCurrency);
                                insertCmd.Parameters.AddWithValue("@ToCurrency", toCurrency);
                                insertCmd.Parameters.AddWithValue("@ValidFrom", validFrom);
                                insertCmd.Parameters.AddWithValue("@ExchangeRate", exchangeRate);
                                insertCmd.Parameters.AddWithValue("@CompanyId", companyId);

                                int rowsAffected = await insertCmd.ExecuteNonQueryAsync();
                                _logger.LogDebug($"Inserted record ID {uniqueId} for company {companyId}, RecId {record.RecId} (rows: {rowsAffected})");
                            }

                            migratedRecords++;
                        }
                        catch (PostgresException pgEx)
                        {
                            var errorMsg = $"RecId {record.RecId}, Company {companyId}: PostgreSQL Error {pgEx.SqlState} - {pgEx.Message}";
                            _logger.LogWarning(errorMsg);
                            errors.Add(errorMsg);
                            skippedRecords++;
                            continue;
                        }
                        catch (Exception ex)
                        {
                            var errorMsg = $"RecId {record.RecId}, Company {companyId}: {ex.Message}";
                            _logger.LogError(errorMsg);
                            errors.Add(errorMsg);
                            skippedRecords++;
                        }
                    }
                }

                _logger.LogInformation($"Migration completed. Migrated: {migratedRecords}, Skipped: {skippedRecords}");
                
                if (errors.Any())
                {
                    _logger.LogWarning($"Encountered {errors.Count} errors during migration");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Migration failed");
                throw;
            }

            return migratedRecords;
        }

        private string GetStringValue(string? value, int maxLength)
        {
            if (string.IsNullOrWhiteSpace(value))
                return string.Empty;

            value = value.Trim();
            return value.Length > maxLength ? value.Substring(0, maxLength) : value;
        }

        private DateTime? GetDateTimeValue(DateTime? value)
        {
            if (!value.HasValue)
                return null;

            return DateTime.SpecifyKind(value.Value, DateTimeKind.Utc);
        }
    }
}
