using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace DataMigration.Services
{
    public class EventPriceBidColumnsMigration
    {
        private readonly ILogger<EventPriceBidColumnsMigration> _logger;
        private readonly IConfiguration _configuration;

        public EventPriceBidColumnsMigration(IConfiguration configuration, ILogger<EventPriceBidColumnsMigration> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        public List<object> GetMappings()
        {
            return new List<object>
            {
                new { source = "PBID", target = "event_price_bid_columns_id", type = "int -> integer (base ID)" },
                new { source = "EVENTID", target = "event_id", type = "int -> integer (FK to event_master)" },
                new { source = "PRTRANSID", target = "N/A", type = "Lookup only (not stored)" },
                new { source = "HEADER1-10", target = "column_name", type = "nvarchar -> text (if data exists)" },
                new { source = "ExtChargeHeader1-10", target = "column_name", type = "nvarchar -> text (if data exists)" },
                new { source = "N/A", target = "column_type", type = "text (default='Text')" },
                new { source = "N/A", target = "mandatory", type = "boolean (default=true)" },
                new { source = "N/A", target = "sequence_number", type = "integer (default=1 or incremental)" }
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

                _logger.LogInformation("Starting EventPriceBidColumns migration...");

                // Get valid event_ids from PostgreSQL
                var validEventIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_id FROM event_master", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        validEventIds.Add(reader.GetInt32(0));
                    }
                }

                _logger.LogInformation($"Found {validEventIds.Count} valid event_ids");

                // Fetch data from SQL Server - all header columns
                var sourceData = new List<SourceRow>();
                
                using (var cmd = new SqlCommand(@"SELECT 
                    PBID,
                    EVENTID,
                    PRTRANSID,
                    HEADER1, HEADER2, HEADER3, HEADER4, HEADER5,
                    HEADER6, HEADER7, HEADER8, HEADER9, HEADER10,
                    ExtChargeHeader1, ExtChargeHeader2, ExtChargeHeader3, ExtChargeHeader4, ExtChargeHeader5,
                    ExtChargeHeader6, ExtChargeHeader7, ExtChargeHeader8, ExtChargeHeader9, ExtChargeHeader10
                FROM TBL_PB_BUYER", sqlConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var row = new SourceRow
                        {
                            PBID = reader.GetInt32(0),
                            EVENTID = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            PRTRANSID = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                            Headers = new Dictionary<string, string>()
                        };

                        // Read all header columns
                        for (int i = 1; i <= 10; i++)
                        {
                            var headerValue = reader.IsDBNull(2 + i) ? null : reader.GetString(2 + i);
                            if (!string.IsNullOrWhiteSpace(headerValue))
                            {
                                row.Headers[$"HEADER{i}"] = headerValue;
                            }
                        }

                        // Read all ExtChargeHeader columns
                        for (int i = 1; i <= 10; i++)
                        {
                            var headerValue = reader.IsDBNull(12 + i) ? null : reader.GetString(12 + i);
                            if (!string.IsNullOrWhiteSpace(headerValue))
                            {
                                row.Headers[$"ExtChargeHeader{i}"] = headerValue;
                            }
                        }

                        sourceData.Add(row);
                    }
                }

                _logger.LogInformation($"Found {sourceData.Count} records in source table");

                // Load existing IDs into memory for fast lookup
                var existingIds = new HashSet<int>();
                using (var cmd = new NpgsqlCommand("SELECT event_price_bid_columns_id FROM event_price_bid_columns", pgConnection))
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        existingIds.Add(reader.GetInt32(0));
                    }
                }
                _logger.LogInformation($"Found {existingIds.Count} existing records in target table");

                int totalRowsGenerated = 0;
                const int batchSize = 500;
                var insertBatch = new List<TargetRow>();
                var updateBatch = new List<TargetRow>();

                foreach (var record in sourceData)
                {
                    try
                    {
                        // Validate event_id exists (FK constraint)
                        if (!record.EVENTID.HasValue || !validEventIds.Contains(record.EVENTID.Value))
                        {
                            _logger.LogDebug($"PBID {record.PBID}: event_id {record.EVENTID} not found in event_master (FK constraint violation)");
                            skippedRecords++;
                            continue;
                        }

                        // If no headers have data, skip this record
                        if (!record.Headers.Any())
                        {
                            _logger.LogDebug($"PBID {record.PBID}: No header data found, skipping");
                            skippedRecords++;
                            continue;
                        }

                        // Generate a row for each header that has data
                        // Sort headers numerically: HEADER1, HEADER2, ..., HEADER10, ExtChargeHeader1, ...
                        var sortedHeaders = record.Headers
                            .OrderBy(h => h.Key.StartsWith("ExtChargeHeader") ? 1 : 0) // HEADER first, then ExtChargeHeader
                            .ThenBy(h => {
                                // Extract numeric part for proper numeric sorting
                                var key = h.Key;
                                var numStr = new string(key.Where(char.IsDigit).ToArray());
                                return int.TryParse(numStr, out int num) ? num : 0;
                            });

                        int sequenceNumber = 1;
                        foreach (var header in sortedHeaders)
                        {
                            // Generate unique ID: PBID * 1000 + sequenceNumber
                            var eventPriceBidColumnsId = (record.PBID * 1000) + sequenceNumber;

                            var targetRow = new TargetRow
                            {
                                Id = eventPriceBidColumnsId,
                                EventId = record.EVENTID.Value,
                                ColumnName = header.Value,
                                SequenceNumber = sequenceNumber
                            };

                            if (existingIds.Contains(eventPriceBidColumnsId))
                            {
                                updateBatch.Add(targetRow);
                            }
                            else
                            {
                                insertBatch.Add(targetRow);
                            }

                            totalRowsGenerated++;
                            sequenceNumber++;
                        }

                        migratedRecords++;

                        // Execute batch when it reaches the size limit
                        if (insertBatch.Count >= batchSize)
                        {
                            await ExecuteInsertBatch(pgConnection, insertBatch, _logger);
                            insertBatch.Clear();
                        }

                        if (updateBatch.Count >= batchSize)
                        {
                            await ExecuteUpdateBatch(pgConnection, updateBatch, _logger);
                            updateBatch.Clear();
                        }
                    }
                    catch (Exception ex)
                    {
                        var errorMsg = $"PBID {record.PBID}: {ex.Message}";
                        _logger.LogError(errorMsg);
                        errors.Add(errorMsg);
                        skippedRecords++;
                    }
                }

                // Execute remaining batches
                if (insertBatch.Any())
                {
                    await ExecuteInsertBatch(pgConnection, insertBatch, _logger);
                }

                if (updateBatch.Any())
                {
                    await ExecuteUpdateBatch(pgConnection, updateBatch, _logger);
                }

                _logger.LogInformation($"Migration completed. Source Records Processed: {migratedRecords}, Skipped: {skippedRecords}, Total Rows Generated: {totalRowsGenerated}");
                
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

        private static async Task ExecuteInsertBatch(NpgsqlConnection connection, List<TargetRow> batch, ILogger logger)
        {
            if (!batch.Any()) return;

            var sql = new System.Text.StringBuilder();
            sql.AppendLine("INSERT INTO event_price_bid_columns (");
            sql.AppendLine("    event_price_bid_columns_id, event_id, column_name, column_type,");
            sql.AppendLine("    mandatory, sequence_number, created_by, created_date,");
            sql.AppendLine("    modified_by, modified_date, is_deleted, deleted_by, deleted_date");
            sql.AppendLine(") VALUES");

            var values = new List<string>();
            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@Id{i}, @EventId{i}, @ColumnName{i}, 'Text', true, @SequenceNumber{i}, NULL, CURRENT_TIMESTAMP, NULL, NULL, false, NULL, NULL)");
                
                cmd.Parameters.AddWithValue($"@Id{i}", row.Id);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@ColumnName{i}", row.ColumnName);
                cmd.Parameters.AddWithValue($"@SequenceNumber{i}", row.SequenceNumber);
            }

            sql.AppendLine(string.Join(",\n", values));
            cmd.CommandText = sql.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                logger.LogDebug($"Batch inserted {rowsAffected} records");
            }
            catch (Exception ex)
            {
                logger.LogError($"Batch insert failed: {ex.Message}");
                throw;
            }
        }

        private static async Task ExecuteUpdateBatch(NpgsqlConnection connection, List<TargetRow> batch, ILogger logger)
        {
            if (!batch.Any()) return;

            using var cmd = new NpgsqlCommand();
            cmd.Connection = connection;

            var cases = new System.Text.StringBuilder();
            cases.AppendLine("UPDATE event_price_bid_columns SET");
            cases.AppendLine("    event_id = data.event_id,");
            cases.AppendLine("    column_name = data.column_name,");
            cases.AppendLine("    column_type = data.column_type,");
            cases.AppendLine("    mandatory = data.mandatory,");
            cases.AppendLine("    sequence_number = data.sequence_number,");
            cases.AppendLine("    modified_by = NULL,");
            cases.AppendLine("    modified_date = CURRENT_TIMESTAMP");
            cases.AppendLine("FROM (VALUES");

            var values = new List<string>();
            for (int i = 0; i < batch.Count; i++)
            {
                var row = batch[i];
                values.Add($"(@Id{i}::integer, @EventId{i}::integer, @ColumnName{i}::text, 'Text'::text, true::boolean, @SequenceNumber{i}::integer)");
                
                cmd.Parameters.AddWithValue($"@Id{i}", row.Id);
                cmd.Parameters.AddWithValue($"@EventId{i}", row.EventId);
                cmd.Parameters.AddWithValue($"@ColumnName{i}", row.ColumnName);
                cmd.Parameters.AddWithValue($"@SequenceNumber{i}", row.SequenceNumber);
            }

            cases.AppendLine(string.Join(",\n", values));
            cases.AppendLine(") AS data(id, event_id, column_name, column_type, mandatory, sequence_number)");
            cases.AppendLine("WHERE event_price_bid_columns.event_price_bid_columns_id = data.id");
            
            cmd.CommandText = cases.ToString();

            try
            {
                var rowsAffected = await cmd.ExecuteNonQueryAsync();
                logger.LogDebug($"Batch updated {rowsAffected} records");
            }
            catch (Exception ex)
            {
                logger.LogError($"Batch update failed: {ex.Message}");
                throw;
            }
        }

        private class TargetRow
        {
            public int Id { get; set; }
            public int EventId { get; set; }
            public string ColumnName { get; set; } = string.Empty;
            public int SequenceNumber { get; set; }
        }

        private class SourceRow
        {
            public int PBID { get; set; }
            public int? EVENTID { get; set; }
            public int? PRTRANSID { get; set; }
            public Dictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
        }
    }
}
