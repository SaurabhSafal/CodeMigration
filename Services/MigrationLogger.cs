using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace DataMigration.Services
{
    /// <summary>
    /// Represents a single log entry during migration
    /// </summary>
    public class MigrationLogEntry
    {
        public DateTime Timestamp { get; set; }
        public MigrationLogLevel Level { get; set; }
        public string Message { get; set; } = "";
        public string RecordIdentifier { get; set; } = ""; // e.g., "ID=123", "PBID=456"
        public string Category { get; set; } = ""; // e.g., "Validation", "FK Constraint", "Data Error"
        public Dictionary<string, object> Details { get; set; } = new Dictionary<string, object>();
    }

    public enum MigrationLogLevel
    {
        Info,
        Warning,
        Error,
        Debug
    }

    /// <summary>
    /// Centralized logging service for all migration operations
    /// Captures all logs including skipped records with details
    /// </summary>
    public class MigrationLogger
    {
        private readonly ILogger _logger;
        private readonly ConcurrentBag<MigrationLogEntry> _logs = new ConcurrentBag<MigrationLogEntry>();
        private readonly string _tableName;
        private int _skippedCount = 0;
        private int _insertedCount = 0;
        private int _processedCount = 0;
        private int _errorCount = 0;

        public MigrationLogger(ILogger logger, string tableName)
        {
            _logger = logger;
            _tableName = tableName;
        }

        public int SkippedCount => _skippedCount;
        public int InsertedCount => _insertedCount;
        public int ProcessedCount => _processedCount;
        public int ErrorCount => _errorCount;

        /// <summary>
        /// Log a record that was successfully inserted
        /// </summary>
        public void LogInserted(string? recordIdentifier = null)
        {
            System.Threading.Interlocked.Increment(ref _insertedCount);
            System.Threading.Interlocked.Increment(ref _processedCount);
            
            if (!string.IsNullOrEmpty(recordIdentifier))
            {
                var entry = new MigrationLogEntry
                {
                    Timestamp = DateTime.Now,
                    Level = MigrationLogLevel.Debug,
                    Message = $"Record inserted successfully",
                    RecordIdentifier = recordIdentifier,
                    Category = "Success"
                };
                _logs.Add(entry);
                _logger.LogDebug($"[{_tableName}] {recordIdentifier}: Inserted successfully");
            }
        }

        /// <summary>
        /// Log a record that was skipped with reason
        /// </summary>
        public void LogSkipped(string reason, string? recordIdentifier = null, Dictionary<string, object>? details = null)
        {
            System.Threading.Interlocked.Increment(ref _skippedCount);
            System.Threading.Interlocked.Increment(ref _processedCount);
            
            var entry = new MigrationLogEntry
            {
                Timestamp = DateTime.Now,
                Level = MigrationLogLevel.Warning,
                Message = reason,
                RecordIdentifier = recordIdentifier ?? "Unknown",
                Category = "Skipped",
                Details = details ?? new Dictionary<string, object>()
            };
            _logs.Add(entry);
            
            var detailsStr = details != null && details.Any() 
                ? " [" + string.Join(", ", details.Select(kv => $"{kv.Key}={kv.Value}")) + "]"
                : "";
            _logger.LogWarning($"[{_tableName}] {recordIdentifier}: Skipped - {reason}{detailsStr}");
        }

        /// <summary>
        /// Log an error during migration
        /// </summary>
        public void LogError(string error, string? recordIdentifier = null, Exception? ex = null, Dictionary<string, object>? details = null)
        {
            System.Threading.Interlocked.Increment(ref _errorCount);
            
            var entry = new MigrationLogEntry
            {
                Timestamp = DateTime.Now,
                Level = MigrationLogLevel.Error,
                Message = error,
                RecordIdentifier = recordIdentifier ?? "Unknown",
                Category = "Error",
                Details = details ?? new Dictionary<string, object>()
            };
            
            if (ex != null)
            {
                entry.Details["Exception"] = ex.Message;
                if (ex.StackTrace != null)
                {
                    entry.Details["StackTrace"] = ex.StackTrace;
                }
            }
            
            _logs.Add(entry);
            
            var detailsStr = details != null && details.Any() 
                ? " [" + string.Join(", ", details.Select(kv => $"{kv.Key}={kv.Value}")) + "]"
                : "";
            
            if (ex != null)
            {
                _logger.LogError(ex, $"[{_tableName}] {recordIdentifier}: {error}{detailsStr}");
            }
            else
            {
                _logger.LogError($"[{_tableName}] {recordIdentifier}: {error}{detailsStr}");
            }
        }

        /// <summary>
        /// Log informational message
        /// </summary>
        public void LogInfo(string message, string? recordIdentifier = null, Dictionary<string, object>? details = null)
        {
            var entry = new MigrationLogEntry
            {
                Timestamp = DateTime.Now,
                Level = MigrationLogLevel.Info,
                Message = message,
                RecordIdentifier = recordIdentifier ?? "",
                Category = "Info",
                Details = details ?? new Dictionary<string, object>()
            };
            _logs.Add(entry);
            
            _logger.LogInformation($"[{_tableName}] {message}");
        }

        /// <summary>
        /// Log debug message
        /// </summary>
        public void LogDebug(string message, string? recordIdentifier = null, Dictionary<string, object>? details = null)
        {
            var entry = new MigrationLogEntry
            {
                Timestamp = DateTime.Now,
                Level = MigrationLogLevel.Debug,
                Message = message,
                RecordIdentifier = recordIdentifier ?? "",
                Category = "Debug",
                Details = details ?? new Dictionary<string, object>()
            };
            _logs.Add(entry);
            
            _logger.LogDebug($"[{_tableName}] {message}");
        }

        /// <summary>
        /// Get all logs
        /// </summary>
        public List<MigrationLogEntry> GetAllLogs()
        {
            return _logs.OrderBy(l => l.Timestamp).ToList();
        }

        /// <summary>
        /// Get logs filtered by level
        /// </summary>
        public List<MigrationLogEntry> GetLogsByLevel(MigrationLogLevel level)
        {
            return _logs.Where(l => l.Level == level).OrderBy(l => l.Timestamp).ToList();
        }

        /// <summary>
        /// Get all skipped records with details
        /// </summary>
        public List<MigrationLogEntry> GetSkippedRecords()
        {
            return _logs.Where(l => l.Category == "Skipped").OrderBy(l => l.Timestamp).ToList();
        }

        /// <summary>
        /// Get all errors with details
        /// </summary>
        public List<MigrationLogEntry> GetErrors()
        {
            return _logs.Where(l => l.Level == MigrationLogLevel.Error).OrderBy(l => l.Timestamp).ToList();
        }

        /// <summary>
        /// Get summary of migration
        /// </summary>
        public MigrationSummary GetSummary()
        {
            return new MigrationSummary
            {
                TableName = _tableName,
                TotalProcessed = _processedCount,
                TotalInserted = _insertedCount,
                TotalSkipped = _skippedCount,
                TotalErrors = _errorCount,
                Logs = GetAllLogs()
            };
        }

        /// <summary>
        /// Get grouped logs (consolidates repeated similar logs into ranges)
        /// </summary>
        public List<GroupedLogEntry> GetGroupedLogs(bool includeDebug = false)
        {
            var logs = includeDebug 
                ? GetAllLogs() 
                : _logs.Where(l => l.Level != MigrationLogLevel.Debug).OrderBy(l => l.Timestamp).ToList();

            var groupedLogs = new List<GroupedLogEntry>();
            
            if (!logs.Any())
                return groupedLogs;

            MigrationLogEntry? currentGroup = null;
            var currentGroupIds = new List<string>();

            foreach (var log in logs)
            {
                // Check if this log can be grouped with the current group
                if (currentGroup != null && 
                    currentGroup.Level == log.Level && 
                    currentGroup.Message == log.Message &&
                    currentGroup.Category == log.Category)
                {
                    // Add to current group
                    currentGroupIds.Add(log.RecordIdentifier);
                }
                else
                {
                    // Save previous group if exists
                    if (currentGroup != null)
                    {
                        groupedLogs.Add(new GroupedLogEntry
                        {
                            Level = currentGroup.Level,
                            Message = currentGroup.Message,
                            Category = currentGroup.Category,
                            RecordIdentifiers = new List<string>(currentGroupIds),
                            RecordCount = currentGroupIds.Count,
                            FirstTimestamp = currentGroup.Timestamp,
                            LastTimestamp = logs.Where(l => currentGroupIds.Contains(l.RecordIdentifier)).Max(l => l.Timestamp)
                        });
                    }

                    // Start new group
                    currentGroup = log;
                    currentGroupIds = new List<string> { log.RecordIdentifier };
                }
            }

            // Add last group
            if (currentGroup != null)
            {
                groupedLogs.Add(new GroupedLogEntry
                {
                    Level = currentGroup.Level,
                    Message = currentGroup.Message,
                    Category = currentGroup.Category,
                    RecordIdentifiers = new List<string>(currentGroupIds),
                    RecordCount = currentGroupIds.Count,
                    FirstTimestamp = currentGroup.Timestamp,
                    LastTimestamp = logs.Where(l => currentGroupIds.Contains(l.RecordIdentifier)).Max(l => l.Timestamp)
                });
            }

            return groupedLogs;
        }

        /// <summary>
        /// Format logs for display/export
        /// </summary>
        public string FormatLogsForDisplay(bool includeDebug = false)
        {
            var logs = includeDebug 
                ? GetAllLogs() 
                : _logs.Where(l => l.Level != MigrationLogLevel.Debug).OrderBy(l => l.Timestamp).ToList();

            if (!logs.Any())
                return "No logs available.";

            var output = new System.Text.StringBuilder();
            output.AppendLine($"=== Migration Logs for {_tableName} ===");
            output.AppendLine($"Total Processed: {_processedCount}, Inserted: {_insertedCount}, Skipped: {_skippedCount}, Errors: {_errorCount}");
            output.AppendLine();

            foreach (var log in logs)
            {
                output.AppendLine($"[{log.Timestamp:yyyy-MM-dd HH:mm:ss}] [{log.Level}] {log.RecordIdentifier}: {log.Message}");
                if (log.Details.Any())
                {
                    foreach (var detail in log.Details)
                    {
                        output.AppendLine($"  - {detail.Key}: {detail.Value}");
                    }
                }
            }

            return output.ToString();
        }

        /// <summary>
        /// Format grouped logs for display (avoids redundancy)
        /// </summary>
        public string FormatGroupedLogsForDisplay(bool includeDebug = false)
        {
            var groupedLogs = GetGroupedLogs(includeDebug);

            if (!groupedLogs.Any())
                return "No logs available.";

            var output = new System.Text.StringBuilder();
            output.AppendLine($"=== Migration Logs for {_tableName} ===");
            output.AppendLine($"Total Processed: {_processedCount}, Inserted: {_insertedCount}, Skipped: {_skippedCount}, Errors: {_errorCount}");
            output.AppendLine();

            foreach (var group in groupedLogs)
            {
                if (group.RecordCount == 1)
                {
                    output.AppendLine($"[{group.FirstTimestamp:yyyy-MM-dd HH:mm:ss}] [{group.Level}] {group.RecordIdentifiers[0]}: {group.Message}");
                }
                else
                {
                    var idRange = GetIdRange(group.RecordIdentifiers);
                    output.AppendLine($"[{group.FirstTimestamp:yyyy-MM-dd HH:mm:ss}] [{group.Level}] {idRange} ({group.RecordCount} records): {group.Message}");
                }
            }

            return output.ToString();
        }

        /// <summary>
        /// Extract ID range from record identifiers
        /// </summary>
        private string GetIdRange(List<string> identifiers)
        {
            if (identifiers.Count == 1)
                return identifiers[0];

            // Try to extract numeric IDs
            var numericIds = new List<int>();
            foreach (var id in identifiers)
            {
                var match = System.Text.RegularExpressions.Regex.Match(id, @"(\d+)");
                if (match.Success && int.TryParse(match.Groups[1].Value, out int numId))
                {
                    numericIds.Add(numId);
                }
            }

            if (numericIds.Any())
            {
                numericIds.Sort();
                if (numericIds.Count == numericIds.Max() - numericIds.Min() + 1)
                {
                    // Continuous range
                    var prefix = identifiers[0].Split('=')[0];
                    return $"{prefix}={numericIds.Min()}-{numericIds.Max()}";
                }
                else
                {
                    // Show first, last and count
                    var prefix = identifiers[0].Split('=')[0];
                    return $"{prefix}={numericIds.Min()}...{numericIds.Max()}";
                }
            }

            return $"{identifiers[0]}...{identifiers[identifiers.Count - 1]}";
        }

        /// <summary>
        /// Clear all logs (use with caution)
        /// </summary>
        public void Clear()
        {
            _logs.Clear();
            _skippedCount = 0;
            _insertedCount = 0;
            _processedCount = 0;
            _errorCount = 0;
        }
    }

    /// <summary>
    /// Summary of migration results
    /// </summary>
    public class MigrationSummary
    {
        public string TableName { get; set; } = "";
        public int TotalProcessed { get; set; }
        public int TotalInserted { get; set; }
        public int TotalSkipped { get; set; }
        public int TotalErrors { get; set; }
        public List<MigrationLogEntry> Logs { get; set; } = new List<MigrationLogEntry>();
    }

    /// <summary>
    /// Grouped log entry for reducing redundancy
    /// </summary>
    public class GroupedLogEntry
    {
        public MigrationLogLevel Level { get; set; }
        public string Message { get; set; } = "";
        public string Category { get; set; } = "";
        public List<string> RecordIdentifiers { get; set; } = new List<string>();
        public int RecordCount { get; set; }
        public DateTime FirstTimestamp { get; set; }
        public DateTime LastTimestamp { get; set; }
    }
}
