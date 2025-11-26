using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System;

public abstract class MigrationService
{
    private readonly IConfiguration _configuration;
    protected abstract string SelectQuery { get; }
    protected abstract string InsertQuery { get; }
    protected abstract List<string> GetLogics();

    public MigrationService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public SqlConnection GetSqlServerConnection()
    {
        return new SqlConnection(_configuration.GetConnectionString("SqlServer"));
    }

    public NpgsqlConnection GetPostgreSqlConnection()
    {
        return new NpgsqlConnection(_configuration.GetConnectionString("PostgreSql"));
    }

    /// <summary>
    /// Abstract method that derived classes must implement for their specific migration logic
    /// </summary>
    protected abstract Task<int> ExecuteMigrationAsync(SqlConnection sqlConn, NpgsqlConnection pgConn, NpgsqlTransaction? transaction = null);

    /// <summary>
    /// Common migration method with transaction support
    /// </summary>
    public virtual async Task<int> MigrateAsync(bool useTransaction = true)
    {
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();

        if (useTransaction)
        {
            using var transaction = await pgConn.BeginTransactionAsync();
            try
            {
                int result = await ExecuteMigrationAsync(sqlConn, pgConn, transaction);
                await transaction.CommitAsync();
                return result;
            }
            catch (Exception)
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        else
        {
            return await ExecuteMigrationAsync(sqlConn, pgConn);
        }
    }

    /// <summary>
    /// Dynamically generates mappings based on SelectQuery, InsertQuery, and Logics
    /// </summary>
    public List<object> GetMappings()
    {
        // Parse sources from SELECT
        var sources = ParseSelectColumns(SelectQuery);
        
        // Parse targets from INSERT
        var targets = ParseInsertColumns(InsertQuery);
        
        // Get logics from derived class
        var logics = GetLogics();

        // Build mappings
        var mappings = new List<object>();
        for (int i = 0; i < targets.Count; i++)
        {
            var source = i < sources.Count ? sources[i] : "-";
            var logic = i < logics.Count ? logics[i] : "Unknown";
            mappings.Add(new { source = source, logic = logic, target = targets[i] });
        }
        return mappings;
    }

    /// <summary>
    /// Parses column names from SELECT query
    /// </summary>
    protected List<string> ParseSelectColumns(string selectQuery)
    {
        // Simple parsing: assume "SELECT col1, col2, ... FROM table"
        var start = selectQuery.IndexOf("SELECT", System.StringComparison.OrdinalIgnoreCase) + 7;
        var end = selectQuery.IndexOf("FROM", System.StringComparison.OrdinalIgnoreCase);
        var columnsPart = selectQuery.Substring(start, end - start).Trim();
        return columnsPart.Split(',').Select(c => c.Trim()).ToList();
    }

    /// <summary>
    /// Parses column names from INSERT query
    /// </summary>
    protected List<string> ParseInsertColumns(string insertQuery)
    {
        // Simple parsing: assume "INSERT INTO table (col1, col2, ...) VALUES (...)"
        var start = insertQuery.IndexOf("(") + 1;
        var end = insertQuery.IndexOf(")");
        var columnsPart = insertQuery.Substring(start, end - start).Trim();
        return columnsPart.Split(',').Select(c => c.Trim()).ToList();
    }

    /// <summary>
    /// Migrates multiple migration services in a single transaction
    /// </summary>
    public static async Task<(int TotalMigrated, Dictionary<string, int> Results)> MigrateMultipleAsync(
        IEnumerable<MigrationService> migrationServices, 
        bool useCommonTransaction = true)
    {
        var results = new Dictionary<string, int>();
        int totalMigrated = 0;

        if (!migrationServices.Any())
            return (0, results);

        var firstService = migrationServices.First();

        if (useCommonTransaction)
        {
            using var sqlConn = firstService.GetSqlServerConnection();
            using var pgConn = firstService.GetPostgreSqlConnection();
            await sqlConn.OpenAsync();
            await pgConn.OpenAsync();

            using var transaction = await pgConn.BeginTransactionAsync();
            try
            {
                foreach (var service in migrationServices)
                {
                    var serviceName = service.GetType().Name;
                    try
                    {
                        int result = await service.ExecuteMigrationAsync(sqlConn, pgConn, transaction);
                        results[serviceName] = result;
                        totalMigrated += result;
                    }
                    catch (Exception)
                    {
                        results[serviceName] = -1; // Indicates failure
                        throw; // Re-throw to trigger rollback
                    }
                }

                await transaction.CommitAsync();
            }
            catch (Exception)
            {
                await transaction.RollbackAsync();
                throw;
            }
        }
        else
        {
            foreach (var service in migrationServices)
            {
                var serviceName = service.GetType().Name;
                try
                {
                    int result = await service.MigrateAsync(useTransaction: true);
                    results[serviceName] = result;
                    totalMigrated += result;
                }
                catch (Exception)
                {
                    results[serviceName] = -1; // Indicates failure
                    // Continue with next service in case of individual failures
                }
            }
        }

        return (totalMigrated, results);
    }
}