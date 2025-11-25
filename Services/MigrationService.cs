using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Linq;

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
}